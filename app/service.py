from dataclasses import dataclass
from typing import List, Dict, Any, Tuple

from decimal import Decimal, ROUND_CEILING
from django.db import transaction
from django.db.models import Sum

from .models import (
    TEPCode,
    Material,
    ForecastRun,
    ForecastLine,
    MaterialList,
    MaterialAllocation,
    Customer,
)


@dataclass
class ForecastInput:
    part_code: str
    forecast_qty: int
    schedule_month: str = ""


def _normalize_partcode(s: str) -> str:
    return (s or "").strip()


def compute_material_requirements_for_partcode(part_code: str, forecast_qty: int):
    """
    Prototype logic:
      - Find ONE TEPCode row for this part_code (first match).
      - Pull its BOM lines from Material (FIFO by id).
      - required_qty = Material.total * forecast_qty
      - Aggregate per mat_partcode.

    Returns:
      tep (TEPCode or None),
      aggregated list of dicts:
        [{mat_partcode, mat_partname, mat_maker, unit, per_unit_total, required_qty}, ...]
    """
    part_code = _normalize_partcode(part_code)
    if not part_code:
        return None, []

    qs = TEPCode.objects.filter(part_code=part_code).order_by("id")
    try:
        qs = TEPCode.objects.filter(part_code=part_code).order_by("-is_active", "id")
    except Exception:
        pass

    tep = qs.first()
    if not tep:
        return None, []

    bom_qs = Material.objects.filter(tep_code=tep).order_by("id")

    grouped = (
        bom_qs.values("mat_partcode", "mat_partname", "mat_maker", "unit")
        .annotate(per_unit_total=Sum("total"))
        .order_by("mat_partcode")
    )

    out = []
    for row in grouped:
        per_unit = float(row["per_unit_total"] or 0)
        req = round(per_unit * int(forecast_qty), 4)
        out.append({
            "mat_partcode": row["mat_partcode"],
            "mat_partname": row["mat_partname"],
            "mat_maker": row["mat_maker"],
            "unit": row["unit"],
            "per_unit_total": per_unit,
            "required_qty": req,
        })

    return tep, out


@transaction.atomic
def run_forecast_and_save(
    inputs: List[ForecastInput],
    created_by=None,
    note: str = "",
) -> ForecastRun:
    """
    Creates ForecastRun header + ForecastLine rows (output of computation).

    UPDATED BEHAVIOR (service-only):
    - If schedule_month is provided (and consistent across inputs),
      reuse the latest ForecastRun for that month instead of creating a new one.
    - This lets the dashboard "latest run" contain multiple customers at once.
    """
    # If all inputs share the same schedule_month use it on the header,
    # otherwise leave it blank.
    schedule_month = ""
    if inputs:
        months = {(item.schedule_month or "").strip() for item in inputs}
        months.discard("")
        if len(months) == 1:
            schedule_month = months.pop()

    month_key = schedule_month[:7]  # 'YYYY-MM'

    # ✅ REUSE RUN (so old customers don't "disappear" from the table)
    run = None
    if month_key:
        run = (
            ForecastRun.objects
            .filter(schedule_month=month_key)
            .order_by("-id")
            .first()
        )

    # If no existing run for that month, create one (original behavior)
    if run is None:
        run = ForecastRun.objects.create(
            note=note or "Prototype forecast run",
            created_by=created_by,
            schedule_month=month_key,
        )
    else:
        # Keep existing run; don't change views.
        # (Optional) You could update the note, but leaving it alone is safest.
        pass

    for item in inputs:
        tep, rows = compute_material_requirements_for_partcode(item.part_code, item.forecast_qty)

        if not tep:
            ForecastLine.objects.create(
                run=run,
                part_code=item.part_code,
                forecast_qty=item.forecast_qty,
                mat_partcode="(NO TEP FOUND)",
                mat_partname="—",
                mat_maker="—",
                unit="—",
                per_unit_total=0,
                required_qty=0,
                tep_code="—",
                customer_name="—",
            )
            continue

        for r in rows:
            ForecastLine.objects.create(
                run=run,
                part_code=item.part_code,
                forecast_qty=item.forecast_qty,
                mat_partcode=r["mat_partcode"],
                mat_partname=r["mat_partname"],
                mat_maker=r["mat_maker"],
                unit=r["unit"],
                per_unit_total=r["per_unit_total"],
                required_qty=r["required_qty"],
                tep_code=tep.tep_code,
                customer_name=tep.customer.customer_name,
            )

    return run

@transaction.atomic
def reserve_from_latest_forecast_run(created_by=None, allow_partial: bool = False) -> Dict[str, Any]:
    """
    Creates MaterialAllocation rows (status='reserved') from the latest ForecastRun.

    Rules:
    - Uses latest ForecastRun (by id).
    - Groups by material code across the run (SUM required_qty) so you don't reserve duplicates per line.
    - qty_allocated is integer => CEIL(total_required_qty).
    - available = on_hand - already_reserved
    - allow_partial:
        False -> skip if available < needed
        True  -> reserve min(available, needed) if available > 0
    - Customer is taken from ForecastLine.customer_name; must exist in Customer table.
    - TEP is optional; from ForecastLine.tep_code if found in TEPCode.
    - Material must exist in MaterialList.

    Returns dict like:
      {"ok": True/False, "message": str, "created": int, "skipped": int, "notes": [str]}
    """
    latest = ForecastRun.objects.order_by("-id").first()
    if not latest:
        return {"ok": False, "message": "No forecast run found.", "created": 0, "skipped": 0, "notes": []}

    try:
        base_qs = latest.lines.all()
    except Exception:
        base_qs = ForecastLine.objects.filter(run=latest)

    if not base_qs.exists():
        return {"ok": False, "message": "Latest forecast run has no lines.", "created": 0, "skipped": 0, "notes": []}

    forecast_ref = f"RUN:{latest.id}"
    created = 0
    skipped = 0
    notes: List[str] = []

    base_qs = base_qs.exclude(mat_partcode__startswith="(").exclude(mat_partcode__isnull=True).exclude(mat_partcode="")

    grouped = (
        base_qs.values("mat_partcode", "customer_name", "tep_code")
        .annotate(total_required=Sum("required_qty"))
        .order_by("mat_partcode")
    )

    for g in grouped:
        mat_code = (g.get("mat_partcode") or "").strip()
        cname = (g.get("customer_name") or "").strip()
        tcode = (g.get("tep_code") or "").strip()
        total_required = g.get("total_required") or 0

        if not mat_code:
            skipped += 1
            continue
        if not cname or cname == "—":
            skipped += 1
            notes.append(f"Skipped {mat_code}: customer_name missing.")
            continue

        master = MaterialList.objects.filter(mat_partcode=mat_code).first()
        if not master:
            skipped += 1
            notes.append(f"Skipped {mat_code}: not found in MaterialList.")
            continue

        cust = Customer.objects.filter(customer_name=cname).first()
        if not cust:
            skipped += 1
            notes.append(f"Skipped {mat_code}: customer not found ({cname}).")
            continue

        tep = None
        if tcode and tcode != "—":
            tep = TEPCode.objects.filter(tep_code=tcode).first()

        needed = _ceil_int(total_required)
        if needed <= 0:
            skipped += 1
            continue

        try:
            on_hand = int(master.stock.on_hand_qty or 0)
        except Exception:
            on_hand = 0

        reserved = (
            MaterialAllocation.objects
            .filter(material=master, status="reserved")
            .aggregate(total=Sum("qty_allocated"))
            .get("total") or 0
        )
        reserved = int(reserved or 0)

        available = max(on_hand - reserved, 0)

        if allow_partial:
            take = min(available, needed)
            if take <= 0:
                skipped += 1
                continue
        else:
            if available < needed:
                skipped += 1
                continue
            take = needed

        MaterialAllocation.objects.create(
            material=master,
            customer=cust,
            tep_code=tep,
            qty_allocated=take,
            forecast_ref=forecast_ref,
            status="reserved",
            created_by=created_by,
        )
        created += 1

    if created <= 0:
        return {
            "ok": False,
            "message": f"No allocations created from latest forecast ({forecast_ref}). Check stocks / master list / customers.",
            "created": created,
            "skipped": skipped,
            "notes": notes,
        }

    return {
        "ok": True,
        "message": f"Reserved allocations created from latest forecast ({forecast_ref}).",
        "created": created,
        "skipped": skipped,
        "notes": notes,
    }