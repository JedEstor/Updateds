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
    PartMaster,
)

try:
    from .models import BOMMaterial
except Exception:  # pragma: no cover
    BOMMaterial = None


@dataclass
class ForecastInput:
    part_code: str
    forecast_qty: int
    schedule_month: str = ""


def _normalize_partcode(s: str) -> str:
    return (s or "").strip()




def get_shared_part_master_map() -> Dict[str, str]:
    part_map: Dict[str, str] = {}
    for part in PartMaster.objects.filter(is_active=True).order_by("part_code"):
        code = _normalize_partcode(part.part_code)
        if code:
            part_map[code] = (part.part_name or "").strip() or code
    return part_map


def save_shared_part_master(part_code: str, part_name: str = "") -> PartMaster:
    part_code = _normalize_partcode(part_code)
    part_name = (part_name or "").strip()
    if not part_code:
        raise ValueError("Part code is required.")
    if not part_name:
        raise ValueError("Part name is required.")

    obj, _ = PartMaster.objects.update_or_create(
        part_code=part_code,
        defaults={"part_name": part_name, "is_active": True},
    )
    return obj


def get_shared_part_name(part_code: str) -> str:
    part_code = _normalize_partcode(part_code)
    if not part_code:
        return ""

    part = PartMaster.objects.filter(part_code=part_code).first()
    if part:
        return (part.part_name or "").strip()

    return part_code


def _to_decimal(value, places: str = "0.0001") -> Decimal:
    try:
        return Decimal(str(value or 0)).quantize(Decimal(places))
    except Exception:
        return Decimal("0.0000")


def _ceil_int(value) -> int:
    try:
        return int(Decimal(str(value or 0)).to_integral_value(rounding=ROUND_CEILING))
    except Exception:
        return 0


def _get_active_tep_for_partcode(part_code: str):
    """
    Returns the preferred TEP for a part code.

    Since part_code is no longer globally unique, we do NOT use .get().
    We prefer an active TEP first, then the earliest id as a stable fallback.
    """
    part_code = _normalize_partcode(part_code)
    if not part_code:
        return None

    qs = TEPCode.objects.filter(part_code=part_code).select_related("customer")

    # Prefer active rows when the field exists.
    try:
        active = qs.filter(is_active=True).order_by("id").first()
        if active:
            return active
    except Exception:
        pass

    return qs.order_by("id").first()


def _get_bom_rows_for_tep(tep):
    """
    Returns registered recipe rows for the given TEP.

    Priority:
    1) Shared BOMMaterial rows for tep.part_code
    2) Legacy Material rows under the current TEP
    """
    if not tep:
        return []

    part_code = _normalize_partcode(getattr(tep, "part_code", ""))

    if BOMMaterial is not None and part_code:
        try:
            qs = (
                BOMMaterial.objects
                .filter(part_code=part_code)
                .select_related("material", "source_tep")
                .order_by("mat_partcode", "id")
            )
            if qs.exists():
                return list(qs)
        except Exception:
            pass

    try:
        return list(Material.objects.filter(tep_code=tep).order_by("id"))
    except Exception:
        return []


def _material_row_to_dict(row) -> Dict[str, Any]:
    return {
        "mat_partcode": getattr(row, "mat_partcode", "") or "",
        "mat_partname": getattr(row, "mat_partname", "") or "",
        "mat_maker": getattr(row, "mat_maker", "") or "",
        "unit": getattr(row, "unit", "") or "",
        "per_unit_total": _to_decimal(getattr(row, "total", 0)),
    }


def get_registered_materials_for_partcode(part_code: str) -> Tuple[Any, List[Dict[str, Any]]]:
    """
    Helper for views / APIs.

    Returns:
      tep, rows where rows look like:
      [{mat_partcode, mat_partname, mat_maker, unit, dim_qty, loss_percent, total}, ...]
    """
    tep = _get_active_tep_for_partcode(part_code)
    if not tep:
        return None, []

    rows = []
    for row in _get_bom_rows_for_tep(tep):
        rows.append({
            "mat_partcode": getattr(row, "mat_partcode", "") or "",
            "mat_partname": getattr(row, "mat_partname", "") or "",
            "mat_maker": getattr(row, "mat_maker", "") or "",
            "unit": getattr(row, "unit", "") or "",
            "dim_qty": getattr(row, "dim_qty", 0) or 0,
            "loss_percent": getattr(row, "loss_percent", 0) or 0,
            "total": getattr(row, "total", 0) or 0,
        })
    return tep, rows


def compute_material_requirements_for_partcode(part_code: str, forecast_qty: int):
    """
    Forecast logic:
      - Find ONE preferred TEP row for this part_code.
      - Pull its registered recipe/BOM lines.
      - required_qty = per_unit_total * forecast_qty
      - Aggregate by material part code.

    Returns:
      tep (TEPCode or None),
      aggregated list of dicts:
        [{mat_partcode, mat_partname, mat_maker, unit, per_unit_total, required_qty}, ...]
    """
    part_code = _normalize_partcode(part_code)
    if not part_code:
        return None, []

    try:
        forecast_qty = int(forecast_qty or 0)
    except Exception:
        forecast_qty = 0

    tep = _get_active_tep_for_partcode(part_code)
    if not tep:
        return None, []

    grouped: Dict[str, Dict[str, Any]] = {}

    for row in _get_bom_rows_for_tep(tep):
        item = _material_row_to_dict(row)
        mat_code = (item["mat_partcode"] or "").strip()
        if not mat_code:
            continue

        if mat_code not in grouped:
            grouped[mat_code] = {
                "mat_partcode": mat_code,
                "mat_partname": item["mat_partname"],
                "mat_maker": item["mat_maker"],
                "unit": item["unit"],
                "per_unit_total": Decimal("0.0000"),
            }

        grouped[mat_code]["per_unit_total"] += _to_decimal(item["per_unit_total"])

    out = []
    for _, row in sorted(grouped.items(), key=lambda kv: kv[0]):
        per_unit = row["per_unit_total"].quantize(Decimal("0.0001"))
        required_qty = (per_unit * Decimal(str(forecast_qty or 0))).quantize(Decimal("0.0001"))
        out.append({
            "mat_partcode": row["mat_partcode"],
            "mat_partname": row["mat_partname"],
            "mat_maker": row["mat_maker"],
            "unit": row["unit"],
            "per_unit_total": per_unit,
            "required_qty": required_qty,
        })

    return tep, out


@transaction.atomic
def run_forecast_and_save(
    inputs: List[ForecastInput],
    created_by=None,
    note: str = "",
) -> ForecastRun:
    """
    Creates / reuses a ForecastRun header + writes ForecastLine rows.

    Behavior:
    - If all inputs share the same schedule_month, reuse the latest run for that month.
    - For the same run + part_code, old computed lines are deleted first so re-running
      that part code in the same month won't keep duplicating rows.
    """
    schedule_month = ""
    if inputs:
        months = {(item.schedule_month or "").strip() for item in inputs}
        months.discard("")
        if len(months) == 1:
            schedule_month = months.pop()

    month_key = schedule_month[:7]  # 'YYYY-MM'

    run = None
    if month_key:
        run = (
            ForecastRun.objects
            .filter(schedule_month=month_key)
            .order_by("-id")
            .first()
        )

    if run is None:
        run = ForecastRun.objects.create(
            note=note or "Prototype forecast run",
            created_by=created_by,
            schedule_month=month_key,
        )

    # Prevent duplicate lines when re-running the same part_code for the same reused run.
    part_codes = sorted({(_normalize_partcode(item.part_code)) for item in inputs if _normalize_partcode(item.part_code)})
    if part_codes:
        ForecastLine.objects.filter(run=run, part_code__in=part_codes).delete()

    for item in inputs:
        tep, rows = compute_material_requirements_for_partcode(item.part_code, item.forecast_qty)

        if not tep:
            ForecastLine.objects.create(
                run=run,
                part_code=_normalize_partcode(item.part_code),
                forecast_qty=int(item.forecast_qty or 0),
                mat_partcode="(NO TEP FOUND)",
                mat_partname="—",
                mat_maker="—",
                unit="—",
                per_unit_total=Decimal("0.0000"),
                required_qty=Decimal("0.0000"),
                tep_code="—",
                customer_name="—",
            )
            continue

        for r in rows:
            ForecastLine.objects.create(
                run=run,
                part_code=_normalize_partcode(item.part_code),
                forecast_qty=int(item.forecast_qty or 0),
                mat_partcode=r["mat_partcode"],
                mat_partname=r["mat_partname"],
                mat_maker=r["mat_maker"],
                unit=r["unit"],
                per_unit_total=r["per_unit_total"],
                required_qty=r["required_qty"],
                tep_code=getattr(tep, "tep_code", "") or "—",
                customer_name=getattr(getattr(tep, "customer", None), "customer_name", "") or "—",
            )

    return run


@transaction.atomic


def get_shared_bom_rows_for_partcode(part_code: str) -> List[Dict[str, Any]]:
    part_code = _normalize_partcode(part_code)
    if not part_code:
        return []

    tep = _get_active_tep_for_partcode(part_code)
    rows = []
    for row in _get_bom_rows_for_tep(tep):
        rows.append({
            "mat_partcode": getattr(row, "mat_partcode", "") or "",
            "mat_partname": getattr(row, "mat_partname", "") or "",
            "mat_maker": getattr(row, "mat_maker", "") or "",
            "unit": getattr(row, "unit", "") or "",
            "dim_qty": getattr(row, "dim_qty", 0) or 0,
            "loss_percent": getattr(row, "loss_percent", 0) or 0,
            "total": getattr(row, "total", 0) or 0,
        })
    return rows


@transaction.atomic
def replace_bom_for_partcode(part_code: str, rows: List[Dict[str, Any]], source_tep=None):
    """Replace the shared BOM of a part code in one save action."""
    if BOMMaterial is None:
        raise RuntimeError("BOMMaterial model is not available.")

    part_code = _normalize_partcode(part_code)
    if not part_code:
        raise ValueError("Part code is required.")

    cleaned = []
    for row in rows:
        mat_code = _normalize_partcode(row.get("mat_partcode"))
        if not mat_code:
            continue
        master = MaterialList.objects.filter(mat_partcode__iexact=mat_code).first()
        if not master:
            raise ValueError(f"Material code not found in master list: {mat_code}")

        try:
            dim_qty = Decimal(str(row.get("dim_qty") or 0))
        except Exception:
            raise ValueError(f"Dim/Qty must be numeric for {mat_code}.")

        try:
            loss_percent = Decimal(str(row.get("loss_percent") or 10))
        except Exception:
            raise ValueError(f"Loss % must be numeric for {mat_code}.")

        cleaned.append({
            "master": master,
            "dim_qty": dim_qty,
            "loss_percent": loss_percent,
        })

    if not cleaned:
        raise ValueError("Add at least one BOM material row before saving.")

    BOMMaterial.objects.filter(part_code=part_code).delete()

    created = []
    for row in cleaned:
        master = row["master"]
        obj = BOMMaterial.objects.create(
            part_code=part_code,
            source_tep=source_tep,
            material=master,
            mat_partcode=master.mat_partcode,
            mat_partname=master.mat_partname,
            mat_maker=master.mat_maker,
            unit=master.unit,
            dim_qty=row["dim_qty"],
            loss_percent=row["loss_percent"],
        )
        created.append(obj)

        if source_tep is not None:
            Material.objects.update_or_create(
                tep_code=source_tep,
                mat_partcode=master.mat_partcode,
                defaults={
                    "mat_partname": master.mat_partname,
                    "mat_maker": master.mat_maker,
                    "unit": master.unit,
                    "dim_qty": float(row["dim_qty"]),
                    "loss_percent": float(row["loss_percent"]),
                },
            )

    return created


def reserve_from_latest_forecast_run(created_by=None, allow_partial: bool = False) -> Dict[str, Any]:
    """
    Creates / updates MaterialAllocation rows (status='reserved') from the latest ForecastRun.

    Rules:
    - Uses latest ForecastRun (by id).
    - Groups by material + customer + tep_code across the run.
    - qty_allocated is integer => CEIL(total_required_qty).
    - available = on_hand - already_reserved(excluding same forecast_ref row)
    - allow_partial:
        False -> skip if available < needed
        True  -> reserve min(available, needed) if available > 0
    - If a matching reserved allocation for the same forecast_ref already exists,
      update it instead of inserting a duplicate row.
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
        .order_by("mat_partcode", "customer_name", "tep_code")
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

        existing_reserved_same_ref = MaterialAllocation.objects.filter(
            material=master,
            customer=cust,
            tep_code=tep,
            forecast_ref=forecast_ref,
            status="reserved",
        ).first()

        reserved_other = (
            MaterialAllocation.objects
            .filter(material=master, status="reserved")
            .exclude(id=getattr(existing_reserved_same_ref, "id", None))
            .aggregate(total=Sum("qty_allocated"))
            .get("total") or 0
        )
        reserved_other = int(reserved_other or 0)

        available = max(on_hand - reserved_other, 0)

        if allow_partial:
            take = min(available, needed)
            if take <= 0:
                skipped += 1
                continue
        else:
            if available < needed:
                skipped += 1
                notes.append(f"Skipped {mat_code}: insufficient stock (need {needed}, available {available}).")
                continue
            take = needed

        if existing_reserved_same_ref:
            existing_reserved_same_ref.qty_allocated = take
            existing_reserved_same_ref.created_by = created_by
            existing_reserved_same_ref.save(update_fields=["qty_allocated", "created_by"])
        else:
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

    if created <= 0 and not MaterialAllocation.objects.filter(forecast_ref=forecast_ref, status="reserved").exists():
        return {
            "ok": False,
            "message": f"No allocations created from latest forecast ({forecast_ref}). Check stocks / master list / customers.",
            "created": created,
            "skipped": skipped,
            "notes": notes,
        }

    return {
        "ok": True,
        "message": f"Reserved allocations processed from latest forecast ({forecast_ref}).",
        "created": created,
        "skipped": skipped,
        "notes": notes,
    }
