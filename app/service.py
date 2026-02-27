from dataclasses import dataclass
from typing import Dict, List, Tuple

from django.db import transaction
from django.db.models import Sum

from .models import TEPCode, Material, ForecastRun, ForecastLine


@dataclass
class ForecastInput:
    part_code: str
    forecast_qty: int


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

    tep = TEPCode.objects.filter(part_code=part_code).order_by("id").first()
    if not tep:
        return None, []

    # FIFO for lines
    bom_qs = Material.objects.filter(tep_code=tep).order_by("id")

    # Aggregate by mat_partcode
    grouped = (
        bom_qs.values("mat_partcode", "mat_partname", "mat_maker", "unit")
        .annotate(per_unit_total=Sum("total"))  # if duplicates exist, sum per-unit totals
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
def run_forecast_and_save(inputs: List[ForecastInput], created_by=None, note: str = "") -> ForecastRun:
    """
    Creates ForecastRun header + ForecastLine rows (output of computation).
    """
    run = ForecastRun.objects.create(
        note=note or "Prototype forecast run",
        created_by=created_by,
    )

    for item in inputs:
        tep, rows = compute_material_requirements_for_partcode(item.part_code, item.forecast_qty)

        # Save inputs even if no TEP found (so you can see failures in DB)
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


def prototype_static_run(created_by=None) -> ForecastRun:
    """
    Your requested prototype:
      - L73436-001 REV.D -> 5000
      - LT3435-001 REV.C -> 10000
    """
    inputs = [
        ForecastInput(part_code="L73436-001 REV.D", forecast_qty=5000),
        ForecastInput(part_code="LT3435-001 REV.C", forecast_qty=10000),
    ]
    return run_forecast_and_save(inputs, created_by=created_by, note="STATIC prototype run (5000/10000)")