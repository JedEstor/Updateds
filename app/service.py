from dataclasses import dataclass
from typing import List, Dict, Any, Tuple, Optional
import io, csv, re
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
except Exception:
    BOMMaterial = None


@dataclass
class ForecastInput:
    customer_id: Optional[int] = None
    customer_name: str = ""
    part_code: str = ""
    forecast_qty: int = 0
    schedule_month: str = ""


def _normalize_partcode(s: str) -> str:
    return (s or "").strip()


# =========================================================
# PART MASTER HELPERS
# =========================================================

def get_shared_part_master_map() -> Dict[str, str]:
    part_map: Dict[str, str] = {}

    for part in PartMaster.objects.filter(is_active=True).order_by("part_code"):
        code = _normalize_partcode(part.part_code)
        if code:
            part_map[code] = (part.part_name or "").strip() or code

    return part_map


def get_shared_part_name(part_code: str) -> str:
    part_code = _normalize_partcode(part_code)

    if not part_code:
        return ""

    part = PartMaster.objects.filter(part_code=part_code).first()

    if part:
        return (part.part_name or "").strip()

    return part_code


def get_active_part_master_choices():
    return list(
        PartMaster.objects
        .filter(is_active=True)
        .order_by("part_code")
        .values("part_code", "part_name")
    )

def _normalize_unit_value(unit: str) -> str:
    u = (unit or "").strip().lower()
    mapping = {
        "pc": "pc",
        "pcs": "pcs",
        "piece": "pc",
        "pieces": "pcs",
        "m": "m",
        "meter": "m",
        "meters": "m",
        "g": "g",
        "gram": "g",
        "grams": "g",
        "kg": "kg",
        "kilogram": "kg",
        "kilograms": "kg",
    }
    return mapping.get(u, u or "pc")


def _parse_loss_value(raw) -> Decimal:
    if raw is None:
        return Decimal("0")

    s = str(raw).strip()

    s = s.replace("%", "")
    s = s.replace(",", ".")
    s = s.replace("\r", "")
    s = s.replace("\n", "")

    try:
        return Decimal(s)
    except Exception:
        print("BAD LOSS:", raw)   # debug
        return Decimal("0")


def _parse_decimal_value(raw) -> Decimal:
    if raw is None:
        return Decimal("0")

    s = str(raw).strip()

    # remove hidden characters
    s = s.replace(",", ".")
    s = s.replace("\r", "")
    s = s.replace("\n", "")

    try:
        return Decimal(s)
    except Exception:
        print("BAD DECIMAL:", raw)   # debug
        return Decimal("0")


@transaction.atomic
def import_bom_csv_file(uploaded_file, created_by=None) -> Dict[str, Any]:
    """
    CSV columns expected:
    PartCode, MaterialsCode, MaterialPartname, Maker, U/M, Qty/Dimens, Loss

    Behavior:
    - auto-create PartMaster if missing
    - auto-create MaterialList if missing
    - replace BOM rows per part code found in the CSV
    """
    if not uploaded_file:
        raise ValueError("No CSV file uploaded.")

    raw = uploaded_file.read()
    try:
        text = raw.decode("utf-8-sig")
    except Exception:
        text = raw.decode("utf-8", errors="ignore")

    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader)

    if not rows:
        raise ValueError("CSV file is empty.")

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    created_parts = 0
    created_materials = 0
    imported_rows = 0

    for idx, row in enumerate(rows, start=2):
        part_code = _normalize_partcode(row.get("PartCode"))
        mat_code = _normalize_partcode(row.get("MaterialsCode"))
        mat_name = (row.get("MaterialPartname") or "").strip()
        maker = (row.get("Maker") or "").strip()
        unit = _normalize_unit_value(row.get("U/M"))
        dim_qty = _parse_decimal_value(row.get("Qty/Dimension"))
        loss_percent = _parse_loss_value(row.get("Loss%"))

        if not part_code:
            continue
        if not mat_code:
            continue

        part_obj, part_created = PartMaster.objects.get_or_create(
            part_code=part_code,
            defaults={
                "part_name": part_code,
                "is_active": True,
            },
        )
        if part_created:
            created_parts += 1

        if mat_name and (not part_obj.part_name or part_obj.part_name == part_obj.part_code):
            part_obj.part_name = part_obj.part_name or part_code
            part_obj.save(update_fields=["part_name"])

        material_obj, material_created = MaterialList.objects.get_or_create(
            mat_partcode=mat_code,
            defaults={
                "mat_partname": mat_name or mat_code,
                "mat_maker": maker or "-",
                "unit": unit or "pc",
            },
        )
        if material_created:
            created_materials += 1
        else:
            changed = False
            if mat_name and not material_obj.mat_partname:
                material_obj.mat_partname = mat_name
                changed = True
            if maker and not material_obj.mat_maker:
                material_obj.mat_maker = maker
                changed = True
            if unit and not material_obj.unit:
                material_obj.unit = unit
                changed = True
            if changed:
                material_obj.save()

        grouped.setdefault(part_code, [])

        existing_codes = {r["mat_partcode"] for r in grouped[part_code]}

        if material_obj.mat_partcode not in existing_codes:
            grouped[part_code].append({
                "mat_partcode": material_obj.mat_partcode,
                "dim_qty": dim_qty,
                "loss_percent": loss_percent,
            })
        imported_rows += 1

    if not grouped:
        raise ValueError("No valid BOM rows found in the uploaded CSV.")

    for part_code, bom_rows in grouped.items():
        source_tep = _get_active_tep_for_partcode(part_code)
        replace_bom_for_partcode(
            part_code=part_code,
            rows=bom_rows,
            source_tep=source_tep,
        )

    return {
        "ok": True,
        "parts_count": len(grouped),
        "rows_count": imported_rows,
        "created_parts": created_parts,
        "created_materials": created_materials,
    }

# =========================================================
# BASIC UTILS
# =========================================================

def _to_decimal(value, places: str = "0.0001") -> Decimal:
    try:
        return Decimal(str(value or 0)).quantize(Decimal(places))
    except Exception:
        return Decimal("0.0000")


def _ceil_int(value) -> int:
    try:
        return int(
            Decimal(str(value or 0))
            .to_integral_value(rounding=ROUND_CEILING)
        )
    except Exception:
        return 0


# =========================================================
# TEP + BOM LOOKUPS
# =========================================================

def _get_active_tep_for_partcode(part_code: str):

    part_code = _normalize_partcode(part_code)

    if not part_code:
        return None

    qs = TEPCode.objects.filter(part_code=part_code)

    try:
        active = qs.filter(is_active=True).order_by("id").first()

        if active:
            return active
    except Exception:
        pass

    return qs.order_by("id").first()


def _get_active_tep_for_customer_partcode(customer_id: int, part_code: str):

    part_code = _normalize_partcode(part_code)

    if not customer_id or not part_code:
        return None

    qs = TEPCode.objects.filter(
        customer_id=customer_id,
        part_code=part_code,
    )

    try:
        active = qs.filter(is_active=True).order_by("id").first()

        if active:
            return active
    except Exception:
        pass

    return qs.order_by("id").first()


def _get_bom_rows_for_tep(tep):

    if not tep:
        return []

    part_code = getattr(tep, "part_code", "")

    if BOMMaterial and part_code:
        rows = BOMMaterial.objects.filter(
            part_code=part_code
        ).select_related("material")

        if rows.exists():
            return rows

    return Material.objects.filter(tep_code=tep)

def _get_row_total(row) -> Decimal:
    raw_total = getattr(row, "total", None)

    try:
        total = Decimal(str(raw_total))
        if total > 0:
            return total
    except Exception:
        pass

    dim_qty = _to_decimal(getattr(row, "dim_qty", 0))
    loss_percent = _to_decimal(getattr(row, "loss_percent", 0))

    computed_total = dim_qty + (dim_qty * loss_percent / Decimal("100"))
    return computed_total

def _material_row_to_dict(row):
    return {
        "mat_partcode": getattr(row, "mat_partcode", "") or "",
        "mat_partname": getattr(row, "mat_partname", "") or "",
        "mat_maker": getattr(row, "mat_maker", "") or "",
        "unit": getattr(row, "unit", "") or "",
        "per_unit_total": _get_row_total(row),
    }

def get_registered_materials_for_partcode(part_code: str) -> Tuple[Any, List[Dict[str, Any]]]:
    """
    Returns:
      tep, rows where rows look like:
      [{mat_partcode, mat_partname, mat_maker, unit, dim_qty, loss_percent, total}, ...]
    """
    part_code = _normalize_partcode(part_code)

    if not part_code:
        return None, []

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
            "total": _get_row_total(row),
        })

    return tep, rows

def get_shared_bom_rows_for_partcode(part_code: str) -> List[Dict[str, Any]]:
    part_code = _normalize_partcode(part_code)

    if not part_code:
        return []

    rows = []

    if BOMMaterial:
        bom_qs = (
            BOMMaterial.objects
            .filter(part_code=part_code)
            .select_related("material")
            .order_by("id")
        )

        if bom_qs.exists():
            for row in bom_qs:
                master = getattr(row, "material", None)

                rows.append({
                    "mat_partcode": getattr(row, "mat_partcode", "") or getattr(master, "mat_partcode", "") or "",
                    "mat_partname": getattr(row, "mat_partname", "") or getattr(master, "mat_partname", "") or "",
                    "mat_maker": getattr(row, "mat_maker", "") or getattr(master, "mat_maker", "") or "",
                    "unit": getattr(row, "unit", "") or getattr(master, "unit", "") or "",
                    "dim_qty": getattr(row, "dim_qty", 0) or 0,
                    "loss_percent": getattr(row, "loss_percent", 0) or 0,
                    "total": _get_row_total(row),
                })
            return rows

    tep = _get_active_tep_for_partcode(part_code)

    for row in _get_bom_rows_for_tep(tep):
        rows.append({
            "mat_partcode": getattr(row, "mat_partcode", "") or "",
            "mat_partname": getattr(row, "mat_partname", "") or "",
            "mat_maker": getattr(row, "mat_maker", "") or "",
            "unit": getattr(row, "unit", "") or "",
            "dim_qty": getattr(row, "dim_qty", 0) or 0,
            "loss_percent": getattr(row, "loss_percent", 0) or 0,
            "total": _get_row_total(row),
        })

    return rows

def compute_material_requirements_for_partcode(
    customer_id: int,
    part_code: str,
    forecast_qty: int,
):
    part_code = _normalize_partcode(part_code)

    tep = _get_active_tep_for_customer_partcode(customer_id, part_code)

    if not tep:
        return None, []

    grouped: Dict[str, Dict[str, Any]] = {}

    for row in _get_bom_rows_for_tep(tep):
        item = _material_row_to_dict(row)

        mat_code = item["mat_partcode"].strip()

        if not mat_code:
            continue

        if mat_code not in grouped:
            grouped[mat_code] = {
                "mat_partcode": mat_code,
                "mat_partname": item["mat_partname"],
                "mat_maker": item["mat_maker"],
                "unit": item["unit"],
                "per_unit_total": Decimal("0"),
            }

        grouped[mat_code]["per_unit_total"] += item["per_unit_total"]

    out = []

    for row in grouped.values():
        per_unit = row["per_unit_total"]
        required = (per_unit * Decimal(str(forecast_qty))).quantize(Decimal("0.00001"))

        out.append({
            "mat_partcode": row["mat_partcode"],
            "mat_partname": row["mat_partname"],
            "mat_maker": row["mat_maker"],
            "unit": row["unit"],
            "per_unit_total": per_unit,
            "required_qty": required,
        })

    return tep, out


@transaction.atomic
def run_forecast_and_save(
    inputs: List[ForecastInput],
    created_by=None,
    note: str = "",
):
    schedule_month = ""
    if inputs:
        schedule_month = (inputs[0].schedule_month or "").strip()

    run = ForecastRun.objects.create(
        note=note or "Prototype forecast run",
        created_by=created_by,
        schedule_month=schedule_month,
    )

    for item in inputs:
        tep, rows = compute_material_requirements_for_partcode(
            item.customer_id,
            item.part_code,
            item.forecast_qty,
        )

        if not tep:
            continue

        for r in rows:
            create_kwargs = {
                "run": run,
                "part_code": item.part_code,
                "forecast_qty": item.forecast_qty,
                "mat_partcode": r["mat_partcode"],
                "mat_partname": r["mat_partname"],
                "mat_maker": r["mat_maker"],
                "unit": r["unit"],
                "per_unit_total": r["per_unit_total"],
                "required_qty": r["required_qty"],
                "tep_code": tep.tep_code,
                "customer_name": item.customer_name or tep.customer.customer_name,
            }

            if hasattr(ForecastLine, "schedule_month"):
                create_kwargs["schedule_month"] = item.schedule_month or run.schedule_month

            ForecastLine.objects.create(**create_kwargs)

    return run

@transaction.atomic
def reserve_from_latest_forecast_run(created_by=None, allow_partial: bool = False) -> Dict[str, Any]:
    """
    Creates MaterialAllocation rows (status='reserved') from the latest ForecastRun.
"""
# =========================================================
# BOM SAVE
# =========================================================

@transaction.atomic
def replace_bom_for_partcode(part_code: str, rows, source_tep=None):

    if not BOMMaterial:
        raise RuntimeError("BOMMaterial not available")

    part_code = _normalize_partcode(part_code)

    BOMMaterial.objects.filter(part_code=part_code).delete()

    for row in rows:

        master = MaterialList.objects.filter(
            mat_partcode=row["mat_partcode"]
        ).first()

        if not master:
            raise ValueError(
                f"Material not found in master list: {row['mat_partcode']}"
            )

        BOMMaterial.objects.create(
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


# =========================================================
# ALLOCATION
# =========================================================

@transaction.atomic
def reserve_from_latest_forecast_run(created_by=None):

    latest = ForecastRun.objects.order_by("-id").first()

    if not latest:
        return {"ok": False, "message": "No forecast run"}

    grouped = (
        latest.lines
        .values("mat_partcode", "customer_name", "tep_code")
        .annotate(total_required=Sum("required_qty"))
    )

    for g in grouped:

        master = MaterialList.objects.filter(
            mat_partcode=g["mat_partcode"]
        ).first()

        if not master:
            continue

        cust = Customer.objects.filter(
            customer_name=g["customer_name"]
        ).first()

        if not cust:
            continue

        qty = _ceil_int(g["total_required"])

        MaterialAllocation.objects.create(
            material=master,
            customer=cust,
            qty_allocated=qty,
            forecast_ref=f"RUN:{latest.id}",
            status="reserved",
            created_by=created_by,
        )

    return {"ok": True}
