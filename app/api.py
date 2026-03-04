from ninja import NinjaAPI, File
from ninja.files import UploadedFile
from django.http import JsonResponse
from django.db import IntegrityError, transaction
from django.shortcuts import get_object_or_404
from django.db.models import Q
from django.db.models import Prefetch
import csv, io, re
from .models import Customer, TEPCode, Material, CustomerCSV, MaterialList
from .schemas import (CustomerIn, CustomerOut, CustomerFullOut, TEPCodeIn, TEPCodeOut, MaterialIn, MaterialOut, MaterialListIn)


api = NinjaAPI(title="Sales API")

def jresponse(data, status=200):
    return JsonResponse(data, status=status, safe=False)

import re

def _normalize_space(s):
    return re.sub(r"\s+", " ", (s or "").strip())

def _unique_partname_for_customer(customer, base_name, part_code):
    """
    Unique Partname per customer.parts.
    If base_name already exists for a different Partcode, returns base_name 1, base_name 2, ...
    """
    base_name = _normalize_space(base_name)
    part_code = _normalize_space(part_code)

    parts = customer.parts or []

    for p in parts:
        if isinstance(p, dict) and _normalize_space(p.get("Partcode")) == part_code:
            existing = _normalize_space(p.get("Partname"))
            return existing or base_name
        
    existing_names = set()
    for p in parts:
        if isinstance(p, dict):
            n = _normalize_space(p.get("Partname"))
            if n:
                existing_names.add(n.lower())

    if base_name.lower() not in existing_names:
        return base_name

    i = 1
    while True:
        candidate = f"{base_name} {i}"
        if candidate.lower() not in existing_names:
            return candidate
        i += 1

def _ensure_customer_part_entry(customer, part_code, part_name):
    """
    Ensures customer.parts contains Partcode.
    If not exists, adds it with unique Partname (Tape, Tape 1, Tape 2...).
    Returns (changed: bool, used_partname: str)
    """
    part_code = _normalize_space(part_code)
    part_name = _normalize_space(part_name) or part_code

    parts = customer.parts or []

    for p in parts:
        if isinstance(p, dict) and _normalize_space(p.get("Partcode")) == part_code:
            used = _normalize_space(p.get("Partname")) or part_name
            return False, used

    unique_name = _unique_partname_for_customer(customer, part_name, part_code)

    parts.append({"Partcode": part_code, "Partname": unique_name})
    customer.parts = parts
    customer.save()

    return True, unique_name

def _allocate_material_name(tep, base_name: str, exclude_partcode: str = "") -> str:
    """
    Desired behavior per TEP:
      - First insert:        TAPE
      - Second insert:       (rename existing TAPE -> TAPE 1), new -> TAPE 2
      - Third insert:        new -> TAPE 3
    """
    base = (base_name or "").strip()
    if not base:
        base = "UNKNOWN"

    exclude_partcode = (exclude_partcode or "").strip()

    qs = Material.objects.filter(
        tep_code=tep,
        mat_partname__iregex=rf"^{re.escape(base)}( \d+)?$"
    )
    if exclude_partcode:
        qs = qs.exclude(mat_partcode=exclude_partcode)

    existing_names = list(qs.values_list("mat_partname", flat=True))

    if not existing_names:
        return base

    numbers = []
    for n in existing_names:
        m = re.match(
            rf"^{re.escape(base)}(?: (\d+))?$",
            (n or "").strip(),
            flags=re.IGNORECASE
        )
        if m and m.group(1):
            numbers.append(int(m.group(1)))

    if not numbers:
        existing_base = Material.objects.filter(
            tep_code=tep,
            mat_partname__iexact=base,
        )
        if exclude_partcode:
            existing_base = existing_base.exclude(mat_partcode=exclude_partcode)

        first = existing_base.order_by("id").first()
        if first:
            first.mat_partname = f"{base} 1"
            first.save(update_fields=["mat_partname"])

        return f"{base} 2"

    return f"{base} {max(numbers) + 1}"

@api.get("/customers", tags=["CUSTOMER"])
def customers_tree(request, q: str = ""):
    """
    Returns JSON exactly like:
    {
      "customer_name": "...",
      "Customer Part": [
        {
          "Partcode": "...",
          "Partname": "...",
          "TEP Codes": [
            {
              "TEP Code": "...",
              "Materials": [...]
            }
          ]
        }
      ]
    }
    """
    qs = (
        Customer.objects
        .prefetch_related("tep_codes__materials")
        .order_by("customer_name")
    )

    if q:
        qs = qs.filter(
            Q(customer_name__icontains=q)
            | Q(tep_codes__tep_code__icontains=q)
            | Q(tep_codes__part_code__icontains=q)
            | Q(tep_codes__materials__mat_partcode__icontains=q)
            | Q(tep_codes__materials__mat_partname__icontains=q)
            | Q(tep_codes__materials__mat_maker__icontains=q)
        ).distinct()

    out = []

    for cust in qs:
        parts = cust.parts or []
        customer_parts = []

        for p in parts:
            if not isinstance(p, dict):
                continue

            partcode = (p.get("Partcode") or "").strip()
            partname = (p.get("Partname") or "").strip()
            if not partcode:
                continue

            tep_list = []
            tep_objs = [t for t in cust.tep_codes.all() if t.part_code == partcode]

            for tep in tep_objs:
                mats = []
                for m in tep.materials.all():
                    mats.append({
                        "mat_partcode": m.mat_partcode,
                        "mat_partname": m.mat_partname,
                        "mat_maker": m.mat_maker,
                        "unit": m.unit,
                        "dim_qty": m.dim_qty,
                        "loss_percent": m.loss_percent,
                        "total": m.total,
                    })

                tep_list.append({
                    "TEP Code": tep.tep_code,
                    "Materials": mats
                })

            customer_parts.append({
                "Partcode": partcode,
                "Partname": partname,
                "TEP Codes": tep_list
            })

        out.append({
            "customer_name": cust.customer_name,
            "Customer Part": customer_parts
        })

    return jresponse(out, status=200)


@api.post("/customers", response=CustomerOut, tags=["CUSTOMER"])
def create_customer(request, payload: CustomerIn):
    parts = payload.parts or []

    for i, p in enumerate(parts):
        if not p.Partcode or not p.Partname:
            return jresponse({"error": f"parts[{i}] must contain Partcode and Partname"}, status=400)

    customer = Customer.objects.create(
        customer_name=payload.customer_name,
        parts=[p.dict() for p in parts],
    )
    return customer


@api.put("/customers/{customer_id}", response=CustomerOut, tags=["CUSTOMER"])
def update_customer(request, customer_id: int, payload: CustomerIn):
    customer = get_object_or_404(Customer, id=customer_id)
    customer.customer_name = payload.customer_name
    customer.parts = [p.dict() for p in (payload.parts or [])]
    customer.save()
    return customer

@api.delete("/customers/{customer_id}", tags=["CUSTOMER"])
def delete_customer(request, customer_id: int):
    Customer.objects.filter(id=customer_id).delete()
    return jresponse({"message": "Customer deleted"})


@api.get("/customers/{customer_id}/tep-codes", response=list[TEPCodeOut], tags=["TEP"])
def list_tep_codes(request, customer_id: int, part_code: str = ""):
    customer = get_object_or_404(Customer, id=customer_id)
    qs = customer.tep_codes.all().order_by("tep_code")
    if part_code:
        qs = qs.filter(part_code=part_code)
    return qs


@api.post("/parts/{part_code}/tep-codes", response=TEPCodeOut, tags=["TEP"])
def create_tep_code_by_part_code(request, part_code: str, payload: TEPCodeIn):
    part_code = (part_code or "").strip()
    tep_code = (payload.tep_code or "").strip()

    if not part_code:
        return jresponse({"error": "part_code is required"}, status=400)
    if not tep_code:
        return jresponse({"error": "tep_code is required"}, status=400)

    customer = None
    for c in Customer.objects.all():
        parts = c.parts or []
        if any(
            isinstance(p, dict) and str(p.get("Partcode", "")).strip() == part_code
            for p in parts
        ):
            customer = c
            break

    if not customer:
        return jresponse({"error": f"part_code '{part_code}' not found in any customer.parts"}, status=404)

    tep, created = TEPCode.objects.get_or_create(
        customer=customer,
        part_code=part_code,
        tep_code=tep_code,
    )

    return tep

@api.delete("/tep-codes/{tep_code}", tags=["TEP"])
def delete_tep_code_by_code(request, tep_code: str):
    tep_code = (tep_code or "").strip()

    if not tep_code:
        return jresponse({"error": "tep_code is required"}, status=400)

    deleted_count, _ = TEPCode.objects.filter(tep_code=tep_code).delete()

    if deleted_count == 0:
        return jresponse(
            {"error": f"TEP code '{tep_code}' not found"},
            status=404
        )

    return jresponse(
        {"message": f"TEP code '{tep_code}' deleted successfully"},
        status=200
    )

@api.get("/tep-codes/{tep_code}/materials", response=list[MaterialOut], tags=["MATERIAL"])
def list_materials_by_tep_code(request, tep_code: str):
    tep_code = (tep_code or "").strip()

    if not tep_code:
        return jresponse({"error": "tep_code is required"}, status=400)

    tep = get_object_or_404(TEPCode, tep_code=tep_code)

    return tep.materials.all().order_by("mat_partname")



@api.post("/tep-codes/by-code/{tep_code}/materials", response=MaterialOut, tags=["MATERIAL"])
def create_material_by_tep_code(
    request,
    tep_code: str,
    payload: MaterialIn,
    part_code: str = "",
    customer_name: str = "",
):
    tep_code = (tep_code or "").strip()
    part_code = (part_code or "").strip()
    customer_name = (customer_name or "").strip()

    if not tep_code:
        return jresponse({"error": "tep_code is required"}, status=400)

    qs = TEPCode.objects.select_related("customer").filter(tep_code=tep_code)

    if part_code:
        qs = qs.filter(part_code=part_code)
    if customer_name:
        qs = qs.filter(customer__customer_name=customer_name)
    tep = qs.first()
    if not tep:
        return jresponse(
            {"error": "TEP code not found. Provide part_code and/or customer_name."},
            status=404,
        )

    mat_partcode = (payload.mat_partcode or "").strip()
    if not mat_partcode:
        return jresponse({"error": "mat_partcode is required"}, status=400)

    master = MaterialList.objects.filter(mat_partcode=mat_partcode).first()
    if not master:
        return jresponse(
            {"error": f"mat_partcode '{mat_partcode}' not found in master list."},
            status=404,
        )
    
    loss = payload.loss_percent if payload.loss_percent is not None else 10.0
    total = round(float(payload.dim_qty) * (1 + (float(loss) / 100.0)), 4)

    with transaction.atomic():
        final_name = _allocate_material_name(
            tep=tep,
            base_name=master.mat_partname,
            exclude_partcode=mat_partcode
        )

    with transaction.atomic():
        final_name = _allocate_material_name

    material, created = Material.objects.get_or_create(
        tep_code=tep,
        mat_partcode=mat_partcode,
        defaults={
            "mat_partname": final_name,
            "mat_maker": master.mat_maker,
            "unit": master.unit,
            "dim_qty": payload.dim_qty,
            "loss_percent": loss,
            "total": total,
        }
    )

    if not created:
        return jresponse({"error": "Material already exists for this TEP + mat_partcode."}, status=409)

    return material


@api.put("/tep-codes/{tep_code}/materials/{mat_partcode}",
    response=MaterialOut,
    tags=["MATERIAL"]
)
def update_material_by_tep_and_partcode(
    request,
    tep_code: str,
    mat_partcode: str,
    payload: MaterialIn
):
    tep_code = (tep_code or "").strip()
    mat_partcode = (mat_partcode or "").strip()

    if not tep_code:
        return jresponse({"error": "tep_code is required"}, status=400)

    if not mat_partcode:
        return jresponse({"error": "mat_partcode is required"}, status=400)

    material = get_object_or_404(
        Material,
        tep_code__tep_code=tep_code,
        mat_partcode=mat_partcode
    )

    material.mat_partcode = payload.mat_partcode
    material.mat_partname = payload.mat_partname
    material.mat_maker = payload.mat_maker
    material.unit = payload.unit
    material.dim_qty = payload.dim_qty
    material.loss_percent = payload.loss_percent
    material.total = payload.total

    material.save()

    return material



@api.delete("/tep-codes/{tep_code}/materials/{mat_partcode}", tags=["MATERIAL"])
def delete_material_by_tep_and_partcode(request, tep_code: str, mat_partcode: str):
    tep_code = (tep_code or "").strip()
    mat_partcode = (mat_partcode or "").strip()

    if not tep_code:
        return jresponse({"error": "tep_code is required"}, status=400)

    if not mat_partcode:
        return jresponse({"error": "mat_partcode is required"}, status=400)

    deleted_count, _ = Material.objects.filter(
        tep_code__tep_code=tep_code,
        mat_partcode=mat_partcode
    ).delete()

    if deleted_count == 0:
        return jresponse(
            {
                "error": f"No material found for tep_code '{tep_code}' "
                         f"and mat_partcode '{mat_partcode}'"
            },
            status=404
        )

    return jresponse(
        {
            "message": "Material deleted successfully",
            "tep_code": tep_code,
            "mat_partcode": mat_partcode,
            "deleted_records": deleted_count
        },
        status=200
    )

@api.post("/upload-csv", tags=["CSV"])
def upload_csv(request, file: UploadedFile = File(...)):
    if not file:
        return jresponse({"error": "No file uploaded."}, status=400)

    try:
        content = file.read().decode("utf-8", errors="ignore")
        csv_file = io.StringIO(content)
        reader = csv.DictReader(csv_file)

        reader.fieldnames = [h.strip().lstrip("\ufeff") for h in (reader.fieldnames or [])]

        inserted = 0
        updated = 0
        master_inserted = 0
        master_updated = 0

        ALLOWED_UNITS = {"pc", "pcs", "m", "g", "kg"}

        def fnum(x, default=0.0):
            try:
                if x is None:
                    return float(default)
                s = str(x).strip()
                if s == "":
                    return float(default)
                return float(s)
            except Exception:
                return float(default)

        def sget(row, *keys, default=""):
            for k in keys:
                v = row.get(k)
                if v is not None and str(v).strip() != "":
                    return str(v).strip()
            return default

        with transaction.atomic():
            try:
                CustomerCSV.objects.create(csv_file=file)
            except Exception:
                pass

            for row in reader:
                mat_partcode = sget(row, "mat_partcode", "material_part_code")
                mat_partname = sget(row, "mat_partname", "material_name")
                mat_maker = sget(row, "mat_maker", "maker")
                unit = sget(row, "unit", default="pc").lower()

                if unit not in ALLOWED_UNITS:
                    unit = "pc"

                if not mat_partcode:
                    continue

                master, created_master = MaterialList.objects.get_or_create(
                    mat_partcode=mat_partcode,
                    defaults={
                        "mat_partname": mat_partname or mat_partcode,
                        "mat_maker": mat_maker or "Unknown",
                        "unit": unit,
                    }
                )

                if created_master:
                    master_inserted += 1
                else:
                    changed = False
                    if mat_partname and master.mat_partname != mat_partname:
                        master.mat_partname = mat_partname
                        changed = True
                    if mat_maker and master.mat_maker != mat_maker:
                        master.mat_maker = mat_maker
                        changed = True
                    if unit and master.unit != unit:
                        master.unit = unit
                        changed = True
                    if changed:
                        master.save()
                        master_updated += 1

                customer_name = sget(row, "customer_name")
                partcode = sget(row, "Partcode", "part_code")
                partname = sget(row, "Partname", "part_name")
                tep_code = sget(row, "tep_code")

                dim_qty = fnum(row.get("dim_qty"), 0.0)
                loss_percent = fnum(row.get("loss_percent"), 10.0)

                total_csv = row.get("total")
                if total_csv is None or str(total_csv).strip() == "":
                    total = round(float(dim_qty) * (1 + (float(loss_percent) / 100.0)), 4)
                else:
                    total = round(fnum(total_csv, 0.0), 4)

                if not (customer_name and partcode and partname and tep_code):
                    continue

                customer, _ = Customer.objects.get_or_create(customer_name=customer_name)

                parts = customer.parts or []
                exists = any(
                    isinstance(p, dict) and str(p.get("Partcode", "")).strip() == partcode
                    for p in parts
                )
                if not exists:
                    parts.append({"Partcode": partcode, "Partname": partname})
                    customer.parts = parts
                    customer.save()

                tep, _ = TEPCode.objects.get_or_create(
                    customer=customer,
                    part_code=partcode,
                    tep_code=tep_code,
                )

                with transaction.atomic():
                    existing_mat = Material.objects.filter(
                        tep_code=tep,
                        mat_partcode=master.mat_partcode
                    ).first()

                    if existing_mat:
                        if dim_qty != 0:
                            existing_mat.dim_qty = dim_qty
                        if loss_percent != 0:
                            existing_mat.loss_percent = loss_percent

                        if total_csv is None or str(total_csv).strip() == "":
                            existing_mat.total = round(
                                float(existing_mat.dim_qty) * (1 + (float(existing_mat.loss_percent) / 100.0)),
                                4
                            )
                        else:
                            existing_mat.total = total

                        existing_mat.mat_maker = master.mat_maker
                        existing_mat.unit = master.unit
                        existing_mat.save()

                        updated += 1
                        continue

                    final_name = _allocate_material_name(
                        tep=tep,
                        base_name=master.mat_partname,
                        exclude_partcode=master.mat_partcode
                    )

                    Material.objects.create(
                        tep_code=tep,
                        mat_partcode=master.mat_partcode,
                        mat_partname=final_name,
                        mat_maker=master.mat_maker,
                        unit=master.unit,
                        dim_qty=dim_qty,
                        loss_percent=loss_percent,
                        total=total,
                    )
                    inserted += 1

        return jresponse(
            {
                "message": "CSV uploaded successfully",
                "master_inserted": master_inserted,
                "master_updated": master_updated,
                "inserted_materials": inserted,
                "updated_materials": updated,
            },
            status=200
        )

    except Exception as e:
        return jresponse({"error": str(e)}, status=500)

  
@api.get("/output-format", tags=["GET DETAILS"])
def output_format(request):
    customers = Customer.objects.prefetch_related(
        Prefetch(
            "tep_codes",
            queryset=TEPCode.objects.prefetch_related(
                Prefetch("materials", queryset=Material.objects.all().order_by("mat_partname"))
            ).all()
        )
    ).all().order_by("customer_name")

    result = []

    for customer in customers:
        teps_by_part = {}
        for tep in customer.tep_codes.all():
            teps_by_part.setdefault(tep.part_code, []).append(tep)

        parts_out = []
        for p in (customer.parts or []):
            if not isinstance(p, dict):
                continue

            partcode = str(p.get("Partcode", "")).strip()
            partname = str(p.get("Partname", "")).strip()

            if not partcode:
                continue

            tep_codes_out = []
            for tep in teps_by_part.get(partcode, []):
                mats_out = []
                for m in tep.materials.all():
                    mats_out.append({
                        "mat_partcode": m.mat_partcode,
                        "mat_partname": m.mat_partname,
                        "mat_maker": m.mat_maker,
                        "unit": m.unit,
                        "dim_qty": m.dim_qty,
                        "loss_percent": m.loss_percent,
                        "total": m.total,
                    })
                tep_codes_out.append({
                    "TEP Code": tep.tep_code,
                    "Materials": mats_out
                })
            parts_out.append({
                "Partcode": partcode,
                "Partname": partname,
                "TEP Codes": tep_codes_out
            })
        result.append({
            "customer_name": customer.customer_name,
            "Customer Part": parts_out
        })
    return jresponse(result)


@api.post("/master/materials", tags=["MASTER LIST"])
def create_master_material(request, payload: MaterialListIn):
    code = (payload.mat_partcode or "").strip()

    if not code:
        return jresponse({"error": "mat_partcode is required"}, status=400)
    
    obj, created = MaterialList.objects.get_or_create(
        mat_partcode=code,
        defaults = {
            "mat_partname": (payload.mat_partname or "").strip(),
            "mat_maker": (payload.mat_maker or "").strip(),
            "unit": (payload.unit or "").strip(),
        }
    )
    if not created:
        return jresponse({"error": "mat_partcode already exists in master list"}, status=409)
    return jresponse(
        {
            "message": "Master material created",
            "mat_partcode": obj.mat_partcode,
            "mat_partname": obj.mat_partname,
            "mat_maker": obj.mat_maker,
            "unit": obj.unit,
        },
        status=201
    )


