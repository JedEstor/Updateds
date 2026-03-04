from django.views.decorators.cache import never_cache
import json, csv, io, re
from collections import defaultdict
from django.views.decorators.http import require_POST
from django.core.paginator import Paginator
from django.db import transaction, IntegrityError
from django.db.models import Q, Sum
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render, get_object_or_404, redirect
from django.contrib import messages
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.models import User
from django.contrib.auth.decorators import login_required, user_passes_test
from django.urls import reverse
from django.utils.http import url_has_allowed_host_and_scheme
from django.views.decorators.http import require_GET
from django.utils import timezone
from .models import (
    Customer,
    TEPCode,
    Material,
    MaterialList,
    MaterialStock,
    MaterialAllocation,
    ForecastRun,
    ForecastLine,
)

from .forms import EmployeeCreateForm
from .service import prototype_static_run, reserve_from_latest_forecast_run


def is_admin(user):
    return user.is_authenticated and user.is_superuser


def can_edit(user):
    return user.is_authenticated and user.is_staff


def home(request):
    return HttpResponse("Welcome to the Home Page!")


def login_view(request):
    error = ""

    if request.method == "POST":
        employee_id = (request.POST.get("employee_id") or "").strip()
        password = request.POST.get("password") or ""

        user = authenticate(request, username=employee_id, password=password)

        if user is not None and user.is_active:
            login(request, user)

            if user.is_superuser:
                return redirect("app:admin_dashboard")

            if user.is_staff:
                return redirect("app:customer_list")

            return redirect("app:customer_list")
        else:
            error = "Invalid Employee ID or password"

    return render(request, "login.html", {"error": error})


def _normalize_space(s):
    return re.sub(r"\s+", " ", (s or "").strip())


def _unique_partname_for_customer(customer, base_name, part_code):
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
    customer.save(update_fields=["parts"])
    return True, unique_name


def _allocate_material_name(tep, base_name: str, exclude_partcode: str = "") -> str:
    """
    Desired behavior per TEP:
      - First insert:        BASE
      - Second insert:       (rename existing BASE -> BASE 1), new -> BASE 2
      - Third insert:        new -> BASE 3
    """
    base = (base_name or "").strip() or "UNKNOWN"
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
        m = re.match(rf"^{re.escape(base)}(?: (\d+))?$", (n or "").strip(), flags=re.IGNORECASE)
        if m and m.group(1):
            numbers.append(int(m.group(1)))

    if not numbers:
        existing_base = Material.objects.filter(tep_code=tep, mat_partname__iexact=base)
        if exclude_partcode:
            existing_base = existing_base.exclude(mat_partcode=exclude_partcode)

        first = existing_base.order_by("id").first()
        if first:
            first.mat_partname = f"{base} 1"
            first.save(update_fields=["mat_partname"])

        return f"{base} 2"

    return f"{base} {max(numbers) + 1}"


def _next_tep_code(existing_tep_code: str) -> str:
    """
    "same prefix, increment last number"
    Examples:
      TEP00-01 -> TEP00-02
      TEP00-09 -> TEP00-10
      ABC      -> ABC-01
    Keeps zero-padding based on existing last number width.
    """
    s = (existing_tep_code or "").strip()
    if not s:
        return "TEP-01"

    m = re.match(r"^(.*?)(\d+)$", s)
    if not m:
        return f"{s}-01"

    prefix = m.group(1)
    num_str = m.group(2)
    width = len(num_str)

    try:
        n = int(num_str)
    except Exception:
        return f"{s}-01"

    return f"{prefix}{str(n + 1).zfill(width)}"


def _generate_unique_next_tep_code(base_tep_code: str) -> str:
    """
    Keep incrementing until tep_code is unique in DB.
    """
    candidate = _next_tep_code(base_tep_code)
    guard = 0
    while TEPCode.objects.filter(tep_code=candidate).exists():
        candidate = _next_tep_code(candidate)
        guard += 1
        if guard > 500:
            raise ValueError("Could not generate unique tep_code after many attempts.")
    return candidate


def build_customer_table(q: str):
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

    grouped = defaultdict(lambda: {
        "parts_by_code": {},
        "teps_by_part": defaultdict(list),
    })

    for cust in qs:
        name = cust.customer_name

        for p in cust.parts or []:
            if not isinstance(p, dict):
                continue
            pc = (p.get("Partcode") or "").strip()
            pn = (p.get("Partname") or "").strip()
            if pc and pc not in grouped[name]["parts_by_code"]:
                grouped[name]["parts_by_code"][pc] = pn

        for tep in cust.tep_codes.all():
            grouped[name]["teps_by_part"][tep.part_code].append(tep)

    customers = []

    for name, g in grouped.items():
        parts_by_code = g["parts_by_code"]
        teps_by_part = g["teps_by_part"]

        part_code_options = sorted(parts_by_code.keys())
        part_code_map = {}

        for pc in part_code_options:
            tep_objs = teps_by_part.get(pc, [])
            tep_objs = sorted(tep_objs, key=lambda t: (not bool(getattr(t, "is_active", True)), t.tep_code))

            teps = [
                {
                    "tep_id": t.id,
                    "tep_code": t.tep_code,
                    "materials_count": t.materials.count(),
                    "is_active": getattr(t, "is_active", True),
                }
                for t in tep_objs
            ]

            default_tep = teps[0] if teps else None

            part_code_map[pc] = {
                "part_name": parts_by_code.get(pc, ""),
                "teps": teps,
                "default_tep_id": default_tep["tep_id"] if default_tep else None,
                "default_tep_code": default_tep["tep_code"] if default_tep else "",
                "default_materials_count": default_tep["materials_count"] if default_tep else 0,
            }

        default_pc = part_code_options[0] if part_code_options else ""

        customers.append({
            "customer_name": name,
            "part_code_options": part_code_options,
            "default_part_code": default_pc,

            "default_tep_options": part_code_map.get(default_pc, {}).get("teps", []),
            "default_tep_id": part_code_map.get(default_pc, {}).get("default_tep_id"),
            "default_tep_code": part_code_map.get(default_pc, {}).get("default_tep_code", ""),
            "default_materials_count": part_code_map.get(default_pc, {}).get("default_materials_count", 0),

            "part_code_map_json": json.dumps(part_code_map, ensure_ascii=False),
        })

    return customers


@require_POST
@login_required
@user_passes_test(is_admin)
def update_material_stock(request):
    material_id = (request.POST.get("material_id") or "").strip()
    on_hand_qty_raw = (request.POST.get("on_hand_qty") or "").strip()
    sq = (request.GET.get("sq") or request.POST.get("sq") or "").strip()

    if not material_id:
        messages.error(request, "Missing material_id.")
        return redirect(reverse("app:admin_dashboard") + "?tab=stocks")

    try:
        on_hand_qty = int(on_hand_qty_raw) if on_hand_qty_raw != "" else 0
        if on_hand_qty < 0:
            on_hand_qty = 0
    except Exception:
        messages.error(request, "On hand qty must be a whole number.")
        return redirect(reverse("app:admin_dashboard") + "?tab=stocks")

    mat = get_object_or_404(MaterialList, id=material_id)

    try:
        MaterialStock.objects.update_or_create(
            material=mat,
            defaults={
                "on_hand_qty": on_hand_qty,
                "last_updated_by": request.user,
            }
        )
        messages.success(request, f"Saved stock: {mat.mat_partcode} = {on_hand_qty}")
    except Exception as e:
        messages.error(request, f"Failed to save stock: {e}")

    url = reverse("app:admin_dashboard") + "?tab=stocks"
    if sq:
        url += f"&sq={sq}"
    return redirect(url)


@never_cache
@login_required
@user_passes_test(is_admin)
def admin_dashboard(request):
    tab = (request.GET.get("tab") or "customers").strip().lower()
    action = ""  

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()

        if action == "run_prototype_forecast":
            prototype_static_run(created_by=request.user)
            messages.success(request, "Prototype forecast run created (5000 / 10000).")
            return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

        if action == "reserve_from_latest_run":
            allow_partial = (request.POST.get("allow_partial") == "1")
            result = reserve_from_latest_forecast_run(created_by=request.user, allow_partial=allow_partial)

            if not result.get("ok"):
                messages.error(request, result.get("message") or "Reserve failed.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            messages.success(
                request,
                f"{result.get('message')} Created={result.get('created')} Skipped={result.get('skipped')}"
            )
            for note in (result.get("notes") or []):
                messages.info(request, note)

            return redirect(reverse("app:admin_dashboard") + "?tab=stocks")

        if action == "allocate_from_run":
            run_id = (request.POST.get("run_id") or "").strip()
            forecast_ref = (request.POST.get("forecast_ref") or "").strip()

            if not run_id:
                messages.error(request, "Missing run_id.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            run = get_object_or_404(ForecastRun, id=run_id)
            if not forecast_ref:
                forecast_ref = f"RUN-{run.id}"

            req_rows = (
                ForecastLine.objects
                .filter(run=run)
                .exclude(mat_partcode="(NO TEP FOUND)")
                .values("mat_partcode")
                .annotate(total_required=Sum("required_qty"))
                .order_by("mat_partcode")
            )

            created = 0
            skipped = 0

            pool_customer, _ = Customer.objects.get_or_create(customer_name="FORECAST_POOL")

            for row in req_rows:
                mat_partcode = (row.get("mat_partcode") or "").strip()
                if not mat_partcode:
                    skipped += 1
                    continue

                mat = MaterialList.objects.filter(mat_partcode=mat_partcode).first()
                if not mat:
                    skipped += 1
                    continue

                total_required = row.get("total_required") or 0
                try:
                    needed = int(__import__("math").ceil(float(total_required)))
                except Exception:
                    needed = 0

                if needed <= 0:
                    skipped += 1
                    continue

                try:
                    on_hand = int(mat.stock.on_hand_qty or 0)
                except Exception:
                    on_hand = 0

                reserved = (
                    MaterialAllocation.objects
                    .filter(material=mat, status="reserved")
                    .aggregate(total=Sum("qty_allocated"))
                    .get("total") or 0
                )
                reserved = int(reserved or 0)
                available = max(on_hand - reserved, 0)

                if available <= 0:
                    skipped += 1
                    continue

                reserve_qty = min(available, needed)

                try:
                    MaterialAllocation.objects.create(
                        material=mat,
                        customer=pool_customer,
                        tep_code=None,
                        qty_allocated=reserve_qty,
                        forecast_ref=forecast_ref,
                        status="reserved",
                        created_by=request.user,
                    )
                    created += 1
                except Exception:
                    skipped += 1

            messages.success(request, f"Allocated/Reserved for {forecast_ref}. Created={created} Skipped={skipped}")
            return redirect(reverse("app:admin_dashboard") + "?tab=stocks")

        if action == "release_allocations_ref":
            forecast_ref = (request.POST.get("forecast_ref") or "").strip()
            if not forecast_ref:
                messages.error(request, "Missing forecast_ref.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            updated = (
                MaterialAllocation.objects
                .filter(forecast_ref=forecast_ref, status="reserved")
                .update(status="released")
            )
            messages.success(request, f"Released {updated} reserved allocations for ref={forecast_ref}.")
            return redirect(reverse("app:admin_dashboard") + "?tab=stocks")

        if action == "fulfill_allocations_ref":
            forecast_ref = (request.POST.get("forecast_ref") or "").strip()
            if not forecast_ref:
                messages.error(request, "Missing forecast_ref.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            updated = (
                MaterialAllocation.objects
                .filter(forecast_ref=forecast_ref, status="reserved")
                .update(status="fulfilled")
            )
            messages.success(request, f"Fulfilled {updated} reserved allocations for ref={forecast_ref}.")
            return redirect(reverse("app:admin_dashboard") + "?tab=stocks")

        if action == "revise_tep":
            tep_id = (request.POST.get("tep_id") or "").strip()
            if not tep_id:
                messages.error(request, "Missing tep_id.")
                return redirect(reverse("app:admin_dashboard") + "?tab=customers")

            old_tep = get_object_or_404(TEPCode.objects.select_related("customer"), id=tep_id)

            if not getattr(old_tep, "is_active", True):
                messages.error(request, f"{old_tep.tep_code} is already obsolete.")
                return redirect(reverse("app:admin_dashboard") + f"?tab=customers&open_panel=1&tep_id={old_tep.id}")

            try:
                with transaction.atomic():
                    new_code = _generate_unique_next_tep_code(old_tep.tep_code)

                    new_tep = TEPCode.objects.create(
                        customer=old_tep.customer,
                        part_code=old_tep.part_code,
                        tep_code=new_code,
                        is_active=True,
                    )

                    old_tep.is_active = False
                    old_tep.superseded_by = new_tep
                    old_tep.revised_at = timezone.now()
                    old_tep.save(update_fields=["is_active", "superseded_by", "revised_at"])

                messages.success(request, f"Revised: {old_tep.tep_code} → {new_tep.tep_code} (new TEP is empty)")
                return redirect(reverse("app:admin_dashboard") + f"?tab=customers&open_panel=1&tep_id={new_tep.id}")

            except Exception as e:
                messages.error(request, f"Failed to revise TEP: {e}")
                return redirect(reverse("app:admin_dashboard") + f"?tab=customers&open_panel=1&tep_id={old_tep.id}")

        if action == "add_customer_full":
            customer_name = _normalize_space(request.POST.get("customer_name"))
            part_code = _normalize_space(request.POST.get("part_code"))
            part_name = _normalize_space(request.POST.get("part_name"))
            tep_code = _normalize_space(request.POST.get("tep_code"))

            if not customer_name:
                messages.error(request, "Customer Name is required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=customers")
            if not part_code:
                messages.error(request, "Partcode is required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=customers")
            if not part_name:
                messages.error(request, "Partname is required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=customers")
            if not tep_code:
                messages.error(request, "TEP Code is required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=customers")

            try:
                with transaction.atomic():
                    customer, _ = Customer.objects.get_or_create(customer_name=customer_name)
                    _ensure_customer_part_entry(customer, part_code, part_name)

                    TEPCode.objects.create(
                        customer=customer,
                        part_code=part_code,
                        tep_code=tep_code,
                        is_active=True,
                    )

                messages.success(request, f"Saved customer record: {customer_name} | {part_code} | {tep_code}")

            except IntegrityError as e:
                msg = str(e).lower()
                if "tepcode.tep_code" in msg or "app_tepcode.tep_code" in msg:
                    messages.error(request, "TEP Code already exists.")
                elif "customer.customer_name" in msg or "app_customer.customer_name" in msg:
                    messages.error(request, "Customer name already exists.")
                else:
                    messages.error(request, "Failed to save customer record.")
            except Exception as e:
                messages.error(request, f"Failed to save customer record: {e}")

            return redirect(reverse("app:admin_dashboard") + "?tab=customers")

        if action == "add_material":
            mat_partcode = (request.POST.get("mat_partcode") or "").strip()
            mat_partname = (request.POST.get("mat_partname") or "").strip()
            mat_maker = (request.POST.get("mat_maker") or "").strip()
            unit = (request.POST.get("unit") or "").strip().lower()

            allowed_units = {"pc", "pcs", "m", "g", "kg"}
            if unit not in allowed_units:
                unit = "pc"

            if not mat_partcode:
                messages.error(request, "Part Code is required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=materials")

            try:
                obj, created = MaterialList.objects.get_or_create(
                    mat_partcode=mat_partcode,
                    defaults={
                        "mat_partname": mat_partname or mat_partcode,
                        "mat_maker": mat_maker or "Unknown",
                        "unit": unit,
                    }
                )

                if created:
                    messages.success(request, f"Added material: {mat_partcode}")
                else:
                    changed = False
                    if mat_partname and obj.mat_partname != mat_partname:
                        obj.mat_partname = mat_partname
                        changed = True
                    if mat_maker and obj.mat_maker != mat_maker:
                        obj.mat_maker = mat_maker
                        changed = True
                    if unit and obj.unit != unit:
                        obj.unit = unit
                        changed = True

                    if changed:
                        obj.save()
                        messages.success(request, f"Updated material: {mat_partcode}")
                    else:
                        messages.info(request, f"No changes for: {mat_partcode}")

            except Exception as e:
                messages.error(request, f"Failed to save material: {e}")

            return redirect(reverse("app:admin_dashboard") + "?tab=materials")

        if action == "update_material":
            mat_id = (request.POST.get("mat_id") or "").strip()
            mat_partcode = (request.POST.get("mat_partcode") or "").strip()
            mat_partname = (request.POST.get("mat_partname") or "").strip()
            mat_maker = (request.POST.get("mat_maker") or "").strip()
            unit = (request.POST.get("unit") or "").strip().lower()

            allowed_units = {"pc", "pcs", "m", "g", "kg"}
            if unit not in allowed_units:
                unit = "pc"

            if not mat_id:
                messages.error(request, "Missing material ID.")
                return redirect(reverse("app:admin_dashboard") + "?tab=materials")

            try:
                obj = MaterialList.objects.get(id=mat_id)

                if not mat_partcode:
                    messages.error(request, "Part Code is required.")
                    return redirect(reverse("app:admin_dashboard") + "?tab=materials")

                if mat_partcode != obj.mat_partcode:
                    if MaterialList.objects.filter(mat_partcode=mat_partcode).exclude(id=obj.id).exists():
                        messages.error(request, f"Part Code already exists: {mat_partcode}")
                        return redirect(reverse("app:admin_dashboard") + "?tab=materials")

                obj.mat_partcode = mat_partcode
                obj.mat_partname = mat_partname or mat_partcode
                obj.mat_maker = mat_maker or "Unknown"
                obj.unit = unit
                obj.save()

                messages.success(request, f"Saved changes: {obj.mat_partcode}")

            except MaterialList.DoesNotExist:
                messages.error(request, "Material not found.")
            except Exception as e:
                messages.error(request, f"Failed to update: {e}")

            return redirect(reverse("app:admin_dashboard") + "?tab=materials")

        if action == "delete_material":
            mat_id = (request.POST.get("mat_id") or "").strip()

            if not mat_id:
                messages.error(request, "Missing material ID.")
                return redirect(reverse("app:admin_dashboard") + "?tab=materials")

            try:
                obj = MaterialList.objects.get(id=mat_id)
                code = obj.mat_partcode
                obj.delete()
                messages.success(request, f"Deleted material: {code}")
            except MaterialList.DoesNotExist:
                messages.error(request, "Material not found.")
            except Exception as e:
                messages.error(request, f"Failed to delete: {e}")

            return redirect(reverse("app:admin_dashboard") + "?tab=materials")

        if action == "add_employee":
            employee_id = (request.POST.get("employee_id") or "").strip()
            full_name = (request.POST.get("full_name") or "").strip()
            department = (request.POST.get("department") or "").strip()
            password = (request.POST.get("password") or "")

            if not employee_id or not full_name or not department or not password:
                messages.error(request, "All fields are required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=users")

            if User.objects.filter(username=employee_id).exists():
                messages.error(request, f"Employee ID already exists: {employee_id}")
                return redirect(reverse("app:admin_dashboard") + "?tab=users")

            try:
                user = User.objects.create_user(username=employee_id, password=password)
                user.is_staff = True
                user.is_superuser = False
                user.save()

                try:
                    from .models import EmployeeProfile
                    EmployeeProfile.objects.create(
                        user=user,
                        employee_id=employee_id,
                        full_name=full_name,
                        department=department
                    )
                except Exception:
                    pass

                messages.success(request, f"Employee created: {employee_id}")

            except Exception as e:
                messages.error(request, f"Failed to create employee: {e}")

            return redirect(reverse("app:admin_dashboard") + "?tab=users")

        if action == "toggle_user_active":
            user_id = (request.POST.get("user_id") or "").strip()
            if not user_id:
                messages.error(request, "Missing user ID.")
                return redirect(reverse("app:admin_dashboard") + "?tab=users")

            try:
                u = User.objects.get(id=user_id)
                if u.id == request.user.id:
                    messages.error(request, "You can't disable your own account.")
                    return redirect(reverse("app:admin_dashboard") + "?tab=users")

                u.is_active = not u.is_active
                u.save(update_fields=["is_active"])
                messages.success(request, f"Updated user: {u.username} (active={u.is_active})")
            except User.DoesNotExist:
                messages.error(request, "User not found.")
            except Exception as e:
                messages.error(request, f"Failed to update user: {e}")

            return redirect(reverse("app:admin_dashboard") + "?tab=users")

        if action == "toggle_user_admin":
            user_id = (request.POST.get("user_id") or "").strip()
            if not user_id:
                messages.error(request, "Missing user ID.")
                return redirect(reverse("app:admin_dashboard") + "?tab=users")

            try:
                u = User.objects.get(id=user_id)
                if u.id == request.user.id:
                    messages.error(request, "You can't change your own admin role here.")
                    return redirect(reverse("app:admin_dashboard") + "?tab=users")

                if u.is_superuser:
                    u.is_superuser = False
                    u.is_staff = True
                    u.save(update_fields=["is_superuser", "is_staff"])
                    messages.success(request, f"{u.username} is now Staff.")
                else:
                    u.is_superuser = True
                    u.is_staff = True
                    u.save(update_fields=["is_superuser", "is_staff"])
                    messages.success(request, f"{u.username} is now Admin.")

            except User.DoesNotExist:
                messages.error(request, "User not found.")
            except Exception as e:
                messages.error(request, f"Failed to update role: {e}")

            return redirect(reverse("app:admin_dashboard") + "?tab=users")

        if action == "remove_staff":
            user_id = (request.POST.get("user_id") or "").strip()
            if not user_id:
                messages.error(request, "Missing user ID.")
                return redirect(reverse("app:admin_dashboard") + "?tab=users")

            try:
                u = User.objects.get(id=user_id)
                if u.id == request.user.id:
                    messages.error(request, "You can't delete your own account.")
                    return redirect(reverse("app:admin_dashboard") + "?tab=users")

                try:
                    prof = getattr(u, "employeeprofile", None)
                    if prof is not None:
                        prof.delete()
                except Exception:
                    pass

                username = u.username
                u.delete()
                messages.success(request, f"Deleted user: {username}")

            except User.DoesNotExist:
                messages.error(request, "User not found.")
            except Exception as e:
                messages.error(request, f"Failed to delete user: {e}")

            return redirect(reverse("app:admin_dashboard") + "?tab=users")

    q = (request.GET.get("q") or "").strip()
    customers = build_customer_table(q)

    master_map = {
        m["mat_partcode"]: {
            "mat_partname": m["mat_partname"],
            "mat_maker": m["mat_maker"],
            "unit": m["unit"],
        }
        for m in MaterialList.objects.all().values("mat_partcode", "mat_partname", "mat_maker", "unit")
    }

    mq = (request.GET.get("mq") or "").strip()
    materials_qs = MaterialList.objects.all().order_by("mat_partcode")
    if mq:
        materials_qs = materials_qs.filter(
            Q(mat_partcode__icontains=mq) |
            Q(mat_partname__icontains=mq) |
            Q(mat_maker__icontains=mq) |
            Q(unit__icontains=mq)
        )

    paginator = Paginator(materials_qs, 8)
    page_number = request.GET.get("page")
    page_obj = paginator.get_page(page_number)

    material_total = materials_qs.count()
    material_list = page_obj

    uq = (request.GET.get("uq") or "").strip()
    users_qs = User.objects.all().order_by("-is_superuser", "-is_staff", "username")
    if uq:
        users_qs = users_qs.filter(
            Q(username__icontains=uq) |
            Q(employeeprofile__full_name__icontains=uq) |
            Q(employeeprofile__department__icontains=uq)
        )

    users_paginator = Paginator(users_qs, 10)
    upage = request.GET.get("upage")
    users_page = users_paginator.get_page(upage)
    user_total = users_qs.count()

    sq = (request.GET.get("sq") or "").strip()
    materials_master_qs = MaterialList.objects.all().order_by("mat_partcode")
    if sq:
        materials_master_qs = materials_master_qs.filter(
            Q(mat_partcode__icontains=sq) |
            Q(mat_partname__icontains=sq) |
            Q(mat_maker__icontains=sq)
        )

    materials_master_qs = materials_master_qs.select_related("stock", "stock__last_updated_by")

    stock_paginator = Paginator(materials_master_qs, 8)
    spage = request.GET.get("spage")
    stock_page_obj = stock_paginator.get_page(spage)

    materials_master = stock_page_obj

    mat_ids = [m.id for m in materials_master]
    reserved_map = {
        row["material_id"]: (row["total"] or 0)
        for row in (
            MaterialAllocation.objects
            .filter(material_id__in=mat_ids, status="reserved")
            .values("material_id")
            .annotate(total=Sum("qty_allocated"))
        )
    }

    for m in materials_master:
        try:
            s = m.stock
            m.on_hand_qty = s.on_hand_qty
            m.last_updated_at = s.last_updated_at
            m.last_updated_by = s.last_updated_by
        except Exception:
            m.on_hand_qty = 0
            m.last_updated_at = None
            m.last_updated_by = None

        m.reserved_qty = int(reserved_map.get(m.id, 0) or 0)
        m.available_qty = max(int(m.on_hand_qty or 0) - int(m.reserved_qty or 0), 0)

    tep_id = request.GET.get("tep_id")
    is_ajax = request.headers.get("x-requested-with") == "XMLHttpRequest"

    if tep_id and is_ajax:
        tep = get_object_or_404(TEPCode.objects.select_related("customer", "superseded_by"), id=tep_id)
        materials = Material.objects.filter(tep_code=tep).order_by("mat_partname")

        selected_part = (tep.part_code or "").strip()
        selected_part_name = ""
        for p in (tep.customer.parts or []):
            if isinstance(p, dict) and str(p.get("Partcode", "")).strip() == selected_part:
                selected_part_name = str(p.get("Partname", "")).strip()
                break

        return render(request, "admin/_customer_detail_panel.html", {
            "customer": tep.customer,
            "materials": materials,
            "selected_tep": tep.tep_code,
            "selected_part": selected_part,
            "selected_part_name": selected_part_name,
            "tep_id": tep.id,
            "tep_obj": tep,
        })

    fq = (request.GET.get("fq") or "").strip()
    forecast_latest = ForecastRun.objects.order_by("-id").first()

    forecast_lines_qs = ForecastLine.objects.none()
    if forecast_latest:
        forecast_lines_qs = forecast_latest.lines.all().order_by("part_code", "mat_partcode")
        if fq:
            forecast_lines_qs = forecast_lines_qs.filter(
                Q(part_code__icontains=fq) |
                Q(mat_partcode__icontains=fq) |
                Q(mat_partname__icontains=fq) |
                Q(tep_code__icontains=fq) |
                Q(customer_name__icontains=fq)
            )

    forecast_paginator = Paginator(forecast_lines_qs, 10)
    fpage = request.GET.get("fpage")
    forecast_page = forecast_paginator.get_page(fpage)

    forecast_totals = {"lines": forecast_lines_qs.count()}

    context = {
        "tab": tab,

        "customers_count": Customer.objects.count(),
        "tep_count": TEPCode.objects.count(),
        "materials_count": Material.objects.count(),
        "users_count": User.objects.count(),

        "customers": customers,
        "q": q,

        "mq": mq,
        "material_total": material_total,
        "material_list": material_list,
        "page_obj": page_obj,

        "sq": sq,
        "materials_master": materials_master,
        "stock_page_obj": stock_page_obj,

        "uq": uq,
        "user_total": user_total,
        "users_page": users_page,

        "forecast_latest": forecast_latest,
        "forecast_page": forecast_page,
        "forecast_totals": forecast_totals,
        "fq": fq,

        "master_map_json": json.dumps(master_map, ensure_ascii=False),
    }
    return render(request, "admin/dashboard.html", context)


@login_required
@user_passes_test(is_admin)
def admin_users(request):
    return redirect(reverse("app:admin_dashboard") + "?tab=users")


@login_required
@user_passes_test(is_admin)
def toggle_user_active(request, user_id):
    user_obj = get_object_or_404(User, id=user_id)

    if user_obj == request.user:
        messages.error(request, "You can't disable your own account.")
        return redirect(reverse("app:admin_dashboard") + "?tab=users")

    user_obj.is_active = not user_obj.is_active
    user_obj.save(update_fields=["is_active"])

    messages.success(request, f"Updated user: {user_obj.username} (active={user_obj.is_active})")
    return redirect(reverse("app:admin_dashboard") + "?tab=users")

@require_GET
@login_required
def material_lookup(request):
    partcode = (request.GET.get("mat_partcode") or "").strip()
    if not partcode:
        return JsonResponse({"ok": False, "error": "Missing mat_partcode."}, status=400)

    mat = (
        MaterialList.objects
        .filter(mat_partcode__iexact=partcode)
        .values("mat_partcode", "mat_partname", "mat_maker", "unit")
        .first()
    )

    if not mat:
        return JsonResponse({"ok": False, "error": "Not found."}, status=404)

    return JsonResponse({"ok": True, "material": mat})


@login_required
@user_passes_test(is_admin)
def create_employee(request):
    if request.method == "POST":
        form = EmployeeCreateForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, "Employee account created successfully.")
            return redirect(reverse("app:admin_dashboard") + "?tab=users")
    else:
        form = EmployeeCreateForm()

    return render(request, "create_employee.html", {"form": form})


@login_required
@user_passes_test(is_admin)
def admin_csv_upload(request):
    default_next = reverse("app:admin_dashboard") + "?tab=materials"
    next_url = request.POST.get("next") or request.GET.get("next") or default_next

    if not url_has_allowed_host_and_scheme(next_url, allowed_hosts={request.get_host()}):
        next_url = default_next

    if request.method == "POST" and request.FILES.get("csv_file"):
        f = request.FILES["csv_file"]
        raw = f.read()

        content = None
        for enc in ("utf-8-sig", "utf-16", "cp1252", "latin-1"):
            try:
                content = raw.decode(enc)
                break
            except UnicodeDecodeError:
                continue

        if content is None:
            messages.error(request, "Could not read file encoding. Save as CSV UTF-8 and upload again.")
            return redirect(next_url)

        csv_file = io.StringIO(content)
        reader = csv.DictReader(csv_file)
        reader.fieldnames = [h.strip().lstrip("\ufeff") for h in (reader.fieldnames or [])]

        master_inserted = 0
        master_updated = 0
        ALLOWED_UNITS = {"pc", "pcs", "m", "g", "kg"}

        def sget(row, *keys, default=""):
            for k in keys:
                v = row.get(k)
                if v is not None and str(v).strip() != "":
                    return str(v).strip()
            return default

        try:
            with transaction.atomic():
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

            messages.success(
                request,
                f"CSV uploaded successfully | master_inserted={master_inserted}, master_updated={master_updated}"
            )
            return redirect(next_url)

        except Exception as e:
            messages.error(request, f"Upload failed: {e}")
            return redirect(next_url)

    return redirect(next_url)

@never_cache
@login_required
def customer_list(request):
    q = (request.GET.get("q") or "").strip()
    customers = build_customer_table(q)

    staff_full_name = None
    try:
        staff_full_name = request.user.employeeprofile.full_name
    except Exception:
        pass

    return render(request, "customer_list.html", {
        "customers": customers,
        "q": q,
        "staff_full_name": staff_full_name,
    })

@never_cache
@login_required
def customer_detail(request, tep_id: int):
    tep = get_object_or_404(TEPCode.objects.select_related("customer", "superseded_by"), id=tep_id)

    materials = Material.objects.filter(tep_code=tep).order_by("mat_partname")
    mq = (request.GET.get("mq") or "").strip()
    master_qs = MaterialList.objects.all().order_by("mat_partcode")
    if mq:
        master_qs = master_qs.filter(
            Q(mat_partcode__icontains=mq) |
            Q(mat_partname__icontains=mq) |
            Q(mat_maker__icontains=mq) |
            Q(unit__icontains=mq)
        )

    master_paginator = Paginator(master_qs, 10)
    master_page = master_paginator.get_page(request.GET.get("mpage"))

    selected_part = (tep.part_code or "").strip()
    selected_part_name = ""
    for p in (tep.customer.parts or []):
        if isinstance(p, dict) and str(p.get("Partcode", "")).strip() == selected_part:
            selected_part_name = str(p.get("Partname", "")).strip()
            break

    return render(request, "customer_detail.html", {
        "customer": tep.customer,
        "materials": materials,
        "selected_tep": tep.tep_code,
        "selected_part": selected_part,
        "selected_part_name": selected_part_name,
        "tep_id": tep.id,
        "tep_obj": tep,
        "mq": mq,
        "master_materials": master_page,
    })


@login_required
@user_passes_test(is_admin)
def add_material_to_tep(request):

    tep_id = (request.POST.get("tep_id") or "").strip()
    mat_partcode = _normalize_space(request.POST.get("mat_partcode"))
    dim_qty_raw = (request.POST.get("dim_qty") or "").strip()
    loss_raw = (request.POST.get("loss_percent") or "").strip()

    if not tep_id:
        messages.error(request, "Missing TEP id.")
        return redirect("app:admin_dashboard")

    if not mat_partcode:
        messages.error(request, "Material Part Code is required.")
        return redirect("app:admin_dashboard")

    if not dim_qty_raw:
        messages.error(request, "Dim/Qty is required.")
        return redirect("app:admin_dashboard")

    try:
        dim_qty = float(dim_qty_raw)
    except Exception:
        messages.error(request, "Dim/Qty must be a number.")
        return redirect("app:admin_dashboard")

    loss_percent = 10.0
    if loss_raw != "":
        try:
            loss_percent = float(loss_raw)
        except Exception:
            messages.error(request, "Loss % must be a number.")
            return redirect("app:admin_dashboard")

    tep = get_object_or_404(TEPCode, id=tep_id)

    if not getattr(tep, "is_active", True):
        messages.error(request, f"{tep.tep_code} is obsolete and cannot be edited. Please revise or select the active TEP.")
        return redirect(reverse("app:admin_dashboard") + f"?tab=customers&open_panel=1&tep_id={tep_id}")

    master = MaterialList.objects.filter(mat_partcode=mat_partcode).first()
    if not master:
        messages.error(request, f"mat_partcode not found in master list: {mat_partcode}")
        return redirect(reverse("app:admin_dashboard") + f"?tab=customers&open_panel=1&tep_id={tep_id}")

    total = round(float(dim_qty) * (1 + (float(loss_percent) / 100.0)), 4)

    try:
        with transaction.atomic():
            final_name = _allocate_material_name(
                tep=tep,
                base_name=master.mat_partname,
                exclude_partcode=mat_partcode
            )

            material, created = Material.objects.get_or_create(
                tep_code=tep,
                mat_partcode=mat_partcode,
                defaults={
                    "mat_partname": final_name,
                    "mat_maker": master.mat_maker,
                    "unit": master.unit,
                    "dim_qty": dim_qty,
                    "loss_percent": loss_percent,
                    "total": total,
                }
            )

            if not created:
                messages.error(request, f"Material already exists for this TEP + {mat_partcode}.")
            else:
                messages.success(request, f"Added material: {mat_partcode}")

    except Exception as e:
        messages.error(request, f"Failed to add material: {e}")

    return redirect(reverse("app:admin_dashboard") + f"?tab=customers&open_panel=1&tep_id={tep_id}")

@require_POST
@login_required
@user_passes_test(can_edit)
def add_material_to_tep_staff(request):
    tep_id = (request.POST.get("tep_id") or "").strip()
    if not tep_id:
        messages.error(request, "Missing TEP id.")
        return redirect("app:customer_list")

    tep = get_object_or_404(TEPCode, id=tep_id)

    if not getattr(tep, "is_active", True):
        messages.error(request, f"{tep.tep_code} is obsolete and cannot be edited.")
        return redirect("app:customer_detail", tep_id=tep.id)

    mat_partcode = _normalize_space(request.POST.get("mat_partcode"))
    dim_qty_raw = (request.POST.get("dim_qty") or "").strip()
    loss_raw = (request.POST.get("loss_percent") or "").strip()

    if not mat_partcode:
        messages.error(request, "Material Part Code is required.")
        return redirect("app:customer_detail", tep_id=tep.id)

    if not dim_qty_raw:
        messages.error(request, "Dim/Qty is required.")
        return redirect("app:customer_detail", tep_id=tep.id)

    try:
        dim_qty = float(dim_qty_raw)
    except Exception:
        messages.error(request, "Dim/Qty must be a number.")
        return redirect("app:customer_detail", tep_id=tep.id)

    loss_percent = 10.0
    if loss_raw != "":
        try:
            loss_percent = float(loss_raw)
        except Exception:
            messages.error(request, "Loss % must be a number.")
            return redirect("app:customer_detail", tep_id=tep.id)

    master = MaterialList.objects.filter(mat_partcode__iexact=mat_partcode).first()
    if not master:
        messages.error(request, f"Material code not found in master list: {mat_partcode}")
        return redirect("app:customer_detail", tep_id=tep.id)

    total = round(float(dim_qty) * (1 + (float(loss_percent) / 100.0)), 4)

    try:
        with transaction.atomic():
            final_name = _allocate_material_name(
                tep=tep,
                base_name=master.mat_partname,
                exclude_partcode=mat_partcode
            )

            material, created = Material.objects.get_or_create(
                tep_code=tep,
                mat_partcode=mat_partcode,
                defaults={
                    "mat_partname": final_name,
                    "mat_maker": master.mat_maker,
                    "unit": master.unit,
                    "dim_qty": dim_qty,
                    "loss_percent": loss_percent,
                    "total": total,
                }
            )

            if not created:
                messages.error(request, f"Material already exists for this TEP + {mat_partcode}.")
            else:
                messages.success(request, f"Added material: {mat_partcode}")

    except Exception as e:
        messages.error(request, f"Failed to add material: {e}")

    return redirect("app:customer_detail", tep_id=tep.id)

@never_cache
@login_required
@user_passes_test(can_edit)  
def staff_materials(request):
    mq = (request.GET.get("mq") or "").strip()

    qs = MaterialList.objects.all().order_by("mat_partcode")
    if mq:
        qs = qs.filter(
            Q(mat_partcode__icontains=mq) |
            Q(mat_partname__icontains=mq) |
            Q(mat_maker__icontains=mq) |
            Q(unit__icontains=mq)
        )

    paginator = Paginator(qs, 12)
    page = paginator.get_page(request.GET.get("page"))

    return render(request, "materials_list.html", {
        "mq": mq,
        "page_obj": page,
        "materials": page,  
    })

from django.views.decorators.http import require_POST
from django.views.decorators.cache import never_cache
from django.db.models import Q
from django.core.paginator import Paginator

@never_cache
@login_required
@user_passes_test(can_edit)
def staff_materials(request):
    mq = (request.GET.get("mq") or "").strip()

    qs = MaterialList.objects.all().order_by("mat_partcode")
    if mq:
        qs = qs.filter(
            Q(mat_partcode__icontains=mq) |
            Q(mat_partname__icontains=mq) |
            Q(mat_maker__icontains=mq) |
            Q(unit__icontains=mq)
        )

    paginator = Paginator(qs, 12)
    page_obj = paginator.get_page(request.GET.get("page"))

    return render(request, "materials_list.html", {
        "mq": mq,
        "materials": page_obj,
        "page_obj": page_obj,
    })


@require_POST
@login_required
@user_passes_test(can_edit)
def staff_material_add(request):
    mat_partcode = (request.POST.get("mat_partcode") or "").strip()
    mat_partname = (request.POST.get("mat_partname") or "").strip()
    mat_maker = (request.POST.get("mat_maker") or "").strip()
    unit = (request.POST.get("unit") or "").strip().lower()

    allowed_units = {"pc", "pcs", "m", "g", "kg"}
    if unit not in allowed_units:
        unit = "pc"

    if not mat_partcode:
        messages.error(request, "Material Code is required.")
        return redirect("app:staff_materials")

    if MaterialList.objects.filter(mat_partcode__iexact=mat_partcode).exists():
        messages.error(request, f"Material Code already exists: {mat_partcode}")
        return redirect("app:staff_materials")

    MaterialList.objects.create(
        mat_partcode=mat_partcode,
        mat_partname=mat_partname or mat_partcode,
        mat_maker=mat_maker or "Unknown",
        unit=unit
    )
    messages.success(request, f"Added material: {mat_partcode}")
    return redirect("app:staff_materials")


@require_POST
@login_required
@user_passes_test(can_edit)
def staff_material_update(request):
    mat_id = (request.POST.get("mat_id") or "").strip()
    mat_partcode = (request.POST.get("mat_partcode") or "").strip()
    mat_partname = (request.POST.get("mat_partname") or "").strip()
    mat_maker = (request.POST.get("mat_maker") or "").strip()
    unit = (request.POST.get("unit") or "").strip().lower()

    allowed_units = {"pc", "pcs", "m", "g", "kg"}
    if unit not in allowed_units:
        unit = "pc"

    if not mat_id:
        messages.error(request, "Missing material id.")
        return redirect("app:staff_materials")

    obj = get_object_or_404(MaterialList, id=mat_id)

    if not mat_partcode:
        messages.error(request, "Material Code is required.")
        return redirect("app:staff_materials")

    if MaterialList.objects.filter(mat_partcode__iexact=mat_partcode).exclude(id=obj.id).exists():
        messages.error(request, f"Material Code already exists: {mat_partcode}")
        return redirect("app:staff_materials")

    obj.mat_partcode = mat_partcode
    obj.mat_partname = mat_partname or mat_partcode
    obj.mat_maker = mat_maker or "Unknown"
    obj.unit = unit
    obj.save()

    messages.success(request, f"Updated: {obj.mat_partcode}")
    return redirect("app:staff_materials")


@require_POST
@login_required
@user_passes_test(can_edit)
def staff_material_delete(request):
    mat_id = (request.POST.get("mat_id") or "").strip()
    if not mat_id:
        messages.error(request, "Missing material id.")
        return redirect("app:staff_materials")

    obj = get_object_or_404(MaterialList, id=mat_id)
    code = obj.mat_partcode
    obj.delete()

    messages.success(request, f"Deleted: {code}")
    return redirect("app:staff_materials")


@require_POST
@login_required
@user_passes_test(can_edit)
def staff_materials_csv_upload(request):
    if not request.FILES.get("csv_file"):
        messages.error(request, "Please choose a CSV file.")
        return redirect("app:staff_materials")

    f = request.FILES["csv_file"]
    raw = f.read()

    content = None
    for enc in ("utf-8-sig", "utf-16", "cp1252", "latin-1"):
        try:
            content = raw.decode(enc)
            break
        except UnicodeDecodeError:
            continue

    if content is None:
        messages.error(request, "Could not read file encoding. Save as CSV UTF-8 and upload again.")
        return redirect("app:staff_materials")

    csv_file = io.StringIO(content)
    reader = csv.DictReader(csv_file)
    reader.fieldnames = [h.strip().lstrip("\ufeff") for h in (reader.fieldnames or [])]

    inserted = 0
    updated = 0
    ALLOWED_UNITS = {"pc", "pcs", "m", "g", "kg"}

    def sget(row, *keys, default=""):
        for k in keys:
            v = row.get(k)
            if v is not None and str(v).strip() != "":
                return str(v).strip()
        return default

    try:
        with transaction.atomic():
            for row in reader:
                mat_partcode = sget(row, "mat_partcode", "material_part_code")
                mat_partname = sget(row, "mat_partname", "material_name")
                mat_maker = sget(row, "mat_maker", "maker")
                unit = sget(row, "unit", default="pc").lower()

                if unit not in ALLOWED_UNITS:
                    unit = "pc"
                if not mat_partcode:
                    continue

                obj, created = MaterialList.objects.get_or_create(
                    mat_partcode=mat_partcode,
                    defaults={
                        "mat_partname": mat_partname or mat_partcode,
                        "mat_maker": mat_maker or "Unknown",
                        "unit": unit,
                    }
                )

                if created:
                    inserted += 1
                else:
                    changed = False
                    if mat_partname and obj.mat_partname != mat_partname:
                        obj.mat_partname = mat_partname
                        changed = True
                    if mat_maker and obj.mat_maker != mat_maker:
                        obj.mat_maker = mat_maker
                        changed = True
                    if unit and obj.unit != unit:
                        obj.unit = unit
                        changed = True
                    if changed:
                        obj.save()
                        updated += 1

        messages.success(request, f"CSV uploaded: inserted={inserted}, updated={updated}")
    except Exception as e:
        messages.error(request, f"Upload failed: {e}")

    return redirect("app:staff_materials")


@require_POST
def customer_create(request):
    customer_name = (request.POST.get("customer_name") or "").strip()
    part_code = (request.POST.get("part_code") or "").strip()
    part_name = (request.POST.get("part_name") or "").strip()
    tep_code = (request.POST.get("tep_code") or "").strip()
    parts_json_raw = (request.POST.get("parts_json") or "[]").strip()

    if not customer_name or not part_code or not part_name or not tep_code:
        messages.error(request, "Please fill up all fields.")
        return redirect("app:customer_list")

    try:
        parts = json.loads(parts_json_raw) if parts_json_raw else []
        if not isinstance(parts, list):
            parts = []
    except Exception:
        parts = []

    try:
        with transaction.atomic():
            customer = Customer(customer_name=customer_name, parts=parts)
            customer.full_clean()
            customer.save()

            tep = TEPCode(customer=customer, part_code=part_code, tep_code=tep_code, is_active=True)
            tep.full_clean()
            tep.save()

        messages.success(request, "Customer created successfully.")
        return redirect("app:customer_list")

    except IntegrityError as e:
        msg = str(e)

        if "tepcode.tep_code" in msg.lower() or "app_tepcode.tep_code" in msg.lower():
            messages.error(request, "TEP Code already exists.")
        elif "customer.customer_name" in msg.lower() or "app_customer.customer_name" in msg.lower():
            messages.error(request, "Customer name already exists.")
        else:
            messages.error(request, "Failed to save customer record.")
        return redirect("app:customer_list")

    except Exception:
        messages.error(request, "Failed to save customer record.")
        return redirect("app:customer_list")

@require_POST
@login_required
@user_passes_test(is_admin)
def create_material_allocation(request):
    material_id = (request.POST.get("material_id") or "").strip()
    customer_id = (request.POST.get("customer_id") or "").strip()
    tep_id = (request.POST.get("tep_id") or "").strip()
    qty_raw = (request.POST.get("qty_allocated") or "").strip()
    forecast_ref = (request.POST.get("forecast_ref") or "").strip()

    sq = (request.POST.get("sq") or "").strip()
    spage = (request.POST.get("spage") or "").strip()

    if not material_id or not customer_id or not qty_raw:
        messages.error(request, "Material, Customer, and Qty are required.")
        return redirect(reverse("app:admin_dashboard") + "?tab=stocks")

    try:
        qty = int(qty_raw)
        if qty <= 0:
            raise ValueError("qty must be > 0")
    except Exception:
        messages.error(request, "Qty must be a whole number greater than 0.")
        return redirect(reverse("app:admin_dashboard") + "?tab=stocks")

    mat = get_object_or_404(MaterialList, id=material_id)
    cust = get_object_or_404(Customer, id=customer_id)

    tep = None
    if tep_id:
        try:
            tep = TEPCode.objects.get(id=tep_id)
        except TEPCode.DoesNotExist:
            tep = None

    try:
        on_hand = int(mat.stock.on_hand_qty or 0)
    except Exception:
        on_hand = 0

    reserved = (
        MaterialAllocation.objects.filter(material=mat, status="reserved")
        .aggregate(total=Sum("qty_allocated"))
        .get("total") or 0
    )
    reserved = int(reserved or 0)

    available = max(on_hand - reserved, 0)

    if qty > available:
        messages.error(request, f"Not enough available stock. Available: {available}")
    else:
        MaterialAllocation.objects.create(
            material=mat,
            customer=cust,
            tep_code=tep,
            qty_allocated=qty,
            forecast_ref=forecast_ref,
            status="reserved",
            created_by=request.user
        )
        messages.success(request, f"Allocated {qty} of {mat.mat_partcode} to {cust.customer_name}.")

    url = reverse("app:admin_dashboard") + "?tab=stocks"
    if sq:
        url += f"&sq={sq}"
    if spage:
        url += f"&spage={spage}"
    return redirect(url)


def logout_view(request):
    logout(request)
    response = redirect(reverse("app:login"))

    response["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response["Pragma"] = "no-cache"
    response["Expires"] = "0"
    return response