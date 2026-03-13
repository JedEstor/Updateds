from django.views.decorators.cache import never_cache
import json, csv, io, re
from collections import defaultdict
from urllib.parse import urlencode
from django.views.decorators.http import require_POST, require_GET
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
from datetime import datetime, date
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .models import (
    Customer,
    TEPCode,
    BOMMaterial,
    Material,
    MaterialList,
    MaterialStock,
    MaterialAllocation,
    ForecastRun,
    ForecastLine,
    Forecast,
    PartMaster,
)
from .forms import EmployeeCreateForm
from .service import (
    ForecastInput,
    run_forecast_and_save,
    reserve_from_latest_forecast_run,
    get_registered_materials_for_partcode,
    get_shared_bom_rows_for_partcode,
    replace_bom_for_partcode,
    import_bom_csv_file,
)


# ── Timezone helpers ──────────────────────────────────────────────────────────

def _get_user_tz(request) -> ZoneInfo:
    """
    Return ZoneInfo for the user's timezone.
    Priority: session (browser-detected) → Django settings.TIME_ZONE → UTC.
    """
    from django.conf import settings
    default_tz = getattr(settings, "TIME_ZONE", "UTC")
    tz_name = request.session.get("user_timezone", default_tz)
    try:
        return ZoneInfo(tz_name)
    except (ZoneInfoNotFoundError, KeyError):
        try:
            return ZoneInfo(default_tz)
        except (ZoneInfoNotFoundError, KeyError):
            return ZoneInfo("UTC")


def _now_in_tz(request) -> datetime:
    """Return current datetime in the user's session timezone."""
    return datetime.now(tz=_get_user_tz(request))


def _today_in_tz(request) -> date:
    """Return today's date in the user's session timezone."""
    return _now_in_tz(request).date()


# ── Helper: normalize a month abbreviation from a date string ─────────────────
_MONTH_NAME_TO_NUM = {
    'january': 1, 'february': 2, 'march': 3, 'april': 4,
    'may': 5, 'june': 6, 'july': 7, 'august': 8,
    'september': 9, 'october': 10, 'november': 11, 'december': 12,
    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4,
    'jun': 6, 'jul': 7, 'aug': 8, 'sep': 9, 'sept': 9,
    'oct': 10, 'nov': 11, 'dec': 12,
}
_SHORT_MONTHS = {
    1: 'JAN', 2: 'FEB', 3: 'MAR', 4: 'APR', 5: 'MAY', 6: 'JUN',
    7: 'JUL', 8: 'AUG', 9: 'SEPT', 10: 'OCT', 11: 'NOV', 12: 'DEC',
}


def _month_abbr_from_date_str(date_str: str) -> str | None:
    """
    Given a date string like 'January-2026' or 'Jan-2026',
    return the abbreviated month label used in the summary tables
    (e.g. 'JAN'), or None if unparseable.
    """
    date_str = (date_str or "").strip()
    if not date_str:
        return None
    parts = date_str.split("-")
    if len(parts) < 2:
        return None
    month_name = parts[0].strip().lower()
    month_num = _MONTH_NAME_TO_NUM.get(month_name)
    if not month_num:
        return None
    return _SHORT_MONTHS[month_num]


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


def _admin_dashboard_url(sidebar_tab: str = "customers", materials_bom_tab: str = "", **params):
    """
    Build admin dashboard URLs while supporting the grouped Materials & BOM panel.

    Backward compatible behavior:
    - sidebar_tab in {bom_master, materials, stocks} maps to:
      ?tab=materials_bom&mbtab=<that tab>
    - sidebar_tab == materials_bom uses mbtab (default bom_master)
    - all other tabs remain unchanged
    """
    sidebar_tab = (sidebar_tab or "customers").strip().lower()
    materials_bom_tab = (materials_bom_tab or "").strip().lower()

    query = {}

    if sidebar_tab in {"bom_master", "materials", "stocks"}:
        query["tab"] = "materials_bom"
        query["mbtab"] = sidebar_tab
    else:
        query["tab"] = sidebar_tab
        if sidebar_tab == "materials_bom":
            query["mbtab"] = materials_bom_tab or "bom_master"

    for key, value in params.items():
        if value is None:
            continue
        if isinstance(value, str):
            value = value.strip()
            if value == "":
                continue
        query[key] = value

    return reverse("app:admin_dashboard") + "?" + urlencode(query)


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


def _parse_schedule_month_key(value: str) -> str:
    """Normalize incoming month values to YYYY-MM when possible."""
    raw = (value or "").strip()
    if not raw:
        return ""

    # already YYYY-MM or longer ISO-like format
    if re.match(r"^\d{4}-\d{2}", raw):
        return raw[:7]

    month_map = {
        "jan": 1, "january": 1,
        "feb": 2, "february": 2,
        "mar": 3, "march": 3,
        "apr": 4, "april": 4,
        "may": 5,
        "jun": 6, "june": 6,
        "jul": 7, "july": 7,
        "aug": 8, "august": 8,
        "sep": 9, "sept": 9, "september": 9,
        "oct": 10, "october": 10,
        "nov": 11, "november": 11,
        "dec": 12, "december": 12,
    }

    parts = [s for s in re.split(r"[-/\s]+", raw.lower()) if s]
    if len(parts) >= 2:
        month_num = month_map.get(parts[0])
        year = parts[-1]
        if month_num and re.fullmatch(r"\d{4}", year):
            return f"{year}-{month_num:02d}"

    return ""


def _build_bom_display_rows(tep):
    """
    Prefer shared BOMMaterial rows by part_code.
    Fallback to legacy Material rows under the current TEP.
    """
    rows = []
    part_code = (getattr(tep, "part_code", "") or "").strip() if tep else ""

    try:
        bom_rows = (
            BOMMaterial.objects
            .filter(part_code=part_code)
            .select_related("material")
            .order_by("mat_partcode", "id")
        )
    except Exception:
        bom_rows = BOMMaterial.objects.none()

    if bom_rows.exists():
        for row in bom_rows:
            master = getattr(row, "material", None)
            if master:
                row.mat_partcode = master.mat_partcode or row.mat_partcode
                row.mat_partname = master.mat_partname or row.mat_partname
                row.mat_maker = master.mat_maker or row.mat_maker
                row.unit = master.unit or row.unit
            rows.append(row)
        return rows

    return list(Material.objects.filter(tep_code=tep).order_by("mat_partname", "id"))


def _format_schedule_month_label(schedule_month: str) -> str:
    schedule_month = (schedule_month or "").strip()
    if not schedule_month:
        return "No month"

    try:
        return datetime.strptime(schedule_month, "%Y-%m").strftime("%B %Y")
    except Exception:
        return schedule_month


def _build_forecast_grouped(forecast_page):
    """
    Group the currently displayed ForecastLine page by customer and part code.

    Keep runs separated so older runs for the same customer / part / month
    do not get merged into the latest run.
    """
    if not forecast_page:
        return []

    grouped = {}

    for line in getattr(forecast_page, "object_list", []):
        customer_name = (getattr(line, "customer_name", "") or "—").strip() or "—"
        part_code = (getattr(line, "part_code", "") or "—").strip() or "—"
        part_name = (getattr(line, "part_name", "") or "").strip()
        if not part_name and part_code and part_code != "—":
            part_name = _get_part_name_for_code(part_code)

        run_obj = getattr(line, "run", None)
        line_schedule_month = (getattr(line, "schedule_month", "") or "").strip()
        run_schedule_month = (getattr(run_obj, "schedule_month", "") or "").strip()
        schedule_month = line_schedule_month or run_schedule_month
        run_id = getattr(run_obj, "id", "") or ""

        part_key = f"{run_id}||{part_code}||{schedule_month}"

        customer_entry = grouped.setdefault(customer_name, {
            "customer_name": customer_name,
            "_parts": {},
        })

        run_label = "—"
        try:
            created_at = getattr(run_obj, "created_at", None)
            if created_at:
                run_label = timezone.localtime(created_at).strftime("%Y-%m-%d %H:%M")
        except Exception:
            pass

        forecast_qty = getattr(line, "forecast_qty", 0)
        try:
            forecast_qty = int(float(forecast_qty or 0))
        except Exception:
            forecast_qty = 0

        part_entry = customer_entry["_parts"].setdefault(part_key, {
            "part_code": part_code,
            "part_name": part_name,
            "forecast_qty": forecast_qty,
            "month_label": _format_schedule_month_label(schedule_month),
            "run_label": run_label,
            "materials": [],
        })

        if not part_entry.get("part_name") and part_name:
            part_entry["part_name"] = part_name
        if not part_entry.get("forecast_qty") and forecast_qty:
            part_entry["forecast_qty"] = forecast_qty

        required_qty = getattr(line, "required_qty", 0)
        try:
            required_qty = float(required_qty or 0)
        except Exception:
            required_qty = 0.0

        part_entry["materials"].append({
            "tep_code": getattr(line, "tep_code", "") or "",
            "mat_partcode": getattr(line, "mat_partcode", "") or "",
            "mat_partname": getattr(line, "mat_partname", "") or "",
            "mat_maker": getattr(line, "mat_maker", "") or "",
            "unit": getattr(line, "unit", "") or "",
            "required_qty": required_qty,
        })

    output = []
    for customer_entry in grouped.values():
        parts = list(customer_entry["_parts"].values())
        parts.sort(key=lambda p: (p.get("run_label", ""), p.get("part_code", "")), reverse=True)
        output.append({
            "customer_name": customer_entry["customer_name"],
            "parts": parts,
        })

    output.sort(key=lambda c: c.get("customer_name", ""))
    return output


def _preferred_tep_for_part_code(part_code: str):
    """
    Return the best source TEP for a shared BOM part code.
    Prefer an active TEP first, then the newest record.
    """
    part_code = (part_code or "").strip()
    if not part_code:
        return None

    return (
        TEPCode.objects
        .filter(part_code=part_code)
        .order_by("-is_active", "-id")
        .first()
    )


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
                    "materials_count": (BOMMaterial.objects.filter(part_code=t.part_code).count() or t.materials.count()),
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


def _get_part_name_for_code(part_code: str) -> str:
    """
    Resolve the part name from PartMaster first.
    Fallback to Customer.parts JSON for older data.
    """
    part_code = (part_code or "").strip()
    if not part_code:
        return ""

    pm = PartMaster.objects.filter(part_code=part_code, is_active=True).first()
    if pm:
        return (pm.part_name or "").strip()

    for cust in Customer.objects.all():
        for item in (cust.parts or []):
            if not isinstance(item, dict):
                continue
            if (item.get("Partcode") or "").strip() == part_code:
                return (item.get("Partname") or "").strip()

    return ""


def get_shared_part_master_map():
    """
    Build a mapping of part_code -> part_name.

    Primary source:
      - PartMaster
    Fallback source:
      - legacy Customer.parts JSON
      - legacy TEPCode.part_code rows
    """
    part_map = {}

    # Primary: PartMaster
    for row in PartMaster.objects.all().order_by("part_code"):
        code = (row.part_code or "").strip()
        if code:
            part_map[code] = (row.part_name or "").strip()

    # Fallback: Customer.parts JSON
    for cust in Customer.objects.all():
        for item in (cust.parts or []):
            if not isinstance(item, dict):
                continue

            code = (item.get("Partcode") or "").strip()
            name = (item.get("Partname") or "").strip()
            if code and code not in part_map:
                part_map[code] = name or code

    # Fallback: existing TEP rows
    if hasattr(TEPCode, "objects"):
        for row in TEPCode.objects.exclude(part_code__isnull=True).exclude(part_code__exact=""):
            code = (row.part_code or "").strip()
            if code and code not in part_map:
                part_map[code] = code

    return part_map


def _build_bom_master_context(part_code: str = "", bq: str = ""):
    part_map = get_shared_part_master_map()
    bom_part_codes = sorted(part_map.keys())

    if bq:
        q = bq.lower()
        bom_part_codes = [
            code for code in bom_part_codes
            if q in code.lower() or q in (part_map.get(code, "") or "").lower()
        ]

    selected_bom_part_code = (part_code or "").strip()
    if not selected_bom_part_code and bom_part_codes:
        selected_bom_part_code = bom_part_codes[0]

    selected_bom_part_name = part_map.get(selected_bom_part_code, "") if selected_bom_part_code else ""
    selected_bom_tep = _preferred_tep_for_part_code(selected_bom_part_code) if selected_bom_part_code else None

    bom_rows = []
    if selected_bom_part_code:
        try:
            bom_rows = get_shared_bom_rows_for_partcode(selected_bom_part_code)
        except Exception:
            bom_rows = []

    master_materials = MaterialList.objects.all().order_by("mat_partcode")

    return {
        "bq": bq,
        "bom_part_codes": bom_part_codes,
        "selected_bom_part_code": selected_bom_part_code,
        "selected_bom_part_name": selected_bom_part_name,
        "selected_bom_tep": selected_bom_tep,
        "bom_rows": bom_rows,
        "bom_master_materials": master_materials,
    }


def _sync_legacy_material_from_bom(tep, master, dim_qty, loss_percent):
    """
    Keep legacy Material rows in sync for screens still reading from Material.
    Safe no-op if duplicates already exist.
    """
    if not tep or not master:
        return None

    total = round(float(dim_qty) * (1 + (float(loss_percent) / 100.0)), 4)

    try:
        obj, _ = Material.objects.update_or_create(
            tep_code=tep,
            mat_partcode=master.mat_partcode,
            defaults={
                "mat_partname": master.mat_partname,
                "mat_maker": master.mat_maker,
                "unit": master.unit,
                "dim_qty": dim_qty,
                "loss_percent": loss_percent,
                "total": total,
            },
        )
        return obj
    except Exception:
        return None


def _generate_unique_next_tep_code(old_code: str) -> str:
    old_code = (old_code or "").strip()
    base = old_code

    m = re.match(r"^(.*?)(?:-R(\d+))?$", old_code, flags=re.I)
    if m:
        base = (m.group(1) or old_code).strip()

    i = 1
    while True:
        candidate = f"{base}-R{i}"
        if not TEPCode.objects.filter(tep_code=candidate).exists():
            return candidate
        i += 1


def _build_forecast_summary(fsq: str = "", fsq_customer: str = "", fsq_month: str = "", user_tz=None):
    """
    Build data for the Forecast Summary tab (current year only).

    fsq_month: abbreviated month label to highlight/filter, e.g. 'JAN'.
               When provided the rows are filtered so only rows that have
               a non-zero quantity for that month are shown.
    """
    from datetime import date
    import calendar

    if user_tz is None:
        from zoneinfo import ZoneInfo
        user_tz = ZoneInfo("UTC")
    today = datetime.now(tz=user_tz).date()
    current_year = today.year
    prev_year = current_year - 1

    qs = (
        Forecast.objects
        .select_related("customer")
        .order_by("customer__customer_name", "part_number")
    )

    if fsq:
        qs = qs.filter(
            Q(part_number__icontains=fsq) | Q(part_name__icontains=fsq)
        )
    if fsq_customer:
        qs = qs.filter(customer__customer_name=fsq_customer)

    prev_month_keys = set()
    fore_month_keys = set()

    def _parse_date_str(date_str):
        date_str = (date_str or "").strip()
        if not date_str:
            return None
        parts = date_str.split("-")
        if len(parts) < 2:
            return None
        month_name = parts[0].strip().lower()
        try:
            year = int(parts[-1].strip())
        except ValueError:
            return None
        month_int = _MONTH_NAME_TO_NUM.get(month_name)
        if not month_int:
            return None
        label = _SHORT_MONTHS[month_int]
        return year, month_int, label

    rows_by_key = {}

    for forecast in qs:
        monthly = forecast.monthly_forecasts or []

        first_entry = next(
            (m for m in monthly if isinstance(m, dict)),
            {}
        )

        try:
            unit_price = float(first_entry.get("unit_price", 0)) if isinstance(first_entry, dict) else 0.0
        except (TypeError, ValueError):
            unit_price = 0.0

        customer_name = forecast.customer.customer_name if forecast.customer else "—"
        key = (customer_name, forecast.part_number)

        row = rows_by_key.get(key)
        if not row:
            row = {
                "customer": customer_name,
                "part_number": forecast.part_number,
                "part_name": forecast.part_name,
                "unit_price": unit_price,
                "prev": {},
                "fore": {},
            }
            rows_by_key[key] = row
        else:
            if unit_price:
                row["unit_price"] = unit_price

        prev_data = row["prev"]
        fore_data = row["fore"]

        for entry in monthly:
            if not isinstance(entry, dict):
                continue
            parsed = _parse_date_str(entry.get("date", ""))
            if not parsed:
                continue
            yr, mo, label = parsed
            try:
                qty = float(entry.get("quantity", 0) or 0)
            except (TypeError, ValueError):
                qty = 0.0

            if yr < current_year:
                prev_data[label] = prev_data.get(label, 0.0) + qty
                prev_month_keys.add((yr, mo, label))
            else:
                fore_data[label] = fore_data.get(label, 0.0) + qty
                fore_month_keys.add((yr, mo, label))

    fs_rows = list(rows_by_key.values())

    # ── Filter rows by selected month ────────────────────────────────────────
    if fsq_month:
        fs_rows = [r for r in fs_rows if r["fore"].get(fsq_month, 0)]

    all_month_labels = [_SHORT_MONTHS[i] for i in range(1, 13)]
    fs_prev_months = all_month_labels
    fs_fore_months = all_month_labels

    fs_total_prev_qty = defaultdict(float)
    fs_total_fore_qty = defaultdict(float)
    fs_total_prev_amt = defaultdict(float)
    fs_total_fore_amt = defaultdict(float)

    for row in fs_rows:
        up = row["unit_price"]
        for lbl, qty in row["prev"].items():
            fs_total_prev_qty[lbl] += qty
            fs_total_prev_amt[lbl] += qty * up
        for lbl, qty in row["fore"].items():
            fs_total_fore_qty[lbl] += qty
            fs_total_fore_amt[lbl] += qty * up

    fs_customers = list(
        Forecast.objects.select_related("customer")
        .values_list("customer__customer_name", flat=True)
        .distinct()
        .order_by("customer__customer_name")
    )

    return {
        "fs_rows":            fs_rows,
        "fs_prev_months":     fs_prev_months,
        "fs_fore_months":     fs_fore_months,
        "fs_total_prev_qty":  dict(fs_total_prev_qty),
        "fs_total_fore_qty":  dict(fs_total_fore_qty),
        "fs_total_prev_amt":  dict(fs_total_prev_amt),
        "fs_total_fore_amt":  dict(fs_total_fore_amt),
        "fs_prev_year":       prev_year,
        "fs_fore_year":       current_year,
        "fs_customers":       fs_customers,
    }


def _build_actual_summary(adq: str = "", ad_customer: str = "", ad_month: str = "", user_tz=None):
    """
    Build data for the Actual Delivered tab.

    ad_month: abbreviated month label to filter on, e.g. 'JAN'.
    """
    from datetime import date

    def _parse_date_str(date_str):
        date_str = (date_str or "").strip()
        if not date_str:
            return None
        parts = date_str.split("-")
        if len(parts) < 2:
            return None
        month_name = parts[0].strip().lower()
        try:
            year = int(parts[-1].strip())
        except ValueError:
            return None
        month_int = _MONTH_NAME_TO_NUM.get(month_name)
        if not month_int:
            return None
        label = _SHORT_MONTHS[month_int]
        return year, month_int, label

    if user_tz is None:
        from zoneinfo import ZoneInfo
        user_tz = ZoneInfo("UTC")
    today = datetime.now(tz=user_tz).date()
    current_year = today.year

    qs = (
        Forecast.objects
        .select_related("customer")
        .order_by("customer__customer_name", "part_number")
    )

    if adq:
        qs = qs.filter(
            Q(part_number__icontains=adq) | Q(part_name__icontains=adq)
        )
    if ad_customer:
        qs = qs.filter(customer__customer_name=ad_customer)

    rows_by_key = {}
    months_seen = set()
    years_seen = set()

    for forecast in qs:
        monthly = forecast.monthly_forecasts or []

        first_entry = next(
            (m for m in monthly if isinstance(m, dict)),
            {}
        )
        try:
            unit_price = float(first_entry.get("unit_price", 0) or 0) if isinstance(first_entry, dict) else 0.0
        except (TypeError, ValueError):
            unit_price = 0.0

        customer_name = forecast.customer.customer_name if forecast.customer else "—"
        key = (customer_name, forecast.part_number)

        row = rows_by_key.get(key)
        if not row:
            row = {
                "customer": customer_name,
                "part_number": forecast.part_number,
                "part_name": forecast.part_name,
                "unit_price": unit_price,
                "months": {},
            }
            rows_by_key[key] = row
        else:
            if unit_price:
                row["unit_price"] = unit_price

        months_map = row["months"]

        for entry in monthly:
            if not isinstance(entry, dict):
                continue
            if "actual_quantity" not in entry:
                continue

            parsed = _parse_date_str(entry.get("date", ""))
            if not parsed:
                continue
            yr, mo, label = parsed
            years_seen.add(yr)
            months_seen.add(label)

            try:
                qty = float(entry.get("actual_quantity", 0) or 0)
            except (TypeError, ValueError):
                qty = 0.0

            months_map[label] = months_map.get(label, 0.0) + qty

    ad_rows = list(rows_by_key.values())

    # ── Filter rows by selected month ────────────────────────────────────────
    if ad_month:
        ad_rows = [r for r in ad_rows if r["months"].get(ad_month, 0)]

    ad_months = [_SHORT_MONTHS[i] for i in range(1, 13)]

    from collections import defaultdict as _dd

    ad_total_qty = _dd(float)
    ad_total_amt = _dd(float)

    for row in ad_rows:
        up = row["unit_price"]
        for lbl, qty in row["months"].items():
            ad_total_qty[lbl] += qty
            ad_total_amt[lbl] += qty * up

    ad_year = sorted(years_seen)[0] if years_seen else current_year

    ad_customers = list(
        Forecast.objects.filter(
            monthly_forecasts__0__actual_quantity__isnull=False
        )
        .select_related("customer")
        .values_list("customer__customer_name", flat=True)
        .distinct()
        .order_by("customer__customer_name")
    )

    return {
        "ad_rows": ad_rows,
        "ad_months": ad_months,
        "ad_total_qty": dict(ad_total_qty),
        "ad_total_amt": dict(ad_total_amt),
        "ad_year": ad_year,
        "ad_customers": ad_customers,
    }


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


def _generate_unique_next_tep_code_old(base_tep_code: str) -> str:
    """
    Keep incrementing until tep_code is unique in DB.
    Legacy version kept for compatibility.
    """
    candidate = _next_tep_code(base_tep_code)
    guard = 0
    while TEPCode.objects.filter(tep_code=candidate).exists():
        candidate = _next_tep_code(candidate)
        guard += 1
        if guard > 500:
            raise ValueError("Could not generate unique tep_code after many attempts.")
    return candidate


@require_POST
@login_required
@user_passes_test(is_admin)
def update_material_stock(request):
    material_id = (request.POST.get("material_id") or "").strip()
    on_hand_qty_raw = (request.POST.get("on_hand_qty") or "").strip()
    sq = (request.GET.get("sq") or request.POST.get("sq") or "").strip()

    if not material_id:
        messages.error(request, "Missing material_id.")
        return redirect(_admin_dashboard_url("materials_bom", "stocks"))

    try:
        on_hand_qty = int(on_hand_qty_raw) if on_hand_qty_raw != "" else 0
        if on_hand_qty < 0:
            on_hand_qty = 0
    except Exception:
        messages.error(request, "On hand qty must be a whole number.")
        return redirect(_admin_dashboard_url("materials_bom", "stocks"))

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

    return redirect(_admin_dashboard_url("materials_bom", "stocks", sq=sq))


@never_cache
@login_required
@user_passes_test(is_admin)
def admin_dashboard(request):
    requested_tab = (request.GET.get("tab") or "customers").strip().lower()
    materials_bom_tab = (request.GET.get("mbtab") or "").strip().lower()

    if requested_tab == "materials_bom":
        materials_bom_tab = materials_bom_tab or "bom_master"
        tab = materials_bom_tab
        sidebar_tab = "materials_bom"
    elif requested_tab in {"bom_master", "materials", "stocks"}:
        tab = requested_tab
        materials_bom_tab = requested_tab
        sidebar_tab = "materials_bom"
    else:
        tab = requested_tab
        sidebar_tab = requested_tab

    action = ""

    # ── Timezone ─────────────────────────────────────────────────────────────
    user_tz = _get_user_tz(request)
    _user_now = datetime.now(tz=user_tz)
    _user_today = _user_now.date()

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()

        if action == "run_prototype_forecast":
            customer_id_raw = (request.POST.get("customer_id") or "").strip()
            part_code = (request.POST.get("part_code") or "").strip()
            forecast_qty_raw = (request.POST.get("forecast_qty") or "").strip()
            schedule_month = (request.POST.get("schedule_month") or "").strip()

            if not customer_id_raw:
                messages.error(request, "Customer is required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast_run")

            try:
                customer_id = int(customer_id_raw)
            except Exception:
                messages.error(request, "Invalid customer selected.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast_run")

            customer = Customer.objects.filter(id=customer_id).first()
            if not customer:
                messages.error(request, "Selected customer was not found.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast_run")

            if not part_code:
                messages.error(request, "Part code is required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast_run")

            if not forecast_qty_raw:
                messages.error(request, "Forecast quantity is required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast_run")

            try:
                forecast_qty = int(forecast_qty_raw)
                if forecast_qty < 0:
                    raise ValueError
            except Exception:
                messages.error(
                    request,
                    "Forecast quantity must be a whole number greater than or equal to 0.",
                )
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast_run")

            if schedule_month:
                try:
                    datetime.strptime(schedule_month, "%Y-%m")
                except Exception:
                    messages.error(
                        request,
                        "Schedule month must be in YYYY-MM format.",
                    )
                    return redirect(reverse("app:admin_dashboard") + "?tab=forecast_run")

            inputs = [
                ForecastInput(
                    customer_id=customer.id,
                    customer_name=customer.customer_name,
                    part_code=part_code,
                    forecast_qty=forecast_qty,
                    schedule_month=schedule_month,
                )
            ]

            run = run_forecast_and_save(
                inputs,
                created_by=request.user,
                note=f"Manual forecast for {customer.customer_name} / {part_code} ({schedule_month or 'no month'})",
            )

            messages.success(
                request,
                f"Forecast run #{run.id} created for {customer.customer_name} - {part_code} (qty={forecast_qty}).",
            )
            return redirect(reverse("app:admin_dashboard") + "?tab=forecast_run")


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

            return redirect(_admin_dashboard_url("materials_bom", "stocks"))

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
            return redirect(_admin_dashboard_url("materials_bom", "stocks"))

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
            return redirect(_admin_dashboard_url("materials_bom", "stocks"))

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
            return redirect(_admin_dashboard_url("materials_bom", "stocks"))

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
                    # Try new method first, fall back to old if it fails
                    try:
                        new_code = _generate_unique_next_tep_code(old_tep.tep_code)
                    except Exception:
                        new_code = _generate_unique_next_tep_code_old(old_tep.tep_code)

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

        if action == "save_part_master":
            part_code = _normalize_space(request.POST.get("part_code"))
            part_name = _normalize_space(request.POST.get("part_name"))

            if not part_code:
                messages.error(request, "Part code is required.")
                return redirect(_admin_dashboard_url("materials_bom", "bom_master"))

            if not part_name:
                messages.error(request, "Part name is required.")
                return redirect(_admin_dashboard_url("materials_bom", "bom_master"))

            try:
                PartMaster.objects.update_or_create(
                    part_code=part_code,
                    defaults={
                        "part_name": part_name,
                        "is_active": True,
                    },
                )
                messages.success(request, f"Saved part code: {part_code}")
                return redirect(_admin_dashboard_url("materials_bom", "bom_master", bpart=part_code))
            except Exception as e:
                messages.error(request, f"Failed to save part code: {e}")
                return redirect(_admin_dashboard_url("materials_bom", "bom_master"))

        if action == "save_bom_master":
            part_code = _normalize_space(request.POST.get("part_code"))
            source_tep_id = (request.POST.get("source_tep_id") or "").strip()
            mat_codes = request.POST.getlist("mat_partcode[]") or request.POST.getlist("mat_partcode")
            dims = request.POST.getlist("dim_qty[]") or request.POST.getlist("dim_qty")
            losses = request.POST.getlist("loss_percent[]") or request.POST.getlist("loss_percent")

            if not part_code:
                messages.error(request, "Part code is required.")
                return redirect(_admin_dashboard_url("materials_bom", "bom_master"))

            source_tep = None
            if source_tep_id:
                source_tep = TEPCode.objects.filter(id=source_tep_id).first()
            if source_tep is None:
                source_tep = _preferred_tep_for_part_code(part_code)

            rows = []
            for i, mat_code in enumerate(mat_codes):
                mat_code = _normalize_space(mat_code)
                dim_qty = (dims[i] if i < len(dims) else "").strip()
                loss_percent = (losses[i] if i < len(losses) else "").strip()
                if not mat_code and not dim_qty and not loss_percent:
                    continue
                rows.append({
                    "mat_partcode": mat_code,
                    "dim_qty": dim_qty,
                    "loss_percent": loss_percent or "10",
                })

            try:
                replace_bom_for_partcode(part_code=part_code, rows=rows, source_tep=source_tep)
                messages.success(request, f"Saved BOM for {part_code}.")
            except Exception as e:
                messages.error(request, f"Failed to save BOM: {e}")

            return redirect(_admin_dashboard_url("materials_bom", "bom_master", bpart=part_code))

        if action == "delete_bom_master":
            part_code = _normalize_space(request.POST.get("part_code"))
            if not part_code:
                messages.error(request, "Missing part code.")
                return redirect(_admin_dashboard_url("materials_bom", "bom_master"))
            deleted = BOMMaterial.objects.filter(part_code=part_code).delete()[0]
            messages.success(request, f"Deleted {deleted} BOM rows for {part_code}.")
            return redirect(_admin_dashboard_url("materials_bom", "bom_master", bpart=part_code))

        if action == "upload_bom_csv":
            bom_csv_file = request.FILES.get("bom_csv_file")

            try:
                result = import_bom_csv_file(bom_csv_file, created_by=request.user)
                messages.success(
                    request,
                    f"BOM CSV imported successfully. "
                    f"Part codes: {result['parts_count']}, "
                    f"BOM rows: {result['rows_count']}, "
                    f"new parts: {result['created_parts']}, "
                    f"new materials: {result['created_materials']}."
                )
            except Exception as e:
                messages.error(request, f"Failed to import BOM CSV: {e}")

            return redirect(_admin_dashboard_url("materials_bom", "bom_master"))

        if action == "add_customer_full":
            customer_name = _normalize_space(request.POST.get("customer_name"))
            part_code = _normalize_space(request.POST.get("part_code"))
            part_name = _normalize_space(request.POST.get("part_name"))
            tep_code = _normalize_space(request.POST.get("tep_code"))

            # Optional: also create the first material row (if provided)
            mat_partcode = _normalize_space(request.POST.get("mat_partcode"))
            dim_qty_raw = (request.POST.get("dim_qty") or "").strip()
            loss_raw = (request.POST.get("loss_percent") or "").strip()
            create_material = bool(mat_partcode)

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
                dim_qty = float(dim_qty_raw) if dim_qty_raw != "" else 0
            except Exception:
                messages.error(request, "Dim/Qty must be numeric.")
                return redirect(reverse("app:admin_dashboard") + "?tab=customers")

            try:
                loss_percent = float(loss_raw) if loss_raw != "" else 10
            except Exception:
                messages.error(request, "Loss % must be numeric.")
                return redirect(reverse("app:admin_dashboard") + "?tab=customers")

            try:
                with transaction.atomic():
                    customer, _ = Customer.objects.get_or_create(customer_name=customer_name)
                    _ensure_customer_part_entry(customer, part_code, part_name)

                    # Keep PartMaster synced
                    PartMaster.objects.update_or_create(
                        part_code=part_code,
                        defaults={"part_name": part_name, "is_active": True},
                    )

                    # Create TEP only if it does not exist yet for this customer+part_code+tep_code
                    tep, _ = TEPCode.objects.get_or_create(
                        customer=customer,
                        part_code=part_code,
                        tep_code=tep_code,
                        defaults=({"is_active": True} if hasattr(TEPCode, "is_active") else {}),
                    )

                    if create_material:
                        master = MaterialList.objects.filter(mat_partcode=mat_partcode).first()
                        if not master:
                            master = MaterialList.objects.create(
                                mat_partcode=mat_partcode,
                                mat_partname=mat_partcode,
                                mat_maker="Unknown",
                                unit="pc",
                            )

                        bom_obj, created = BOMMaterial.objects.get_or_create(
                            part_code=part_code,
                            mat_partcode=master.mat_partcode,
                            defaults={
                                "source_tep": tep,
                                "material": master,
                                "mat_partname": master.mat_partname,
                                "mat_maker": master.mat_maker,
                                "unit": master.unit,
                                "dim_qty": dim_qty,
                                "loss_percent": loss_percent,
                            },
                        )

                        if not created:
                            messages.error(request, f"Material already exists for Part Code {part_code} + {mat_partcode}.")
                            return redirect(reverse("app:admin_dashboard") + "?tab=customers")

                        _sync_legacy_material_from_bom(
                            tep=tep,
                            master=master,
                            dim_qty=dim_qty,
                            loss_percent=loss_percent,
                        )

                if create_material:
                    messages.success(request, f"Saved: {customer_name} | {part_code} | {tep_code} | {mat_partcode}")
                else:
                    messages.success(request, f"Saved customer record: {customer_name} | {part_code} | {tep_code}")

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
                return redirect(_admin_dashboard_url("materials_bom", "materials"))

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

            return redirect(_admin_dashboard_url("materials_bom", "materials"))

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
                return redirect(_admin_dashboard_url("materials_bom", "materials"))

            try:
                obj = MaterialList.objects.get(id=mat_id)

                if not mat_partcode:
                    messages.error(request, "Part Code is required.")
                    return redirect(_admin_dashboard_url("materials_bom", "materials"))

                if mat_partcode != obj.mat_partcode:
                    if MaterialList.objects.filter(mat_partcode=mat_partcode).exclude(id=obj.id).exists():
                        messages.error(request, f"Part Code already exists: {mat_partcode}")
                        return redirect(_admin_dashboard_url("materials_bom", "materials"))

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

            return redirect(_admin_dashboard_url("materials_bom", "materials"))

        if action == "add_forecast":
            customer_name = _normalize_space(request.POST.get("customer_name"))
            part_name = _normalize_space(request.POST.get("part_name"))
            part_number = _normalize_space(request.POST.get("part_number"))
            month = request.POST.get("month")
            year = request.POST.get("year")
            unit_price = request.POST.get("unit_price")
            quantity = request.POST.get("quantity")

            if not customer_name:
                messages.error(request, "Customer name is required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            if not part_name:
                messages.error(request, "Part name is required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            if not part_number:
                messages.error(request, "Part number is required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            if not month or not year:
                messages.error(request, "Month and year are required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            if not unit_price:
                messages.error(request, "Unit price is required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            if not quantity:
                messages.error(request, "Quantity is required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            try:
                unit_price = float(unit_price)
                quantity = float(quantity)
            except ValueError:
                messages.error(request, "Unit price and quantity must be valid numbers.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            date_str = f"{month}-{year}"

            customer, created = Customer.objects.get_or_create(
                customer_name=customer_name,
                defaults={"parts": []}
            )

            existing_forecast = Forecast.objects.filter(
                customer=customer,
                part_number=part_number
            ).first()

            # If a forecast for this part already exists for the customer,
            # allow adding a new month as long as the (month, year) combo
            # does not already exist in its monthly_forecasts.
            if existing_forecast:
                monthly = existing_forecast.monthly_forecasts or []

                # Check for duplicate month/year
                duplicate = any(
                    isinstance(m, dict) and str(m.get("date", "")).strip().lower() == date_str.lower()
                    for m in monthly
                )

                if duplicate:
                    messages.error(
                        request,
                        f"Forecast for part '{part_number}' already exists for {date_str} for this customer."
                    )
                    return redirect(
                        reverse("app:admin_dashboard")
                        + "?tab=forecast"
                        + ("&fq=" + request.GET.get("fq", "") if request.GET.get("fq") else "")
                    )

                # Append the new month entry to the existing forecast
                monthly.append(
                    {
                        "date": date_str,
                        "unit_price": unit_price,
                        "quantity": quantity,
                    }
                )
                existing_forecast.monthly_forecasts = monthly
                existing_forecast.part_name = part_name
                existing_forecast.save()
                forecast = existing_forecast
            else:
                monthly_forecast = [
                    {
                        "date": date_str,
                        "unit_price": unit_price,
                        "quantity": quantity,
                    }
                ]

                forecast = Forecast.objects.create(
                    customer=customer,
                    part_number=part_number,
                    part_name=part_name,
                    monthly_forecasts=monthly_forecast,
                )

            customer_parts = customer.parts or []
            part_exists = any(
                isinstance(p, dict) and str(p.get("Partcode", "")).strip() == part_number
                for p in customer_parts
            )
            if not part_exists:
                customer_parts.append({"Partcode": part_number, "Partname": part_name})
                customer.parts = customer_parts
                customer.save()

            messages.success(request, f"Forecast added successfully for {customer_name} - {part_number}")
            return redirect(reverse("app:admin_dashboard") + "?tab=forecast" + ("&fq=" + request.GET.get("fq", "") if request.GET.get("fq") else ""))

        if action == "update_forecast":
            original_customer = _normalize_space(request.POST.get("original_customer_name"))
            original_part_number = _normalize_space(request.POST.get("original_part_number"))
            customer_name = _normalize_space(request.POST.get("customer_name"))
            part_name = _normalize_space(request.POST.get("part_name"))
            part_number = _normalize_space(request.POST.get("part_number"))
            month = request.POST.get("month")
            year = request.POST.get("year")
            unit_price = request.POST.get("unit_price")
            quantity = request.POST.get("quantity")
            original_date = (request.POST.get("original_date") or "").strip()

            if not original_customer or not original_part_number:
                messages.error(request, "Original customer and part number are required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            if not customer_name:
                messages.error(request, "Customer name is required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            if not part_name:
                messages.error(request, "Part name is required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            if not part_number:
                messages.error(request, "Part number is required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            if not unit_price:
                messages.error(request, "Unit price is required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            if not quantity:
                messages.error(request, "Quantity is required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            try:
                unit_price = float(unit_price)
                quantity = float(quantity)
            except ValueError:
                messages.error(request, "Unit price and quantity must be valid numbers.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            # Find the original customer
            original_customer_obj = Customer.objects.filter(customer_name__iexact=original_customer).first()
            if not original_customer_obj:
                messages.error(request, f"Original customer '{original_customer}' not found.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            # Find the forecast to update
            forecast = Forecast.objects.filter(
                customer=original_customer_obj,
                part_number=original_part_number
            ).first()

            if not forecast:
                messages.error(request, f"Forecast not found for {original_customer} - {original_part_number}")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            # Handle customer change if needed
            if customer_name != original_customer:
                new_customer, created = Customer.objects.get_or_create(
                    customer_name=customer_name,
                    defaults={"parts": []}
                )
                forecast.customer = new_customer
            else:
                forecast.customer = original_customer_obj

            # Update basic fields
            forecast.part_number = part_number
            forecast.part_name = part_name

            # We always expect month/year for per‑month editing
            if not month or not year:
                messages.error(request, "Month and year are required to update a forecast.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            date_str = f"{month}-{year}"

            # Update only the targeted monthly entry (identified by original_date)
            monthly_list = list(forecast.monthly_forecasts or [])
            updated = False
            original_date_normalized = original_date.lower()

            for entry in monthly_list:
                if not isinstance(entry, dict):
                    continue
                existing_date = str(entry.get("date", "")).strip()
                if original_date and existing_date.lower() == original_date_normalized:
                    entry["date"] = date_str
                    entry["unit_price"] = unit_price
                    entry["quantity"] = quantity
                    updated = True
                    break

            if not updated:
                # If we didn't find the original month, append as a new one
                monthly_list.append(
                    {
                        "date": date_str,
                        "unit_price": unit_price,
                        "quantity": quantity,
                    }
                )

            # Prevent duplicate month/year entries for the same forecast
            seen_dates = set()
            deduped = []
            for entry in monthly_list:
                if not isinstance(entry, dict):
                    continue
                d = str(entry.get("date", "")).strip()
                key = d.lower()
                if key and key not in seen_dates:
                    seen_dates.add(key)
                    deduped.append(entry)

            forecast.monthly_forecasts = deduped

            # Save the forecast
            forecast.save()

            # Update customer.parts for both old and new customers
            if customer_name != original_customer:
                # Remove from old customer's parts if no other forecasts use it
                other_forecasts = Forecast.objects.filter(
                    customer=original_customer_obj,
                    part_number=original_part_number
                ).exclude(id=forecast.id).exists()
                
                if not other_forecasts:
                    old_parts = original_customer_obj.parts or []
                    updated_parts = [
                        p for p in old_parts 
                        if not (isinstance(p, dict) and str(p.get("Partcode", "")).strip() == original_part_number)
                    ]
                    original_customer_obj.parts = updated_parts
                    original_customer_obj.save()
                
                # Add to new customer's parts
                new_customer = forecast.customer
                new_parts = new_customer.parts or []
                part_exists = any(
                    isinstance(p, dict) and str(p.get("Partcode", "")).strip() == part_number
                    for p in new_parts
                )
                if not part_exists:
                    new_parts.append({"Partcode": part_number, "Partname": part_name})
                    new_customer.parts = new_parts
                    new_customer.save()
            else:
                # Update part in same customer's parts if needed
                if part_number != original_part_number:
                    # Remove old part
                    old_parts = original_customer_obj.parts or []
                    updated_parts = [
                        p for p in old_parts 
                        if not (isinstance(p, dict) and str(p.get("Partcode", "")).strip() == original_part_number)
                    ]
                    
                    # Add new part if not exists
                    part_exists = any(
                        isinstance(p, dict) and str(p.get("Partcode", "")).strip() == part_number
                        for p in updated_parts
                    )
                    if not part_exists:
                        updated_parts.append({"Partcode": part_number, "Partname": part_name})
                    
                    original_customer_obj.parts = updated_parts
                    original_customer_obj.save()

            messages.success(request, f"Forecast updated successfully for {customer_name} - {part_number}")
            return redirect(reverse("app:admin_dashboard") + "?tab=forecast" + ("&fq=" + request.GET.get("fq", "") if request.GET.get("fq") else ""))

        if action == "delete_forecast":
            customer_name = _normalize_space(request.POST.get("customer_name"))
            part_number = _normalize_space(request.POST.get("part_number"))
            forecast_id = (request.POST.get("forecast_id") or "").strip()
            date_str = (request.POST.get("date") or "").strip()

            if not customer_name or not part_number:
                messages.error(request, "Customer name and part number are required.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            # Find the customer
            customer = Customer.objects.filter(customer_name__iexact=customer_name).first()
            if not customer:
                messages.error(request, f"Customer '{customer_name}' not found.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            # If a specific forecast id and date are provided, delete only that month
            if forecast_id and date_str:
                forecast = Forecast.objects.filter(
                    id=forecast_id,
                    customer=customer,
                    part_number=part_number,
                ).first()

                if not forecast:
                    messages.error(request, f"Forecast not found for {customer_name} - {part_number}")
                    return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

                monthly_list = list(forecast.monthly_forecasts or [])
                target = date_str.strip().lower()
                new_monthly = [
                    m
                    for m in monthly_list
                    if not (
                        isinstance(m, dict)
                        and str(m.get("date", "")).strip().lower() == target
                    )
                ]

                if not new_monthly:
                    # No more months left → delete the whole forecast
                    forecast.delete()
                else:
                    forecast.monthly_forecasts = new_monthly
                    forecast.save()

                # If no other forecasts remain for this part, clean up customer.parts
                other_forecasts = Forecast.objects.filter(
                    customer=customer,
                    part_number=part_number,
                ).exists()

                if not other_forecasts:
                    customer_parts = customer.parts or []
                    updated_parts = [
                        p
                        for p in customer_parts
                        if not (
                            isinstance(p, dict)
                            and str(p.get("Partcode", "")).strip() == part_number
                        )
                    ]
                    customer.parts = updated_parts
                    customer.save()

                messages.success(
                    request,
                    f"Forecast month deleted successfully: {customer_name} - {part_number} ({date_str})",
                )
                return redirect(
                    reverse("app:admin_dashboard")
                    + "?tab=forecast"
                    + ("&fq=" + request.GET.get("fq", "") if request.GET.get("fq") else "")
                )

            # Fallback: delete all forecasts for this customer/part if no specific month given
            forecasts_qs = Forecast.objects.filter(
                customer=customer,
                part_number=part_number
            )

            if not forecasts_qs.exists():
                messages.error(request, f"Forecast not found for {customer_name} - {part_number}")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            forecast_info = f"{customer_name} - {part_number}"
            forecasts_qs.delete()

            other_forecasts = Forecast.objects.filter(
                customer=customer,
                part_number=part_number
            ).exists()

            if not other_forecasts:
                customer_parts = customer.parts or []
                updated_parts = [
                    p for p in customer_parts 
                    if not (isinstance(p, dict) and str(p.get("Partcode", "")).strip() == part_number)
                ]
                customer.parts = updated_parts
                customer.save()

            messages.success(request, f"Forecast deleted successfully: {forecast_info}")
            return redirect(
                reverse("app:admin_dashboard")
                + "?tab=forecast"
                + ("&fq=" + request.GET.get("fq", "") if request.GET.get("fq") else "")
            )

        if action == "bulk_delete_forecast":
            import re as _re

            # Parse items[N][field] from POST data
            items = {}
            for key, val in request.POST.items():
                m = _re.match(r'^items\[(\d+)\]\[(\w+)\]$', key)
                if m:
                    idx = int(m.group(1))
                    field = m.group(2)
                    if idx not in items:
                        items[idx] = {}
                    items[idx][field] = val.strip()

            if not items:
                messages.error(request, "No items selected for deletion.")
                return redirect(reverse("app:admin_dashboard") + "?tab=forecast")

            deleted_count = 0
            errors = []

            for idx in sorted(items.keys()):
                item = items[idx]
                forecast_id   = item.get("forecast_id", "").strip()
                date_str      = item.get("date", "").strip()
                part_number   = item.get("part_number", "").strip()
                customer_name = item.get("customer", "").strip()

                if not forecast_id or not date_str:
                    continue

                try:
                    forecast = Forecast.objects.select_related("customer").get(id=forecast_id)
                except Forecast.DoesNotExist:
                    errors.append(f"Forecast ID {forecast_id} not found.")
                    continue

                monthly_list = list(forecast.monthly_forecasts or [])
                target = date_str.lower()
                new_monthly = [
                    m for m in monthly_list
                    if not (
                        isinstance(m, dict)
                        and str(m.get("date", "")).strip().lower() == target
                    )
                ]

                customer = forecast.customer
                pn = forecast.part_number

                if not new_monthly:
                    # All months removed — delete the whole forecast record
                    forecast.delete()
                else:
                    forecast.monthly_forecasts = new_monthly
                    forecast.save()

                # Clean up customer.parts if no forecasts remain for this part
                if not Forecast.objects.filter(customer=customer, part_number=pn).exists():
                    customer_parts = customer.parts or []
                    customer.parts = [
                        p for p in customer_parts
                        if not (isinstance(p, dict) and str(p.get("Partcode", "")).strip() == pn)
                    ]
                    customer.save(update_fields=["parts"])

                deleted_count += 1

            if deleted_count:
                messages.success(request, f"Deleted {deleted_count} forecast row(s) successfully.")
            for e in errors:
                messages.error(request, e)

            return redirect(
                reverse("app:admin_dashboard")
                + "?tab=forecast"
                + ("&fq=" + request.GET.get("fq", "") if request.GET.get("fq") else "")
            )

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

    # ── GET: build context ────────────────────────────────────────────────────

    q = (request.GET.get("q") or "").strip()
    customers = build_customer_table(q)

    # Distinct part codes from customer JSON records for forecast input suggestions
    part_codes_set = set()
    for cust in Customer.objects.all():
        for item in cust.parts or []:
            if not isinstance(item, dict):
                continue
            pc = (item.get("Partcode") or "").strip()
            if pc:
                part_codes_set.add(pc)
    all_part_codes = sorted(part_codes_set)

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
    materials_stock_qs = MaterialList.objects.all().order_by("mat_partcode")
    if sq:
        materials_stock_qs = materials_stock_qs.filter(
            Q(mat_partcode__icontains=sq) |
            Q(mat_partname__icontains=sq) |
            Q(mat_maker__icontains=sq)
        )

    materials_stock_qs = materials_stock_qs.select_related("stock", "stock__last_updated_by")

    stock_paginator = Paginator(materials_stock_qs, 8)
    spage = request.GET.get("spage")
    stock_page_obj = stock_paginator.get_page(spage)

    materials_stock_list = stock_page_obj

    mat_ids = [m.id for m in materials_stock_list]
    reserved_map = {
        row["material_id"]: (row["total"] or 0)
        for row in (
            MaterialAllocation.objects
            .filter(material_id__in=mat_ids, status="reserved")
            .values("material_id")
            .annotate(total=Sum("qty_allocated"))
        )
    }

    for m in materials_stock_list:
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
        materials = _build_bom_display_rows(tep)

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
    fmonth = (request.GET.get("fmonth") or "").strip()  # "YYYY-MM"

    # Forecast Run tab (ForecastRun / ForecastLine)
    # Show all matching runs, not only the latest one.
    forecast_runs = ForecastRun.objects.all().order_by("-id")
    if fmonth:
        forecast_runs = forecast_runs.filter(schedule_month=fmonth)

    forecast_latest = forecast_runs.first()

    forecast_lines_qs = ForecastLine.objects.none()
    if forecast_runs.exists():
        forecast_lines_qs = (
            ForecastLine.objects
            .filter(run__in=forecast_runs)
            .select_related("run")
            .order_by("-run_id", "customer_name", "part_code", "mat_partcode", "id")
        )
        if fq:
            forecast_lines_qs = forecast_lines_qs.filter(
                Q(part_code__icontains=fq) |
                Q(mat_partcode__icontains=fq) |
                Q(mat_partname__icontains=fq) |
                Q(tep_code__icontains=fq) |
                Q(customer_name__icontains=fq)
            )

    forecast_paginator = Paginator(forecast_lines_qs, 50)
    fpage = request.GET.get("fpage")
    forecast_page = forecast_paginator.get_page(fpage)
    forecast_grouped = _build_forecast_grouped(forecast_page)

    forecast_totals = {
        "lines": forecast_lines_qs.count()
    }

    # ── Current Forecast Tab ──────────────────────────────────────────────────
    fcustomer = (request.GET.get("fcustomer") or "").strip()
    fmonth_forecast = (request.GET.get("fmonth") or "").strip()  # full month name for forecast tab
    page_number = request.GET.get('page', 1)
    
    forecasts_qs = Forecast.objects.select_related("customer").order_by("-id")
    
    if fq:
        forecasts_qs = forecasts_qs.filter(
            Q(part_number__icontains=fq)
            | Q(part_name__icontains=fq)
            | Q(customer__customer_name__icontains=fq)
        )
    
    if fcustomer:
        forecasts_qs = forecasts_qs.filter(customer__customer_name=fcustomer)

    # Get total count before month-filtering (month filter happens in Python
    # because monthly_forecasts is a JSONField list)
    forecasts_total_pre_month = forecasts_qs.count()

    # ── Month filter for the Forecast tab ────────────────────────────────────
    # We need to filter + expand monthly entries by the chosen month.
    # We do this in Python after fetching, then re-paginate the expanded list.

    if fmonth_forecast:
        # Build a flat list of (forecast_obj, month_entry) tuples for the
        # selected month only, then paginate that flat list.
        expanded_rows = []
        for forecast in forecasts_qs.iterator():
            for entry in (forecast.monthly_forecasts or []):
                if not isinstance(entry, dict):
                    continue
                date_str = str(entry.get("date", "")).strip()
                # date_str is like "January-2026"; compare the month part
                entry_month = date_str.split("-")[0].strip() if "-" in date_str else date_str
                if entry_month.lower() == fmonth_forecast.lower():
                    # Attach a synthetic attribute so the template can read it
                    # without change — reuse forecast obj with overridden monthly
                    import copy
                    fc = copy.copy(forecast)
                    fc.monthly_forecasts = [entry]
                    # unit_price_display for rows without monthly override
                    try:
                        fc.unit_price_display = float(entry.get("unit_price", 0) or 0)
                    except (TypeError, ValueError):
                        fc.unit_price_display = 0
                    fc.quantity_display = float(entry.get("quantity", 0) or 0)
                    expanded_rows.append(fc)

        forecasts_total = len(expanded_rows)
        paginator = Paginator(expanded_rows, 8)
        forecasts_page = paginator.get_page(page_number)
        forecasts_list = list(forecasts_page)

    else:
        forecasts_total = forecasts_total_pre_month
        paginator = Paginator(forecasts_qs, 8)
        forecasts_page = paginator.get_page(page_number)

        forecasts_list = []
        for forecast in forecasts_page:
            first_monthly = None
            if forecast.monthly_forecasts and len(forecast.monthly_forecasts) > 0:
                first_monthly = forecast.monthly_forecasts[0]
                if isinstance(first_monthly, dict):
                    first_monthly = {
                        "date": first_monthly.get("date", ""),
                        "unit_price": float(first_monthly.get("unit_price", 0)),
                        "quantity": float(first_monthly.get("quantity", 0)),
                    }

            forecast.unit_price_display = first_monthly.get("unit_price", 0) if first_monthly else 0
            forecast.quantity_display = first_monthly.get("quantity", 0) if first_monthly else 0
            forecasts_list.append(forecast)

    forecasts_monthly_json = {}
    for f in forecasts_list:
        monthly_list = []
        for m in (f.monthly_forecasts or []):
            if isinstance(m, dict):
                monthly_list.append({
                    "date": m.get("date", ""),
                    "unit_price": float(m.get("unit_price", 0)),
                    "quantity": float(m.get("quantity", 0)),
                })
        forecasts_monthly_json[str(f.id)] = monthly_list

    forecasts_monthly_json = json.dumps(forecasts_monthly_json, default=str)

    all_customers = Customer.objects.all().order_by("customer_name")

    # ── Previous Forecast Tab (from old code) ─────────────────────────────────
    pf_customer = (request.GET.get("pf_customer") or "").strip()
    pf_q = (request.GET.get("pf_q") or "").strip()
    # pf_month holds an abbreviated month label, e.g. "JAN"
    pf_month = (request.GET.get("pf_month") or "").strip().upper()
    
    current_year = _user_today.year
    previous_year = current_year - 1
    
    prev_data = {}
    if tab == "previous_forecast":
        month_map = {
            1: 'JAN', 2: 'FEB', 3: 'MAR', 4: 'APR', 5: 'MAY', 6: 'JUN',
            7: 'JUL', 8: 'AUG', 9: 'SEP', 10: 'OCT', 11: 'NOV', 12: 'DEC'
        }
        
        qs = Forecast.objects.select_related("customer").all()
        
        if pf_customer:
            qs = qs.filter(customer__customer_name=pf_customer)
        
        if pf_q:
            qs = qs.filter(
                Q(part_number__icontains=pf_q) | 
                Q(part_name__icontains=pf_q)
            )
        
        rows_by_key = {}
        total_qty = defaultdict(float)
        total_amt = defaultdict(float)
        
        for forecast in qs:
            monthly = forecast.monthly_forecasts or []
            
            first_entry = next(
                (m for m in monthly if isinstance(m, dict)),
                {}
            )
            
            try:
                unit_price = float(first_entry.get("unit_price", 0)) if isinstance(first_entry, dict) else 0.0
            except (TypeError, ValueError):
                unit_price = 0.0
            
            customer_name = forecast.customer.customer_name if forecast.customer else "—"
            key = (customer_name, forecast.part_number)
            
            if key not in rows_by_key:
                rows_by_key[key] = {
                    "customer": customer_name,
                    "part_number": forecast.part_number,
                    "part_name": forecast.part_name,
                    "unit_price": unit_price,
                    "months": defaultdict(float)
                }
            
            row = rows_by_key[key]
            
            for entry in monthly:
                if not isinstance(entry, dict):
                    continue
                
                date_str = entry.get("date", "").strip()
                if not date_str:
                    continue
                
                try:
                    parts = date_str.split('-')
                    if len(parts) < 2:
                        continue
                    
                    month_name = parts[0].strip().lower()
                    year = int(parts[-1].strip())
                    
                    if year != previous_year:
                        continue
                    
                    month_num = _MONTH_NAME_TO_NUM.get(month_name)
                    if not month_num:
                        continue
                    
                    month_abbr = _SHORT_MONTHS[month_num]
                    
                    try:
                        qty = float(entry.get("quantity", 0) or 0)
                    except (TypeError, ValueError):
                        qty = 0.0
                    
                    row["months"][month_abbr] += qty
                    total_qty[month_abbr] += qty
                    total_amt[month_abbr] += qty * unit_price
                    
                except (ValueError, IndexError, KeyError):
                    continue
        
        prev_rows = []
        for key, row in rows_by_key.items():
            row["months"] = dict(row["months"])
            prev_rows.append(row)
        
        # ── Filter by selected month ──────────────────────────────────────────
        if pf_month:
            prev_rows = [r for r in prev_rows if r["months"].get(pf_month, 0)]

        prev_rows.sort(key=lambda x: (x["customer"], x["part_number"]))
        
        prev_customers = list(set(
            Forecast.objects.filter(
                monthly_forecasts__0__date__icontains=str(previous_year)
            ).values_list("customer__customer_name", flat=True).distinct().order_by("customer__customer_name")
        ))
        
        prev_data = {
            "prev_rows": prev_rows,
            "prev_total_qty": dict(total_qty),
            "prev_total_amt": dict(total_amt),
            "prev_customers": prev_customers,
            "pf_customer": pf_customer,
            "pf_q": pf_q,
            "pf_month": pf_month,
            "fs_prev_months": ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 
                               'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'],
            "fs_prev_year": previous_year,
        }

    # ── Forecast Summary tab data ─────────────────────────────────────────────
    fsq = (request.GET.get("fsq") or "").strip()
    fsq_customer = (request.GET.get("fsq_customer") or "").strip()
    # fsq_month holds an abbreviated month label, e.g. "JAN"
    fsq_month = (request.GET.get("fsq_month") or "").strip().upper()

    fs_data = {}
    if tab == "forecast_summary":
        fs_data = _build_forecast_summary(fsq=fsq, fsq_customer=fsq_customer, fsq_month=fsq_month, user_tz=user_tz)

    # Actual Delivered tab data
    adq = (request.GET.get("adq") or "").strip()
    ad_customer = (request.GET.get("ad_customer") or "").strip()
    # ad_month holds an abbreviated month label, e.g. "JAN"
    ad_month = (request.GET.get("ad_month") or "").strip().upper()

    ad_data = {}
    if tab == "actual_delivered":
        ad_data = _build_actual_summary(adq=adq, ad_customer=ad_customer, ad_month=ad_month, user_tz=user_tz)

    # ── Forecast Analytics (from old code) ────────────────────────────────────
    all_forecasts = Forecast.objects.all()
    
    unique_customers = set()
    for forecast in all_forecasts:
        if forecast.customer:
            unique_customers.add(forecast.customer.id)
    forecast_customers_count = len(unique_customers)
    
    current_month = _user_now.strftime('%B')
    current_year = _user_today.year
    active_this_month = set()
    
    for forecast in all_forecasts:
        if not forecast.customer:
            continue
        for monthly in (forecast.monthly_forecasts or []):
            if isinstance(monthly, dict):
                date_str = monthly.get('date', '')
                if current_month in date_str and str(current_year) in date_str:
                    active_this_month.add(forecast.customer.id)
                    break
    
    part_numbers_count = all_forecasts.count()
    
    unique_part_numbers = set()
    for forecast in all_forecasts:
        unique_part_numbers.add(forecast.part_number)
    
    total_forecast_entries = 0
    for forecast in all_forecasts:
        total_forecast_entries += len(forecast.monthly_forecasts or [])
    
    all_months = set()
    for forecast in all_forecasts:
        for monthly in (forecast.monthly_forecasts or []):
            if isinstance(monthly, dict):
                date_str = monthly.get('date', '')
                if date_str:
                    all_months.add(date_str)
    active_months_count = len(all_months)
    
    fs_fore_months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 
                      'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']

    previous_year_parts = set()
    current_year_parts = set()

    for forecast in all_forecasts:
        for monthly in (forecast.monthly_forecasts or []):
            if isinstance(monthly, dict):
                date_str = monthly.get('date', '')
                if str(previous_year) in date_str:
                    previous_year_parts.add(forecast.part_number)
                if str(current_year) in date_str:
                    current_year_parts.add(forecast.part_number)

    previous_year_count = len(previous_year_parts)
    current_year_count = len(current_year_parts)

    # ── Build final context ───────────────────────────────────────────────────
    bpart = (request.GET.get("bpart") or "").strip()
    bq = (request.GET.get("bq") or "").strip()
    bom_ctx = _build_bom_master_context(bpart, bq)

    part_master_options = list(
        PartMaster.objects
        .filter(is_active=True)
        .order_by("part_code")
    )
    part_master_json = {
        (row.part_code or "").strip(): (row.part_name or "").strip()
        for row in part_master_options
        if (row.part_code or "").strip()
    }

    context = {
        "tab": tab,

        "customers_count": Customer.objects.count(),
        "tep_count": TEPCode.objects.count(),
        "materials_count": BOMMaterial.objects.count() if hasattr(BOMMaterial, "objects") else Material.objects.count(),
        "users_count": User.objects.count(),
        "forecasts_count": Forecast.objects.count(),

        "customers": customers,
        "q": q,
        "part_master_options": part_master_options,
        "part_master_json": json.dumps(part_master_json, ensure_ascii=False),

        "mq": mq,
        "sq": sq,
        "material_total": material_total,
        "materials_stock_list": materials_stock_list,
        "stock_page_obj": stock_page_obj,
        "material_list": material_list,
        "page_obj": page_obj,

        "uq": uq,
        "user_total": user_total,
        "users_page": users_page,

        "fq": fq,
        "fcustomer": fcustomer,
        "fmonth": fmonth,
        "fmonth_forecast": fmonth_forecast,
        "forecast_latest": forecast_latest,
        "forecast_page": forecast_page,
        "forecast_grouped": forecast_grouped,
        "forecast_totals": forecast_totals,
        "forecasts_list": forecasts_page,
        "forecasts_total": forecasts_total,
        "forecasts_monthly_json": forecasts_monthly_json,
        "all_customers": all_customers,
        "forecast_customers": Customer.objects.filter(forecasts__isnull=False).distinct().order_by("customer_name"),

        "master_map_json": json.dumps(master_map, ensure_ascii=False),

        # Previous Forecast
        **prev_data,

        # Forecast Summary
        "fsq":         fsq,
        "fsq_customer": fsq_customer,
        "fsq_month": fsq_month,
        **fs_data,

        # Actual Delivered
        "adq": adq,
        "ad_customer": ad_customer,
        "ad_month": ad_month,
        **ad_data,

        # Analytics
        "forecast_customers_count": forecast_customers_count,
        "forecast_customers_active": len(active_this_month),
        "part_numbers_count": part_numbers_count,
        "unique_part_numbers": len(unique_part_numbers),
        "total_forecast_entries": total_forecast_entries,
        "active_months_count": active_months_count,
        "fs_fore_months": fs_fore_months,
        
        "previous_year_count": previous_year_count,
        "current_year_count": current_year_count,
        "display_prev_year": previous_year,
        "display_curr_year": current_year,

        # BOM Master
        **bom_ctx,

        # Timezone
        "user_timezone": request.session.get("user_timezone", "UTC"),
        "user_now": _user_now,
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
def part_master_lookup(request):
    part_code = (request.GET.get("part_code") or "").strip()
    if not part_code:
        return JsonResponse({"ok": False, "error": "Missing part_code."}, status=400)

    row = (
        PartMaster.objects
        .filter(is_active=True)
        .filter(Q(part_code__iexact=part_code))
        .first()
    )
    if not row:
        return JsonResponse({"ok": False, "error": "Part code not found."}, status=404)

    return JsonResponse({
        "ok": True,
        "part_code": row.part_code,
        "part_name": row.part_name,
    })


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


@require_GET
@login_required
@user_passes_test(is_admin)
def forecast_qty_lookup(request):
    part_number = (request.GET.get("part_number") or "").strip()
    schedule_month = (request.GET.get("schedule_month") or "").strip()
    customer_id = (request.GET.get("customer_id") or "").strip()

    if not part_number or not schedule_month:
        return JsonResponse({"ok": False, "quantity": 0})

    month_key = _parse_schedule_month_key(schedule_month)
    forecasts = Forecast.objects.filter(part_number__iexact=part_number)
    
    # Filter by customer if provided
    if customer_id:
        try:
            customer_id_int = int(customer_id)
            forecasts = forecasts.filter(customer_id=customer_id_int)
        except ValueError:
            pass

    total_qty = 0.0

    for forecast in forecasts:
        for entry in (forecast.monthly_forecasts or []):
            if not isinstance(entry, dict):
                continue

            entry_key = _parse_schedule_month_key(entry.get("date", ""))
            if month_key and entry_key != month_key:
                continue

            try:
                total_qty += float(entry.get("quantity", 0) or 0)
            except Exception:
                continue

    try:
        quantity_value = int(total_qty) if float(total_qty).is_integer() else round(total_qty, 5)
    except Exception:
        quantity_value = 0

    return JsonResponse({
        "ok": True,
        "quantity": quantity_value,
    })


@require_GET
@login_required
def part_bom_lookup(request):
    part_code = (request.GET.get("part_code") or "").strip()
    if not part_code:
        return JsonResponse({"ok": False, "materials": [], "error": "Missing part_code."}, status=400)

    tep, rows = get_registered_materials_for_partcode(part_code)
    materials = []
    for row in rows:
        materials.append({
            "mat_partcode": row.get("mat_partcode") or "",
            "mat_partname": row.get("mat_partname") or "",
            "mat_maker": row.get("mat_maker") or "",
            "unit": row.get("unit") or "",
            "dim_qty": row.get("dim_qty") or 0,
            "loss_percent": row.get("loss_percent") or 0,
            "total": row.get("total") or row.get("per_unit_total") or 0,
        })

    return JsonResponse({
        "ok": True,
        "part_code": part_code,
        "tep_id": tep.id if tep else None,
        "tep_code": tep.tep_code if tep else "",
        "customer_name": tep.customer.customer_name if tep and tep.customer_id else "",
        "materials": materials,
    })


@require_GET
@login_required
def bom_part_detail_lookup(request):
    part_code = (request.GET.get("part_code") or "").strip()
    if not part_code:
        return JsonResponse({"ok": False, "message": "Missing part_code."}, status=400)

    tep = _preferred_tep_for_part_code(part_code)
    return JsonResponse({
        "ok": True,
        "part_code": part_code,
        "part_name": _get_part_name_for_code(part_code),
        "tep_id": tep.id if tep else None,
        "tep_code": tep.tep_code if tep else "",
        "rows": get_shared_bom_rows_for_partcode(part_code),
    })


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
@user_passes_test(is_admin)
def admin_forecast_csv_upload(request):
    """
    Upload a CSV file containing forecast data.

    Expected columns (case-insensitive, flexible):
      - customer_name / CUSTOMER
      - part_number / Partcode / PART_NUMBER
      - part_name / Partname / PART_NAME
      - Either:
          * date  (e.g. "January-2025" or "Jan-2025"), or
          * month + year columns which will be combined as "Month-Year"
      - unit_price
      - quantity

    Each row represents one monthly forecast entry. Rows with the same
    (customer_name, part_number, part_name) are grouped into a single
    Forecast record whose monthly_forecasts list contains all months
    from the CSV.
    """
    default_next = reverse("app:admin_dashboard") + "?tab=forecast"
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

        def fnum(val, default=0.0):
            try:
                if val is None:
                    return float(default)
                s = str(val).strip()
                if s == "":
                    return float(default)
                s = s.replace(",", "").replace(" ", "")
                if s == "-" or s == "":
                    return float(default)
                return float(s)
            except Exception:
                return float(default)

        # Try to detect the special 2-row banded header format
        csv_file = io.StringIO(content)
        rows = list(csv.reader(csv_file))

        def _is_wide_band_format(all_rows):
            if len(all_rows) < 3:
                return False
            band_line = " ".join([c or "" for c in all_rows[0]]).upper()
            return "ACTUAL DELIVERED" in band_line and "PREVIOUS FORECAST" in band_line

        grouped = defaultdict(list)

        if _is_wide_band_format(rows):
            header_band = rows[0]
            header_cols = rows[1]
            data_rows = rows[2:]

            def _match_col(names):
                """
                Try to find a column whose header matches any of the expected
                logical names. We normalise by lowercasing and stripping
                non‑alphanumeric characters so that variants like
                "PARTNUM", "Part Num", "part_number" etc. can all be matched.
                """
                import re as _re

                normalized_targets = [n.lower() for n in names]

                for idx, col in enumerate(header_cols):
                    raw = (col or "").strip().lower()
                    if not raw:
                        continue
                    norm = _re.sub(r"[^a-z0-9]", "", raw)

                    for target in normalized_targets:
                        if norm == target or norm.startswith(target) or target in norm:
                            return idx

                return None

            # Customer column is optional in this wide format. If it is missing
            # we will derive the customer name from the CSV filename (which
            # usually comes from the Excel sheet name, e.g. "J-Cash.csv").
            idx_customer = _match_col({"customer", "customer_name"})
            idx_part_no = _match_col(
                {
                    "partnumber",
                    "partnum",
                    "partno",
                    "partcode",
                    "partcd",
                }
            )
            idx_part_name = _match_col(
                {
                    "partname",
                    "partnam",
                    "partnm",
                }
            )
            idx_unit_price = _match_col({"unit price", "unit_price", "unitprice"})

            # Part number and part name are required; customer can fall back
            # to the sheet/file name.
            if idx_part_no is None or idx_part_name is None:
                messages.error(request, "CSV header missing Customer / Part number / Part name.")
                return redirect(next_url)

            import re
            band_info = []
            # Excel often merges the band cell (e.g. "ACTUAL DELIVERED (2025)")
            # across many month columns, so in the CSV only the first column of
            # that band has text and the rest are blank. We therefore need to
            # "carry forward" the last seen band group/year for subsequent
            # columns until a new non-empty band cell appears.
            current_group = None
            current_year_from_band = None

            month_map = {
                "JAN": "January",
                "FEB": "February",
                "MAR": "March",
                "APR": "April",
                "MAY": "May",
                "JUN": "June",
                "JUL": "July",
                "AUG": "August",
                "SEP": "September",
                "SEPT": "September",
                "OCT": "October",
                "NOV": "November",
                "DEC": "December",
            }

            for band_raw, col_raw in zip(header_band, header_cols):
                band_label = (band_raw or "").strip().upper()
                col_label = (col_raw or "").strip().upper()

                # Detect / update the current band when there is text.
                if band_label:
                    group = None
                    if "ACTUAL DELIVERED" in band_label:
                        group = "actual"
                    elif "PREVIOUS FORECAST" in band_label:
                        group = "prev"
                    elif "FORECAST" in band_label and "PREVIOUS" not in band_label:
                        group = "fore"

                    if group is not None:
                        current_group = group
                        m = re.search(r"(\d{4})", band_label)
                        current_year_from_band = int(m.group(1)) if m else None

                # If we still don't have a group or month header, this column
                # does not participate in any band mapping.
                if not col_label or not current_group:
                    band_info.append(None)
                    continue

                key3 = col_label[:3]
                month_full = month_map.get(key3)
                if not month_full:
                    band_info.append(None)
                    continue

                band_info.append(
                    {
                        "group": current_group,
                        "month_full": month_full,
                        "year_from_band": current_year_from_band,
                    }
                )

            from datetime import date as _date_cls
            # For year handling we prefer the explicit year parsed from the
            # band label (e.g. "FORECAST (2026)"). If a particular band has no
            # year in its label, we fall back to sensible defaults based on
            # today's year.
            _today_year = _date_cls.today().year

            # Fallback customer name from file/sheet name when no column exists.
            import os as _os_mod
            default_customer_name = _os_mod.path.splitext(f.name)[0] or "Unknown Customer"

            for row_vals in data_rows:
                if len(row_vals) < len(header_cols):
                    row_vals = row_vals + [""] * (len(header_cols) - len(row_vals))

                if idx_customer is not None and idx_customer < len(row_vals):
                    customer_name = (row_vals[idx_customer] or "").strip()
                else:
                    customer_name = default_customer_name
                part_number = (row_vals[idx_part_no] or "").strip()
                part_name = (row_vals[idx_part_name] or "").strip()

                if not (customer_name and part_number and part_name):
                    continue

                unit_price = 0.0
                if idx_unit_price is not None and idx_unit_price < len(row_vals):
                    unit_price = fnum(row_vals[idx_unit_price], 0.0)

                key = (customer_name, part_number, part_name)
                date_map = {}

                for col_idx, info in enumerate(band_info):
                    if not info or col_idx >= len(row_vals):
                        continue

                    qty = fnum(row_vals[col_idx], 0.0)
                    if not qty:
                        continue

                    group = info["group"]
                    month_full = info["month_full"]

                    # Use the year from the band label when available.
                    year = info["year_from_band"]
                    if year is None:
                        # Fallbacks when the band label has no year:
                        # - actual / previous forecast → previous year
                        # - forecast (current)         -> current year
                        if group in ("actual", "prev"):
                            year = _today_year - 1
                        else:  # "fore"
                            year = _today_year

                    date_str = f"{month_full}-{year}"

                    entry = date_map.get(date_str)
                    if not entry:
                        entry = {
                            "date": date_str,
                            "unit_price": unit_price,
                        }
                        date_map[date_str] = entry

                    # Map each band into the fields used by the various tabs.
                    # - "actual" → stored as actual_quantity (used by Actual Delivered tab)
                    # - "prev"   → stored as both prev_quantity and quantity so that
                    #             Previous Forecast & Forecast Summary (which read
                    #             from "quantity") can see the values for the
                    #             previous year.
                    # - "fore"   → stored as quantity (current forecast)
                    if group == "actual":
                        entry["actual_quantity"] = entry.get("actual_quantity", 0.0) + qty
                    elif group == "prev":
                        prev_val = entry.get("prev_quantity", 0.0) + qty
                        entry["prev_quantity"] = prev_val
                        entry["quantity"] = entry.get("quantity", 0.0) + qty
                    elif group == "fore":
                        entry["quantity"] = entry.get("quantity", 0.0) + qty

                if not date_map:
                    continue

                grouped[key].extend(list(date_map.values()))

        else:
            # Fallback: original DictReader-based handling
            csv_file = io.StringIO(content)
            reader = csv.DictReader(csv_file)
            reader.fieldnames = [h.strip().lstrip("\ufeff") for h in (reader.fieldnames or [])]

            def sget(row, *keys, default=""):
                for k in keys:
                    v = row.get(k)
                    if v is not None and str(v).strip() != "":
                        return str(v).strip()
                return default

            for row in reader:
                customer_name = sget(row, "customer_name", "Customer", "CUSTOMER")
                part_number = sget(row, "part_number", "Partcode", "PART_NUMBER", "part_code")
                part_name = sget(row, "part_name", "Partname", "PART_NAME")

                unit_price = fnum(row.get("unit_price") or row.get("UnitPrice") or row.get("price"), 0.0)
                base_quantity = fnum(row.get("quantity") or row.get("qty") or row.get("Quantity"), 0.0)

                if not (customer_name and part_number and part_name):
                    continue

                key = (customer_name, part_number, part_name)

                date_str = sget(row, "date", "month_year", "MonthYear")
                month = sget(row, "month", "Month")
                year = sget(row, "year", "Year")

                if not date_str and (month or year):
                    if month and year:
                        date_str = f"{month}-{year}"
                    elif month:
                        from datetime import date
                        date_str = f"{month}-{date.today().year}"

                if date_str and base_quantity:
                    grouped[key].append(
                        {
                            "date": date_str,
                            "unit_price": unit_price,
                            "quantity": base_quantity,
                        }
                    )

                month_columns = [
                    ("January", ["JAN", "Jan", "January"]),
                    ("February", ["FEB", "Feb", "February"]),
                    ("March", ["MAR", "Mar", "March"]),
                    ("April", ["APR", "Apr", "April"]),
                    ("May", ["MAY", "May"]),
                    ("June", ["JUN", "Jun", "June"]),
                    ("July", ["JUL", "Jul", "July"]),
                    ("August", ["AUG", "Aug", "August"]),
                    ("September", ["SEP", "Sept", "SEPT", "September"]),
                    ("October", ["OCT", "Oct", "October"]),
                    ("November", ["NOV", "Nov", "November"]),
                    ("December", ["DEC", "Dec", "December"]),
                ]

                from datetime import date as _date_cls
                wide_year_raw = sget(row, "forecast_year", "year_forecast", "year", "Year")
                try:
                    wide_year = int(wide_year_raw) if wide_year_raw else _date_cls.today().year
                except ValueError:
                    wide_year = _date_cls.today().year

                for full_name, aliases in month_columns:
                    header = None
                    for alias in aliases:
                        if alias in row:
                            header = alias
                            break
                    if not header:
                        continue

                    qty_val = fnum(row.get(header), 0.0)
                    if not qty_val:
                        continue

                    grouped[key].append(
                        {
                            "date": f"{full_name}-{wide_year}",
                            "unit_price": unit_price,
                            "quantity": qty_val,
                        }
                    )

        if not grouped:
            messages.error(request, "No valid forecast rows found in CSV.")
            return redirect(next_url)

        created_count = 0
        updated_count = 0

        try:
            with transaction.atomic():
                for (cust_name, part_no, part_nm), monthly in grouped.items():
                    customer, _ = Customer.objects.get_or_create(
                        customer_name=cust_name,
                        defaults={"parts": []}
                    )

                    forecast = Forecast.objects.filter(
                        customer=customer,
                        part_number=part_no
                    ).first()

                    if forecast:
                        forecast.part_name = part_nm or forecast.part_name
                        forecast.monthly_forecasts = monthly
                        forecast.save()
                        updated_count += 1
                    else:
                        Forecast.objects.create(
                            customer=customer,
                            part_number=part_no,
                            part_name=part_nm,
                            monthly_forecasts=monthly,
                        )
                        created_count += 1

                    # Ensure the part exists in customer.parts
                    parts = customer.parts or []
                    exists = any(
                        isinstance(p, dict) and str(p.get("Partcode", "")).strip() == part_no
                        for p in parts
                    )
                    if not exists:
                        parts.append({"Partcode": part_no, "Partname": part_nm})
                        customer.parts = parts
                        customer.save(update_fields=["parts"])

            messages.success(
                request,
                f"Forecast CSV uploaded successfully | created={created_count}, updated={updated_count}"
            )
        except Exception as e:
            messages.error(request, f"Forecast CSV upload failed: {e}")

        return redirect(next_url)

    return redirect(next_url)


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

    materials = _build_bom_display_rows(tep)
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

    try:
        with transaction.atomic():
            # Try to use new BOM system first
            bom_obj, created = BOMMaterial.objects.get_or_create(
                part_code=tep.part_code,
                mat_partcode=master.mat_partcode,
                defaults={
                    "source_tep": tep,
                    "material": master,
                    "mat_partname": master.mat_partname,
                    "mat_maker": master.mat_maker,
                    "unit": master.unit,
                    "dim_qty": dim_qty,
                    "loss_percent": loss_percent,
                },
            )

            if not created:
                messages.error(request, f"Material already exists for this Part Code + {mat_partcode}.")
                return redirect(reverse("app:admin_dashboard") + f"?tab=customers&open_panel=1&tep_id={tep_id}")

            # Sync with legacy Material model
            _sync_legacy_material_from_bom(
                tep=tep,
                master=master,
                dim_qty=dim_qty,
                loss_percent=loss_percent,
            )
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

    try:
        with transaction.atomic():
            # Try to use new BOM system first
            bom_obj, created = BOMMaterial.objects.get_or_create(
                part_code=tep.part_code,
                mat_partcode=master.mat_partcode,
                defaults={
                    "source_tep": tep,
                    "material": master,
                    "mat_partname": master.mat_partname,
                    "mat_maker": master.mat_maker,
                    "unit": master.unit,
                    "dim_qty": dim_qty,
                    "loss_percent": loss_percent,
                },
            )

            if not created:
                messages.error(request, f"Material already exists for this Part Code + {mat_partcode}.")
                return redirect("app:customer_detail", tep_id=tep.id)

            # Sync with legacy Material model
            _sync_legacy_material_from_bom(
                tep=tep,
                master=master,
                dim_qty=dim_qty,
                loss_percent=loss_percent,
            )
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


@require_POST
@login_required
@user_passes_test(is_admin)
def reserve_material(request):
    """UI endpoint: reserve stock from the Stocks tab.

    Reuses create_material_allocation(), which creates a MaterialAllocation
    record (status='reserved') after checking availability.
    """
    return create_material_allocation(request)


# ── Timezone endpoint ─────────────────────────────────────────────────────────

from django.views.decorators.csrf import csrf_exempt

@csrf_exempt
@require_POST
def set_timezone(request):
    """
    Called by browser JS with the user's local IANA timezone string.
    Stores it in the session so all views use the correct local time.
    """
    try:
        body = json.loads(request.body)
        tz_name = (body.get("timezone") or "").strip()
    except (json.JSONDecodeError, AttributeError):
        tz_name = ""

    if tz_name:
        try:
            ZoneInfo(tz_name)          # validate it's a real IANA tz
            request.session["user_timezone"] = tz_name
            request.session.modified = True
            return HttpResponse(
                json.dumps({"ok": True, "timezone": tz_name}),
                content_type="application/json"
            )
        except (ZoneInfoNotFoundError, KeyError):
            pass

    return HttpResponse(
        json.dumps({"ok": False, "error": "Invalid timezone"}),
        content_type="application/json",
        status=400,
    )
