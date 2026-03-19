from django.db import models
from django.conf import settings
from django.core.exceptions import ValidationError
from django.contrib.auth.models import User
from django.db.models import Q


class Customer(models.Model):
    customer_name = models.CharField(max_length=120, unique=True)
    parts = models.JSONField(default=list, blank=True)

    def __str__(self):
        return self.customer_name

    def clean(self):
        if self.parts in (None, ""):
            self.parts = []

        if not isinstance(self.parts, list):
            raise ValidationError({"parts": "parts must be a LIST of objects."})

        for i, item in enumerate(self.parts):
            if not isinstance(item, dict):
                raise ValidationError({"parts": f"parts[{i}] must be an object/dict."})

            if "Partcode" not in item or "Partname" not in item:
                raise ValidationError({"parts": f"parts[{i}] must contain Partcode and Partname."})

            if not str(item["Partcode"]).strip():
                raise ValidationError({"parts": f"parts[{i}].Partcode cannot be empty."})

            if not str(item["Partname"]).strip():
                raise ValidationError({"parts": f"parts[{i}].Partname cannot be empty."})


class PartMaster(models.Model):
    part_code = models.CharField(max_length=60, unique=True)
    part_name = models.CharField(max_length=160)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["part_code"]
        indexes = [
            models.Index(fields=["part_code"]),
            models.Index(fields=["part_name"]),
            models.Index(fields=["is_active"]),
        ]

    def __str__(self):
        return f"{self.part_code} - {self.part_name}"


class TEPCode(models.Model):
    customer = models.ForeignKey(
        Customer,
        on_delete=models.CASCADE,
        related_name="tep_codes",
    )

    part_code = models.CharField(max_length=60)
    tep_code = models.CharField(max_length=60, unique=True)

    is_active = models.BooleanField(default=True)

    superseded_by = models.ForeignKey(
        "self",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="supersedes",
    )

    revised_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True, null=True, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=["customer", "part_code", "tep_code"]),
            models.Index(fields=["part_code"]),
            models.Index(fields=["is_active"]),
        ]

    def __str__(self):
        status = "ACTIVE" if self.is_active else "OBSOLETE"
        return f"{self.customer.customer_name} | {self.part_code} | {self.tep_code} ({status})"


class Material(models.Model):
    UNIT_CHOICES = [
        ("pc", "pc"),
        ("pcs", "pcs"),
        ("m", "m"),
        ("g", "g"),
        ("kg", "kg"),
    ]

    tep_code = models.ForeignKey(
        TEPCode,
        on_delete=models.CASCADE,
        related_name="materials",
    )

    mat_partcode = models.CharField(max_length=80)
    mat_partname = models.CharField(max_length=160)
    mat_maker = models.CharField(max_length=120)

    unit = models.CharField(max_length=10, choices=UNIT_CHOICES)
    dim_qty = models.FloatField()
    loss_percent = models.FloatField(default=10.0)
    total = models.FloatField(default=0)

    created_at = models.DateTimeField(auto_now_add=True, null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=["tep_code", "mat_partcode"]),
            models.Index(fields=["mat_partcode"]),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["tep_code", "mat_partcode"],
                name="uniq_material_per_tep_partcode",
            )
        ]

    def save(self, *args, **kwargs):
        base = float(self.dim_qty or 0)
        loss = float(self.loss_percent or 0)
        self.total = base + (base * loss / 100.0)
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.mat_partname} ({self.mat_partcode})"


class BOMMaterial(models.Model):
    """
    Shared BOM / recipe per part code.
    One part_code (e.g. LT001) can be used by many customers,
    so the recipe must be stored by part_code, not per-customer TEP only.
    """

    UNIT_CHOICES = [
        ("pc", "pc"),
        ("pcs", "pcs"),
        ("m", "m"),
        ("g", "g"),
        ("kg", "kg"),
    ]

    part_code = models.CharField(max_length=60, db_index=True)
    source_tep = models.ForeignKey(
        TEPCode,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="shared_bom_materials",
    )
    material = models.ForeignKey(
        "MaterialList",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="bom_rows",
    )

    mat_partcode = models.CharField(max_length=80)
    mat_partname = models.CharField(max_length=160, blank=True, default="")
    mat_maker = models.CharField(max_length=120, blank=True, default="")
    unit = models.CharField(max_length=10, choices=UNIT_CHOICES, blank=True, default="pc")

    dim_qty = models.DecimalField(max_digits=18, decimal_places=5, default=0)
    loss_percent = models.DecimalField(max_digits=7, decimal_places=2, default=10.00)
    total = models.DecimalField(max_digits=18, decimal_places=5, default=0)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["part_code", "mat_partcode", "id"]
        indexes = [
            models.Index(fields=["part_code", "mat_partcode"]),
            models.Index(fields=["mat_partcode"]),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["part_code", "mat_partcode"],
                name="uniq_bom_material_per_partcode",
            )
        ]

    def save(self, *args, **kwargs):
        base = float(self.dim_qty or 0)
        loss = float(self.loss_percent or 0)
        self.total = base + (base * loss / 100.0)

        if self.source_tep_id and not self.part_code:
            self.part_code = self.source_tep.part_code

        if self.material_id:
            if not self.mat_partcode:
                self.mat_partcode = self.material.mat_partcode
            if not self.mat_partname:
                self.mat_partname = self.material.mat_partname
            if not self.mat_maker:
                self.mat_maker = self.material.mat_maker
            if not self.unit:
                self.unit = self.material.unit

        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.part_code} -> {self.mat_partcode}"


class MaterialList(models.Model):
    UNIT_CHOICES = [
        ("pc", "pc"),
        ("pcs", "pcs"),
        ("m", "m"),
        ("g", "g"),
        ("kg", "kg"),
    ]

    mat_partcode = models.CharField(max_length=80, unique=True)
    mat_partname = models.CharField(max_length=160)
    mat_maker = models.CharField(max_length=120)
    unit = models.CharField(max_length=10, choices=UNIT_CHOICES)

    def __str__(self):
        return f"{self.mat_partname} ({self.mat_partcode})"


class CustomerCSV(models.Model):
    csv_file = models.FileField(upload_to="customer_csvs/")
    uploaded_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"CustomerCSV {self.id}"


class EmployeeProfile(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name="employeeprofile")
    employee_id = models.CharField(max_length=30, unique=True)
    full_name = models.CharField(max_length=150)
    department = models.CharField(max_length=100)

    def __str__(self):
        return f"{self.employee_id} - {self.full_name}"


class MaterialStock(models.Model):
    material = models.OneToOneField(
        MaterialList,
        on_delete=models.CASCADE,
        related_name="stock"
    )
    on_hand_qty = models.PositiveIntegerField(
        default=0,
        help_text="Physical stock counted during monthly inventory"
    )
    last_updated_at = models.DateTimeField(auto_now=True)
    last_updated_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True
    )

    class Meta:
        verbose_name = "Material Stock"
        verbose_name_plural = "Material Stocks"

    def __str__(self):
        return f"{self.material} - On hand: {self.on_hand_qty}"


class MaterialAllocation(models.Model):
    """
    Represents RESERVED / ALLOCATED stock.
    Does NOT change physical stock directly.
    """

    STATUS_CHOICES = [
        ("reserved", "Reserved"),
        ("fulfilled", "Fulfilled"),
        ("released", "Released"),
    ]

    material = models.ForeignKey(
        MaterialList,
        on_delete=models.CASCADE,
        related_name="allocations"
    )

    customer = models.ForeignKey(
        Customer,
        on_delete=models.CASCADE,
        related_name="material_allocations"
    )

    tep_code = models.ForeignKey(
        TEPCode,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="material_allocations"
    )

    qty_allocated = models.PositiveIntegerField(help_text="Quantity reserved from stock")

    forecast_ref = models.CharField(
        max_length=50,
        blank=True,
        help_text="Optional forecast reference (month/week/code)"
    )

    status = models.CharField(
        max_length=20,
        choices=STATUS_CHOICES,
        default="reserved"
    )

    created_at = models.DateTimeField(auto_now_add=True)
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True
    )

    class Meta:
        ordering = ["created_at"]
        verbose_name = "Material Allocation"
        verbose_name_plural = "Material Allocations"
        indexes = [
            models.Index(fields=["material", "status"]),
            models.Index(fields=["forecast_ref"]),
            models.Index(fields=["customer", "status"]),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["forecast_ref", "material", "customer", "tep_code"],
                condition=Q(status="reserved") & ~Q(forecast_ref=""),
                name="uniq_reserved_per_ref_material_customer_tep",
            )
        ]

    def __str__(self):
        return f"{self.material.mat_partcode} | {self.qty_allocated} | {self.status}"


class ForecastRun(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True
    )
    note = models.CharField(max_length=255, blank=True, default="")
    schedule_month = models.CharField(max_length=7, blank=True, default="")

    def __str__(self):
        return f"ForecastRun #{self.id} @ {self.created_at:%Y-%m-%d %H:%M}"


class ForecastLine(models.Model):
    """
    Stores the computed output rows of a forecast run.
    Each row corresponds to one material requirement line under a part_code forecast.
    """
    run = models.ForeignKey(ForecastRun, on_delete=models.CASCADE, related_name="lines")

    part_code = models.CharField(max_length=120)
    forecast_qty = models.PositiveIntegerField(default=0)

    customer_name = models.CharField(max_length=120, blank=True, default="")
    tep_code = models.CharField(max_length=60, blank=True, default="")

    mat_partcode = models.CharField(max_length=80)
    mat_partname = models.CharField(max_length=160, blank=True, default="")
    mat_maker = models.CharField(max_length=120, blank=True, default="")
    unit = models.CharField(max_length=10, blank=True, default="")

    per_unit_total = models.DecimalField(max_digits=18, decimal_places=5, default=0)
    required_qty = models.DecimalField(max_digits=18, decimal_places=5, default=0)

    class Meta:
        indexes = [
            models.Index(fields=["part_code"]),
            models.Index(fields=["mat_partcode"]),
            models.Index(fields=["run"]),
        ]

    def __str__(self):
        return f"{self.part_code} -> {self.mat_partcode} req={self.required_qty}"


class Forecast(models.Model):
    """
    Forecast for a part: part_number, part_name, and monthly forecasts.
    Optionally linked to a Customer.
    """
    customer = models.ForeignKey(
        Customer,
        on_delete=models.CASCADE,
        related_name="forecasts",
        null=True,
        blank=True,
    )
    part_number = models.CharField(max_length=80)
    part_name = models.CharField(max_length=200)
    monthly_forecasts = models.JSONField(
        default=list,
        blank=True,
        help_text="List of {date, unit_price, quantity} per month, e.g. [{'date': 'Jan-2026', 'unit_price': 0.13, 'quantity': 1000}]",
    )

    class Meta:
        ordering = ["part_number"]

    def __str__(self):
        return f"{self.part_number} - {self.part_name}"

    @property
    def monthly_count(self):
        return len(self.monthly_forecasts or [])

    @property
    def months_display(self):
        months = [
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December"
        ]
        abbr = ["jan", "feb", "mar", "apr", "may", "jun",
                "jul", "aug", "sep", "oct", "nov", "dec"]
        items = self.monthly_forecasts or []
        names = []
        seen = set()
        for m in items:
            if isinstance(m, dict):
                d = str(m.get("date", "")).strip()
                if not d:
                    continue
                s_lower = d.lower()
                found = None
                for i, a in enumerate(abbr):
                    if s_lower.startswith(a) or s_lower == a:
                        found = months[i]
                        break
                if found is None:
                    try:
                        n = int(d.split("-")[0] if "-" in d else d.split("/")[0] if "/" in d else d)
                        found = months[n - 1] if 1 <= n <= 12 else d
                    except (ValueError, IndexError):
                        found = d
                if found and found not in seen:
                    seen.add(found)
                    names.append(found)
        return ", ".join(names) if names else "—"

    @property
    def base_unit_price(self) -> float:
        for m in (self.monthly_forecasts or []):
            if isinstance(m, dict):
                try:
                    return float(m.get("unit_price", 0) or 0)
                except (TypeError, ValueError):
                    continue
        return 0.0

    @property
    def latest_quantity(self) -> float:
        items = [m for m in (self.monthly_forecasts or []) if isinstance(m, dict)]
        if not items:
            return 0.0
        try:
            return float(items[-1].get("quantity", 0) or 0)
        except (TypeError, ValueError):
            return 0.0

    @property
    def total_quantity(self) -> float:
        total = 0.0
        for m in (self.monthly_forecasts or []):
            if isinstance(m, dict):
                try:
                    total += float(m.get("quantity", 0) or 0)
                except (TypeError, ValueError):
                    continue
        return total

    
    @property
    def total_amount(self) -> float:
        total = 0.0
        for m in (self.monthly_forecasts or []):
            if isinstance(m, dict):
                try:
                    price = float(m.get("unit_price", 0) or 0)
                    qty = float(m.get("quantity", 0) or 0)
                    total += price * qty
                except (TypeError, ValueError):
                    continue
        return total
