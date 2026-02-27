from django.db import models
from django.conf import settings
from django.core.exceptions import ValidationError
from django.contrib.auth.models import User


class Customer(models.Model):
    customer_name = models.CharField(max_length=120, unique=True)

    parts = models.JSONField(default=list, blank=True)

    def __str__(self):
        return self.customer_name

    def clean(self):
        """
        Optional validation to keep parts JSON clean.
        """
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


class TEPCode(models.Model):
    customer = models.ForeignKey(
        Customer,
        on_delete=models.CASCADE,
        related_name="tep_codes",
    )

    part_code = models.CharField(max_length=60)

    tep_code = models.CharField(max_length=60, unique=True)

    def __str__(self):
        return f"{self.customer.customer_name} | {self.part_code} | {self.tep_code}"


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
    total = models.FloatField()

    def __str__(self):
        return f"{self.mat_partname} ({self.mat_partcode})"


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

    qty_allocated = models.PositiveIntegerField(
        help_text="Quantity reserved from stock"
    )

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

    def __str__(self):
        return f"ForecastRun #{self.id} @ {self.created_at:%Y-%m-%d %H:%M}"


class ForecastLine(models.Model):
    """
    Stores the computed output rows of a forecast run.
    Each row corresponds to one material requirement line under a part_code forecast.
    """
    run = models.ForeignKey(ForecastRun, on_delete=models.CASCADE, related_name="lines")

    # Inputs
    part_code = models.CharField(max_length=120)
    forecast_qty = models.PositiveIntegerField(default=0)

    # Traceability (what BOM we used)
    customer_name = models.CharField(max_length=120, blank=True, default="")
    tep_code = models.CharField(max_length=60, blank=True, default="")

    # Material requirement output
    mat_partcode = models.CharField(max_length=80)
    mat_partname = models.CharField(max_length=160, blank=True, default="")
    mat_maker = models.CharField(max_length=120, blank=True, default="")
    unit = models.CharField(max_length=10, blank=True, default="")

    per_unit_total = models.DecimalField(max_digits=18, decimal_places=4, default=0)
    required_qty = models.DecimalField(max_digits=18, decimal_places=4, default=0)

    class Meta:
        indexes = [
            models.Index(fields=["part_code"]),
            models.Index(fields=["mat_partcode"]),
        ]

    def __str__(self):
        return f"{self.part_code} -> {self.mat_partcode} req={self.required_qty}"