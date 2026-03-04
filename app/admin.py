import json
from django import forms
from django.contrib import admin
from django.core.exceptions import ValidationError

from .models import Customer, TEPCode, Material, MaterialList, MaterialStock, MaterialForecast


class TEPCodeInline(admin.TabularInline):
    model = TEPCode
    extra = 0


class MaterialInline(admin.TabularInline):
    model = Material
    extra = 0


class CustomerAdminForm(forms.ModelForm):
    parts_json = forms.CharField(
        required=False,
        label="Customer Parts (JSON)",
        widget=forms.Textarea(attrs={"rows": 14, "style": "font-family: monospace;"}),
        help_text='Example: [{"Partcode":"00000","Partname":"zeroes"}]',
    )

    class Meta:
        model = Customer
        fields = ("customer_name",)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if self.instance and self.instance.pk:
            self.fields["parts_json"].initial = json.dumps(
                self.instance.parts or [],
                indent=2,
                ensure_ascii=False
            )

    def clean_parts_json(self):
        raw = (self.cleaned_data.get("parts_json") or "").strip()

        if raw == "":
            return []

        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            raise ValidationError(f"Invalid JSON: {e}")

        if not isinstance(data, list):
            raise ValidationError("Parts must be a JSON ARRAY (list).")

        for i, item in enumerate(data):
            if not isinstance(item, dict):
                raise ValidationError(f"Item #{i+1} must be an object/dict.")
            if "Partcode" not in item or "Partname" not in item:
                raise ValidationError(f"Item #{i+1} must contain Partcode and Partname.")

            if not str(item["Partcode"]).strip():
                raise ValidationError(f"Item #{i+1}: Partcode cannot be empty.")
            if not str(item["Partname"]).strip():
                raise ValidationError(f"Item #{i+1}: Partname cannot be empty.")

            item["Partcode"] = str(item["Partcode"]).strip()
            item["Partname"] = str(item["Partname"]).strip()

        return data


@admin.register(Customer)
class CustomerAdmin(admin.ModelAdmin):
    form = CustomerAdminForm

    list_display = ("customer_name", "parts_count", "tep_count")
    search_fields = ("customer_name",)

    fields = ("customer_name", "parts_json")

    inlines = [TEPCodeInline]

    def parts_count(self, obj: Customer):
        return len(obj.parts or [])
    parts_count.short_description = "Parts"

    def tep_count(self, obj: Customer):
        return obj.tep_codes.count()
    tep_count.short_description = "TEP Codes"

    def save_model(self, request, obj, form, change):
        super().save_model(request, obj, form, change)
        obj.parts = form.cleaned_data.get("parts_json", [])
        obj.save()


class TEPCodeAdminForm(forms.ModelForm):
    materials_json = forms.CharField(
        required=False,
        label="Materials (JSON)",
        widget=forms.Textarea(attrs={"rows": 18, "style": "font-family: monospace;"}),
        help_text=(
            'Example: [{"mat_partcode":"123","mat_partname":"One2Tree","mat_maker":"Forest",'
            '"unit":"m","dim_qty":120,"loss_percent":10,"total":132}]'
        ),
    )

    class Meta:
        model = TEPCode
        fields = ("customer", "part_code", "tep_code")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if self.instance and self.instance.pk:
            materials = self.instance.materials.all().order_by("id")
            payload = [
                {
                    "mat_partcode": m.mat_partcode,
                    "mat_partname": m.mat_partname,
                    "mat_maker": m.mat_maker,
                    "unit": m.unit,
                    "dim_qty": m.dim_qty,
                    "loss_percent": m.loss_percent,
                    "total": m.total,
                }
                for m in materials
            ]
            self.fields["materials_json"].initial = json.dumps(payload, indent=2, ensure_ascii=False)

    def clean(self):
        cleaned = super().clean()
        customer = cleaned.get("customer")
        part_code = (cleaned.get("part_code") or "").strip()

        if customer and part_code:
            parts = customer.parts or []
            codes = {str(p.get("Partcode", "")).strip() for p in parts if isinstance(p, dict)}
            if part_code not in codes:
                raise ValidationError(
                    {"part_code": f"part_code '{part_code}' not found inside this customer's parts JSON."}
                )

        return cleaned

    def clean_materials_json(self):
        raw = (self.cleaned_data.get("materials_json") or "").strip()
        if raw == "":
            return []

        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            raise ValidationError(f"Invalid JSON: {e}")

        if not isinstance(data, list):
            raise ValidationError("JSON must be an ARRAY (list) of materials.")

        allowed_units = {"pc", "pcs", "m", "g", "kg"}
        required = ["mat_partcode", "mat_partname", "mat_maker", "unit", "dim_qty", "total"]

        for i, item in enumerate(data):
            if not isinstance(item, dict):
                raise ValidationError(f"Item #{i+1} must be an object/dict.")

            missing = [k for k in required if k not in item]
            if missing:
                raise ValidationError(f"Item #{i+1} missing keys: {', '.join(missing)}")

            if item["unit"] not in allowed_units:
                raise ValidationError(f"Item #{i+1}: unit must be one of {sorted(allowed_units)}")

            if "loss_percent" not in item or item["loss_percent"] in (None, ""):
                item["loss_percent"] = 10.0

            try:
                item["dim_qty"] = float(item["dim_qty"])
                item["loss_percent"] = float(item["loss_percent"])
                item["total"] = float(item["total"])
            except (TypeError, ValueError):
                raise ValidationError(f"Item #{i+1}: dim_qty/loss_percent/total must be numeric.")

        return data


@admin.register(TEPCode)
class TEPCodeAdmin(admin.ModelAdmin):
    form = TEPCodeAdminForm

    list_display = ("tep_code", "customer", "part_code", "materials_count")
    search_fields = ("tep_code", "part_code", "customer__customer_name")
    list_filter = ("customer",)

    fields = ("customer", "part_code", "tep_code", "materials_json")
    inlines = [MaterialInline]

    autocomplete_fields = ("customer",)

    def materials_count(self, obj: TEPCode):
        return obj.materials.count()
    materials_count.short_description = "Materials"

    def save_model(self, request, obj, form, change):
        super().save_model(request, obj, form, change)

        materials_data = form.cleaned_data.get("materials_json", [])

        Material.objects.filter(tep_code=obj).delete()

        for item in materials_data:
            Material.objects.create(
                tep_code=obj,
                mat_partcode=item["mat_partcode"],
                mat_partname=item["mat_partname"],
                mat_maker=item["mat_maker"],
                unit=item["unit"],
                dim_qty=item["dim_qty"],
                loss_percent=item.get("loss_percent", 10.0),
                total=item["total"],
            )


@admin.register(Material)
class MaterialAdmin(admin.ModelAdmin):
    list_display = (
        "mat_partname",
        "mat_partcode",
        "mat_maker",
        "unit",
        "dim_qty",
        "loss_percent",
        "total",
        "tep_code",
        "part_code",
        "customer_name",
    )
    search_fields = (
        "mat_partname",
        "mat_partcode",
        "mat_maker",
        "tep_code__tep_code",
        "tep_code__part_code",
        "tep_code__customer__customer_name",
    )
    list_filter = ("unit", "tep_code__customer")
    autocomplete_fields = ("tep_code",)

    def part_code(self, obj: Material):
        return obj.tep_code.part_code
    part_code.short_description = "Part Code"

    def customer_name(self, obj: Material):
        return obj.tep_code.customer.customer_name
    customer_name.short_description = "Customer"


@admin.register(MaterialList)
class MaterialListAdmin(admin.ModelAdmin):
    list_display = ("mat_partcode", "mat_partname", "mat_maker", "unit")
    search_fields = ("mat_partcode", "mat_partname", "mat_maker")  
    list_filter = ("mat_maker",)


@admin.register(MaterialStock)
class MaterialStockAdmin(admin.ModelAdmin):
    list_display = ("material", "on_hand_qty", "last_updated_at", "last_updated_by")
    search_fields = ("material__mat_partcode", "material__mat_partname", "material__mat_maker")
    autocomplete_fields = ("material",)
<<<<<<< HEAD
    list_editable = ("on_hand_qty",)
=======

    # ✅ optional: edit qty directly in list (super convenient)
    list_editable = ("on_hand_qty",)



@admin.register(MaterialForecast)
class MaterialForecastAdmin(admin.ModelAdmin):
    list_display = ('part_code', 'forecast', 'created_at')
    list_filter = ('created_at',)
    search_fields = ('part_code',)
>>>>>>> 1a45f63b6739df9d2db977eecbc84cd2022e9491
