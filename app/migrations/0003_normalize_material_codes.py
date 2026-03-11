from django.db import migrations
import re

def fix_material_codes(apps, schema_editor):
    MaterialList = apps.get_model("app", "MaterialList")

    for m in MaterialList.objects.all():
        code = m.mat_partcode or ""

        # normalize all dash-like characters
        fixed = re.sub(r"[‐-‒–—−]", "-", code)

        # remove double spaces
        fixed = " ".join(fixed.split())

        if fixed != code:
            print("Fixing:", repr(code), "->", repr(fixed))
            m.mat_partcode = fixed
            m.save(update_fields=["mat_partcode"])


class Migration(migrations.Migration):

    dependencies = [
        ("app", "0002_fix_material_codes"),
    ]

    operations = [
        migrations.RunPython(fix_material_codes),
    ]