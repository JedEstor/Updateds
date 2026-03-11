from django.db import migrations

def fix_material_codes(apps, schema_editor):
    MaterialList = apps.get_model("app", "MaterialList")

    for m in MaterialList.objects.all():
        code = m.mat_partcode or ""
        fixed = code.replace("\\u002D", "-")
        fixed = " ".join(fixed.split())

        if fixed != code:
            m.mat_partcode = fixed
            m.save()

class Migration(migrations.Migration):

    dependencies = [
        ("app", "0001_initial"),  # ← your last migration here
    ]

    operations = [
        migrations.RunPython(fix_material_codes),
    ]