# app/service_materiallist.py
from .models import MaterialList

# Static forecast mapping for testing (optional)
PART_FORECAST_VALUES = {
    "LT3436-001 REV.D": 5000,
    "LT3435-001 REV.C": 10000,
}

def compute_material_total(quantity, unit_price):
    return quantity * unit_price

def forecast_materials_manual(part_code):
    """
    Pull materials for a part code, let user input unit price, compute totals.
    """
    try:
        materials = MaterialList.objects.filter(mat_partcode=part_code)
    except MaterialList.DoesNotExist:
        print(f"No materials found for part code {part_code}")
        return []

    results = []

    print(f"\nMaterials for Part Code: {part_code}\n")

    for mat in materials:
        print(f"Material: {mat.mat_partname} | Unit: {mat.unit}")

        # Ask user to input unit price
        while True:
            try:
                unit_price = float(input(f"Enter unit price for {mat.mat_partname}: "))
                break
            except ValueError:
                print("Invalid input. Please enter a number.")

        # For simplicity, ask user total quantity as well (you could pull this from somewhere else)
        while True:
            try:
                quantity = float(input(f"Enter total quantity for {mat.mat_partname}: "))
                break
            except ValueError:
                print("Invalid input. Please enter a number.")

        total = compute_material_total(quantity, unit_price)
        print(f"-> Material Total for {mat.mat_partname}: {total}\n")

        results.append({
            "material_name": mat.mat_partname,
            "unit": mat.unit,
            "quantity": quantity,
            "unit_price": unit_price,
            "total": total
        })

    # Optional: apply forecast multiplier
    base_forecast = PART_FORECAST_VALUES.get(part_code, 1)
    print(f"\nApplying forecast multiplier: {base_forecast}\n")
    for r in results:
        forecasted_value = r["total"] * base_forecast
        print(f"{r['material_name']} | Forecasted Value: {forecasted_value}")

    return results

# Example test
if __name__ == "__main__":
    part_code_input = input("Enter Part Code to compute materials for: ")
    forecast_materials_manual(part_code_input)