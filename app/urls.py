from django.urls import path, include
from . import views
from .api import api
from django.contrib import admin

app_name = "app"

urlpatterns = [
    #path('home/', views.home, name='home')
    path("", views.customer_list, name="customer_list"), 
    path("customers/tep/<int:tep_id>/", views.customer_detail, name="customer_detail"),
    path("employees/create/", views.create_employee, name="create_employee"),
    path("api/", api.urls),
    path("admin/", admin.site.urls),
    path("login/", views.login_view, name="login"),
    path("logout/", views.logout_view, name="logout"),

    # Admin panel
    path("panel/dashboard/", views.admin_dashboard, name="admin_dashboard"),
    path("panel/users/", views.admin_users, name="admin_users"),
    path("panel/csv-upload/", views.admin_csv_upload, name="admin_csv_upload"),
    path("panel/forecast-csv-upload/", views.admin_forecast_csv_upload, name="admin_forecast_csv_upload"),
    path("panel/users/<int:user_id>/toggle/", views.toggle_user_active, name="toggle_user_active"),
    #path("panel/customers/<int:tep_id>/panel/", views.admin_customer_detail_partial, name="admin_customer_detail_panel"),

    # TEP/Material management
    path("tep/materials/add/", views.add_material_to_tep, name="add_material_to_tep"),
    path("customers/tep/<int:tep_id>/add-material/", views.add_material_to_tep_staff, name="add_material_to_tep_staff"),
    
    # Customer management
    path("customers/create/", views.customer_create, name="customer_create"),
    
    # Stock/Allocation management
    path("material-stock/update/", views.update_material_stock, name="update_material_stock"),
    path("material-allocation/create/", views.create_material_allocation, name="create_material_allocation"),
    path("panel/stocks/reserve/", views.reserve_material, name="reserve_material"),
    
    # Material master management (staff)
    path("materials/", views.staff_materials, name="staff_materials"),
    path("materials/add/", views.staff_material_add, name="staff_material_add"),
    path("materials/update/", views.staff_material_update, name="staff_material_update"),
    path("materials/delete/", views.staff_material_delete, name="staff_material_delete"),
    path("materials/upload-csv/", views.staff_materials_csv_upload, name="staff_materials_csv_upload"),
    
    # API endpoints
    path("api/material-lookup/", views.material_lookup, name="material_lookup"),
    path("api/forecast-qty/", views.forecast_qty_lookup, name="forecast_qty_lookup"),
    path("api/part-bom/", views.part_bom_lookup, name="part_bom_lookup"),
    path("api/bom-part-detail/", views.bom_part_detail_lookup, name="bom_part_detail_lookup"),
    path("api/part-master-lookup/", views.part_master_lookup, name="part_master_lookup"),
    
    # Timezone detection
    path("set-timezone/", views.set_timezone, name="set_timezone"),
]