from django.urls import path, include
from . import views
from .api import api
from django.contrib import admin

#app_name = "app"

urlpatterns = [
    #path('home/', views.home, name='home')
    path("", views.customer_list, name="customer_list"), 
    path("customers/tep/<int:tep_id>/", views.customer_detail, name="customer_detail"),
    path("employees/create/", views.create_employee, name="create_employee"),
    path("api/", api.urls),
    path("admin/", admin.site.urls),
    path("login/", views.login_view, name="login"),
    path("logout/", views.logout_view, name="logout"),

    #bagong addm
    path("panel/dashboard/", views.admin_dashboard, name="admin_dashboard"),
    path("panel/users/", views.admin_users, name="admin_users"),
    path("panel/csv-upload/", views.admin_csv_upload, name="admin_csv_upload"),
    path("panel/users/<int:user_id>/toggle/", views.toggle_user_active, name="toggle_user_active"),
    #path("panel/customers/<int:tep_id>/panel/", views.admin_customer_detail_partial, name="admin_customer_detail_panel"),

    path("tep/materials/add/", views.add_material_to_tep, name="add_material_to_tep"),
    
    path("customers/create/", views.customer_create, name="customer_create"),
    path("material-stock/update/", views.update_material_stock, name="update_material_stock"),
    path("register-allocation/", views.register_allocation, name="register_allocation"),
    path("forecast/schedule/save/", views.register_customer_part_schedule, name="register_customer_part_schedule"),


    #test for the forecast
    
    path("material-forecast/", views.material_forecast_view, name="material_forecast"),

    path("material-allocation/create/", views.create_material_allocation, name="create_material_allocation"),
    
]
