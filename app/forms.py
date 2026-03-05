from django import forms
from django.contrib.auth.models import User
from .models import EmployeeProfile


class EmployeeCreateForm(forms.ModelForm):
    password = forms.CharField(widget=forms.PasswordInput)
    confirm_password = forms.CharField(widget=forms.PasswordInput)

    class Meta:
        model = EmployeeProfile
        fields = ["employee_id", "full_name", "department"]

    def clean_employee_id(self):
        emp_id = self.cleaned_data["employee_id"].strip()

        if EmployeeProfile.objects.filter(employee_id=emp_id).exists():
            raise forms.ValidationError("Employee ID already exists.")

        if User.objects.filter(username=emp_id).exists():
            raise forms.ValidationError("Employee ID already exists in auth system.")

        return emp_id

    def clean(self):
        cleaned = super().clean()
        pw = cleaned.get("password")
        cpw = cleaned.get("confirm_password")

        if pw and cpw and pw != cpw:
            self.add_error("confirm_password", "Passwords do not match.")

        return cleaned

    def save(self, commit=True):
        employee_id = self.cleaned_data["employee_id"].strip()
        password = self.cleaned_data["password"]

        user = User.objects.create_user(
            username=employee_id,
            password=password
        )

        user.is_staff = True
        user.is_active = True
        if commit:
            user.save()

        profile = super().save(commit=False)
        profile.user = user

        if commit:
            profile.save()

        return profile