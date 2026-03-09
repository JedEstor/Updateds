from django import template
from django.contrib.humanize.templatetags.humanize import intcomma as django_intcomma

register = template.Library()

@register.filter
def get_item(dictionary, key):
    """Get an item from a dictionary using the key"""
    if dictionary is None:
        return None
    return dictionary.get(key)

@register.filter
def intcomma(value):
    """Format a number with commas"""
    if value is None or value == "":
        return ""
    try:
        # Convert to int first to remove decimal places
        return django_intcomma(int(float(value)))
    except (ValueError, TypeError):
        return value

@register.filter
def split(value, delimiter=','):
    """Split a string by delimiter and return a list"""
    if not value:
        return []
    return value.split(delimiter)