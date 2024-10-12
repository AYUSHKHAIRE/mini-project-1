from django.urls import path,include
from . import views

urlpatterns = [
    path('get_available_stocks/', views.get_available_stocks),
]