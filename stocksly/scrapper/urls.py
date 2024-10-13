from django.urls import path,include
from . import views

urlpatterns = [
    path('get_available_stocks/', views.get_available_stocks),
    path('get_stock_daily_data/<str:stocksymbol>/', views.get_stocks_daily_data),
]