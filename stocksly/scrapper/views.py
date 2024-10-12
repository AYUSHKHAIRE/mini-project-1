from django.shortcuts import render
from django.http import JsonResponse
from .collector import stocksManager
from .models import setup_stocks_model
import json

STM = stocksManager()

def update_data_for_today():
    if STM.today_update_flag == 0:
        print("starting update for today ___________________________________-")
        symbols = STM.collect_stock_symbols()
        stocks_list_for_setup = []
        new_list = symbols['names']
        catagories = []
        for st in new_list:
            for key,value in st.items():
                for v in value:
                    stocks_list_for_setup.append([key,v])
                if key not in catagories:
                    catagories.append(key)
        setup_stocks_model(stocks_list_for_setup)
        STM.today_update_flag = 1
        print("finishing update for today ___________________________________-")

def get_available_stocks(request):
    available_stocks= STM.check_stock_availability()
    if STM.today_update_flag == 0:
        update_data_for_today()
    return JsonResponse(available_stocks,safe=False)