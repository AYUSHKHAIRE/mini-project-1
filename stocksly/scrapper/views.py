from django.shortcuts import render
from django.http import JsonResponse
from .collector import stocksManager
from .models import setup_stocks_model
from datetime import datetime,timedelta
from scrapper.logger_config import logger
STM = stocksManager()
   
'''
input : nothing

algorithm :
this funcion runs a setup update for a time .
it takes stock symbols for today and set up their models .
it ensures the process of setup should held only once a day .

output : nothing
'''
def update_data_for_today():
    if STM.today_update_flag == 0:
        logger.info("starting update for today ___________________________________-")
        symbols = STM.collect_stock_symbols()
        stocks_list_for_setup = []
        new_list = symbols['names']
        catagories = []
        for st in new_list:
            for key,value in st.items():
                for v in value:
                    stocks_list_for_setup.append(
                        [key,v]
                    )
                if key not in catagories:
                    catagories.append(key)
        setup_stocks_model(stocks_list_for_setup)
        
        STM.update_prices_for_daily(stocks_list_for_setup[:10])
        STM.update_prices_for_per_minute(stocks_list_for_setup[:10])
    
        STM.today_update_flag = 1
        logger.info("finishing update for today ___________________________________-")

'''
input : request

algorithm:
it get the available stocks data and render it .
it checks stock availablbity . if it is not updated , it updates it . 

output:
a json response containing stock data .
'''

def get_available_stocks(request):
    available_stocks= STM.check_stock_availability()
    if STM.today_update_flag == 0:
        update_data_for_today()
    return JsonResponse(
        available_stocks,
        safe=False
    )

'''
input:
request : fired by user 
stocksymbol : symbol for the stock .

algorithm:
it takes startdate , enddate and srock symbol
collect data and render its json .
'''
def get_stocks_daily_data(
        request, 
        stocksymbol
    ):
    startdate = request.GET.get('start', None)  
    enddate = request.GET.get('end', None)  
    data = STM.render_daily_data(
        stocksymbol, 
        startdate, 
        enddate
    )
    return JsonResponse(
        data, 
        safe=False
    )
    
def get_stocks_per_minute_data(
    request, 
    stocksymbol   
):
    starttime = request.GET.get('start', None)  
    endtime = request.GET.get('end', None) 
    starttime = starttime.replace('%',' ')
    endtime = endtime.replace('%',' ')
    data = STM.render_per_minute_data(
        stocksymbol, 
        starttime, 
        endtime
    )
    return JsonResponse(
        data, 
        safe=False
    )