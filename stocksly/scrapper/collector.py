import requests as rq
import pandas as pd
from bs4 import BeautifulSoup
from .models import StockInformation , StocksCategory ,PerMinuteTrade,DayTrade
from datetime import datetime
from stocksly.settings import BASE_DIR
import time 
from tqdm import tqdm
import os

class stocksManager:
    def __init__(self) -> None:
        self.available_stocks = []
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
        }
        self.headers = headers
        self.today_update_flag = 0
        
    def check_stock_availability(self):
        stocks = StockInformation.objects.all()
        stockslist = []
        for st in stocks:
            stockslist.append(st.symbol)
        return {'stocks':stockslist}
    
    def collect_stock_symbols(self):
        targets = [
            'most-active',
            'gainers',
            'losers',
        ]   
    
        limitlist = []

        for page in targets:
            url = f'https://finance.yahoo.com/{page}/?offset=0&count=100'
            r = rq.get(url,headers = self.headers)
            soup = BeautifulSoup(r.text,'html.parser')
            limits = soup.find('div',{'class':'total yf-1tdhqb1'}).text
            limits = limits.split(' ')[2]
            limitlist.append(limits)

        max_hits = []
        for limit in limitlist:
            max_hit = int(int(limit) / 100)
            max_hits.append(max_hit)

        findict = {
            'targets':targets,
            'max_hits':max_hits
        }
        
        urls_for_stocks = []

        i = 0
        for i in range(len(findict['targets'])):
            target = findict['targets'][i]
            maxhit = findict['max_hits'][i]
            for m in range(maxhit+1):
                url = f'https://finance.yahoo.com/markets/stocks/{target}/?start={m*100}&count=100/'
                urls_for_stocks.append(url)

        data = []

        print('collecting data for symbols _______________________________--')
        for u in urls_for_stocks:
            catg = u.split('/')[-3]
            symbol_list = []
            r = rq.get(u,headers = self.headers)
            soup = BeautifulSoup(r.text,'html.parser')
            symbs= soup.find_all('span',{'class':'symbol'})
            for s in symbs:
                symbol_list.append(s.text)
            data.append({catg:symbol_list})
        print("finished collecting data for symbols ______________________________-")
        data = {'names':data}
        return data

    def return_timestamp(unix_timestamps, date_format='%Y-%m-%d %H:%M:%S'):
        formatted_dates = []
        for ut in unix_timestamps:
            date = datetime.fromtimestamp(ut)
            formatted_date = date.strftime(date_format)
            formatted_dates.append(formatted_date)
        return formatted_dates

    def update_prices_for_daily(self,symbol_list):
        current_timestamp = int(time.time())
        date_str = "2015-01-01"
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        period1 = int(time.mktime(date_obj.timetuple()))
        period2 = current_timestamp  
        print(f"checking updates for period1={period1}&period2={period2} for stocks daily _________________")
        for stock in tqdm(symbol_list):
            stock_ = stock[1].replace(' ','')
            json_path = f'{BASE_DIR}/scrapper/data/daily_update/{stock_}.json'
            os.makedirs(os.path.dirname(json_path), exist_ok=True)
            url = f'https://query1.finance.yahoo.com/v8/finance/chart/{stock_}?events=capitalGain%7Cdiv%7Csplit&formatted=true&includeAdjustedClose=true&interval=1d&period1={period1}&period2={period2}&symbol={stock_}&userYfid=true&lang=en-US&region=US'
            r = rq.get(url, headers=self.headers)
            if r.status_code == 200:
                with open(json_path,'wb') as file:
                    file.write(r.content)
            else:
                print("request failed",r.status_code)
        filespaths = f'{BASE_DIR}/scrapper/data/daily_update/'
        jsnlistdaily = os.listdir(filespaths)
        
        print('working on collected daily data _________________________________-')
        
        for jso in tqdm(jsnlistdaily):
            jsonf = pd.read_json(f'{filespaths}/{jso}')
            jsondict = jsonf.to_dict()
            timestamp = jsondict.get('chart').get('result')[0].get('timestamp')
            if timestamp is None:
                pass
        print("daily data update finished _________________________________")