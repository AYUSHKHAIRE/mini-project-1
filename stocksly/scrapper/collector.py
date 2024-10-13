import requests as rq
import pandas as pd
from bs4 import BeautifulSoup
from .models import StockInformation , StocksCategory ,PerMinuteTrade,DayTrade
from datetime import datetime
from stocksly.settings import BASE_DIR
import time 
from tqdm import tqdm
import os
from datetime import datetime,timedelta
import json

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

    def return_timestamp(self,timestamps):
        new_dates = []
        for utnix in timestamps:
            try:
                if isinstance(utnix, str):
                    datetime.strptime(utnix, '%Y-%m-%d %H:%M:%S')
                    new_dates.append(utnix)
                else:
                    utnix = float(utnix)
                    date = datetime.fromtimestamp(utnix).strftime('%Y-%m-%d %H:%M:%S')
                    new_dates.append(date)
            except (ValueError, TypeError):
                new_dates.append(None)  
        return new_dates

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
                # print("request failed",r.status_code)
                continue
        filespaths = f'{BASE_DIR}/scrapper/data/daily_update/'
        jsnlistdaily = os.listdir(filespaths)
        
        print('working on collected daily data _________________________________-')
        
        for jso in tqdm(jsnlistdaily):
            jsonf = pd.read_json(f'{filespaths}/{jso}')
            jsondict = jsonf.to_dict()
            timestamp = jsondict.get('chart').get('result')[0].get('timestamp')
            if timestamp is not None:
                new_timestamps = self.return_timestamp(timestamp)
                jsondict['chart']['result'][0]['timestamp'] = new_timestamps
            with open(f'{filespaths}/{jso}', 'w') as json_file:
                json.dump(jsondict, json_file, indent=4)

        print("daily data update finished _________________________________")
        
    def render_daaily_data(self, stocksymbol, startdate, enddate):
        path = f'{BASE_DIR}/scrapper/data/daily_update/'
        data = pd.read_json(f'{path}/{stocksymbol}.json').to_dict()
        timestmp = data.get('chart').get('result')[0].get('timestamp', [])
        dates =   [ str(t.split(' ')[0]) for t in timestmp ]
        new_data = data.get('chart').get('result')[0].get('indicators').get('quote')[0]

        startindex = None
        endindex = None
        final_message = ''

        if startdate is None or enddate is None:
            message = "Dates were not provided.\n"
            thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            startdate = thirty_days_ago
            enddate = dates[-1]
            final_message = final_message+message

        try:
            if startdate in dates:
                startindex = dates.index(startdate)
            else:
                message = "Start date not found. Locating later date in dates.   "
                final_message = final_message+message
                for i in range(3):
                    next_date = (datetime.strptime(startdate, '%Y-%m-%d') + timedelta(days=i + 1)).strftime('%Y-%m-%d')
                    print(next_date)
                    if next_date in dates:
                        message = f"Located new start date: {next_date}   "
                        final_message = final_message+message
                        startdate = next_date
                        startindex = dates.index(startdate)
                        break
                if startindex is None:
                    raise ValueError("Start date not found within next 3 days range.")

            if enddate in dates:
                endindex = dates.index(enddate)
            else:
                message = "End date not found. Locating later date in dates.   "
                final_message = final_message+message
                for i in range(3):
                    next_date = (datetime.strptime(enddate, '%Y-%m-%d') + timedelta(days=i + 1)).strftime('%Y-%m-%d')
                    if next_date in dates:
                        message = f"Located new end date: {next_date}   "
                        final_message = final_message+message
                        enddate = next_date
                        endindex = dates.index(enddate)
                        break
                if endindex is None:
                    raise ValueError("End date not found within behind 3 days range.")

            data = {
                'dates': dates[startindex:endindex + 1],
                'close': new_data['close'][startindex:endindex + 1],
                'open': new_data['open'][startindex:endindex + 1],
                'high': new_data['high'][startindex:endindex + 1],
                'low': new_data['low'][startindex:endindex + 1],
                'volume': new_data['volume'][startindex:endindex + 1],
            }
            response = {
                'response':final_message,
                'data':data
            }
            return response

        except ValueError as e:
            print(f"Error: {e}")
            return None
