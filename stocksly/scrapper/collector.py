import requests as rq
import pandas as pd
from bs4 import BeautifulSoup
from .models import StockInformation , StocksCategory ,PerMinuteTrade,DayTrade
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

    def render_stock_data(self,stockname,start_date,end_date,format):
        pass