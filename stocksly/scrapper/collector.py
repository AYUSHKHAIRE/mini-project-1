import requests as rq
import pandas as pd
from bs4 import BeautifulSoup
from scrapper.models import StockInformation , StocksCategory ,PerMinuteTrade,DayTrade
from datetime import datetime
from stocksly.settings import BASE_DIR
import time 
from tqdm import tqdm
import os
from datetime import datetime,timedelta
import json
from scrapper.logger_config import logger
import shutil

'''
A class handles all stocks related operations .
'''

class stocksManager:
    '''
    input:
    holds values for 
    stocks : all stocks data to render fast .
    headers : to bypass request check .
    todays_update_flag : if it was updated today or not .
    
    algorithm:none
    
    output:none
    '''
    def __init__(self) -> None:
        self.available_stocks = []
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
        }
        self.headers = headers
        self.today_update_flag = 0
        
    '''
    input : none
    
    algorithm:
    collects all stocks that are in models .
    renders the json response .
    
    output:
    a json response of stocks data .
    '''
    def check_stock_availability(self):
        stocks = StockInformation.objects.all()
        stockslist = []
        for st in stocks:
            stockslist.append(st.symbol)
        return {'stocks':stockslist}
    
    '''
    input : none
    
    algorithm:
    it have basically three targets on yahoo finance website . 
    ie most active , gainers and loosers .
    it detects the pages to hit , max hits , and prepare urls
    to hit in order to getting symbols .
    
    output : data collected - stocknames .
    '''
    def collect_stock_symbols(self):
        targets = [
            'most-active',
            'gainers',
            'losers',
        ]   
    
        limitlist = []

        for page in tqdm(targets):
            url = f'https://finance.yahoo.com/{page}/?offset=0&count=100'
            try:
                r = rq.get(url,headers = self.headers)
            except Exception as e:
                logger.warning("cannot hit url : ",url ,e,r.status_code)
            soup = BeautifulSoup(r.text,'html.parser')
            limits = soup.find(
                'div',{'class':'total yf-1tdhqb1'}
            ).text
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
        for i in range(
            len(
                findict['targets']
                )
            ):
            target = findict['targets'][i]
            maxhit = findict['max_hits'][i]
            for m in range(maxhit+1):
                url = f'https://finance.yahoo.com/markets/stocks/{target}/?start={m*100}&count=100/'
                urls_for_stocks.append(url)

        data = []

        logger.info('collecting data for symbols _______________________________--')
        for u in urls_for_stocks:
            catg = u.split('/')[-3]
            symbol_list = []
            try:
                r = rq.get(u,headers = self.headers)
            except Exception as e:
                logger.warning("cannot hit url : ",u ,r.status_code)
            soup = BeautifulSoup(r.text,'html.parser')
            symbs= soup.find_all('span',{'class':'symbol'})
            for s in symbs:
                symbol_list.append(s.text)
            data.append(
                {catg:symbol_list}
            )
        logger.info("finished collecting data for symbols ______________________________-")
        data = {'names':data}
        return data

    '''
    input : unix timestamp
    
    algorithm:
    it gets unix timestamp , and convert it to redable human timestamp .
    
    output : readable human timestamp .
    '''
    def return_human_timestamp(
        self,
        timestamps
        ):
        new_dates = []
        for utnix in timestamps:
            try:
                if isinstance(
                    utnix, 
                    str
                ):
                    datetime.strptime(utnix, '%Y-%m-%d %H:%M:%S')
                    new_dates.append(utnix)
                else:
                    utnix = float(utnix)
                    date = datetime.fromtimestamp(
                        utnix).strftime('%Y-%m-%d %H:%M:%S')
                    new_dates.append(date)
            except (
                ValueError, TypeError):
                new_dates.append(None)  
        return new_dates

    '''
    input : human timestamp
    
    algorithm:
    it gets human timestamp , and convert it to unix timestamp .
    
    output : unix timestamp .
    '''
    
    def return_unix_timestamps(self,date_strings):
        unix_timestamps = []
        for date_str in date_strings:
            try:
                dt = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
                unix_timestamp = int(dt.timestamp())  
                unix_timestamps.append(unix_timestamp)
            except (ValueError, TypeError):
                unix_timestamps.append(None)
        return unix_timestamps

    '''
    input : list of all symbols 
    
    algorithm:
    it hits all stocks urls and create jsons in 
    /scrapper/data/daily_update/[stocksymbol].json 
    it processes json , and convert unix timestamps to radable .
    
    output : nothing 
    '''
    def update_prices_for_daily(
        self,
        symbol_list
        ):
        current_timestamp = int(time.time())
        date_str = "2015-01-01"
        date_obj = datetime.strptime(
            date_str, "%Y-%m-%d")
        period1 = int(time.mktime(
            date_obj.timetuple())
        )
        period2 = current_timestamp  
        
        logger.info(f"checking updates for period1={period1} & period2={period2} for stocks daily _________________")
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
                logger.warning("request failed",url,r.status_code)
                continue
        filespaths = f'{BASE_DIR}/scrapper/data/daily_update/'
        jsnlistdaily = os.listdir(filespaths)
        
        logger.info('working on collected daily data _________________________________-')
        for jso in tqdm(jsnlistdaily):
            try:
                jsonf = pd.read_json(f'{filespaths}/{jso}')
            except:
                logger.warning("cannot read json " ,jso)
                continue
            jsondict = jsonf.to_dict()
            timestamp = jsondict.get('chart').get('result')[0].get('timestamp')
            if timestamp is not None:
                new_timestamps = self.return_timestamp(timestamp)
                jsondict['chart']['result'][0]['timestamp'] = new_timestamps
            with open(f'{filespaths}/{jso}', 'w') as json_file:
                json.dump(jsondict, json_file, indent=4)
        logger.info("daily data update finished _________________________________")
        
    '''
    input:
    stocksymbol : string : stocks symbol 
    startdate : string : format : "%y-%m-%d" starting date for the user
    enddate : string : format : "%y-%m-%d" ending date for the user
    
    algorithm:
    it reads file in 
    /scrapper/data/daily_update/[stocksymbol].json
    it maintains amessage string .
    it handles cases :
    
    Testing:
    http://localhost:8000/stocks/get_stock_daily_data/NVDA/?start=2100-01-01&end=2100-10-01 passed
    http://localhost:8000/stocks/get_stock_daily_data/NVDA/ passed
    http://localhost:8000/stocks/get_stock_daily_data/NVDA/?start=2023-01-01&end=2100-10-01 passed
    http://localhost:8000/stocks/get_stock_daily_data/NVDA/?start=2024-01-01 passed
    http://localhost:8000/stocks/get_stock_daily_data/NVDA/?start=1900-01-01&end=2024-10-01 passed
    http://localhost:8000/stocks/get_stock_daily_data/NVDA/?end=2024-10-01 passed
    http://localhost:8000/stocks/get_stock_daily_data/NVDA/?start=2024-01-01&end=2023-10-01 passed
    http://localhost:8000/stocks/get_stock_daily_data/NVDA/?start=2022-01-01&end=2024-10-01 passed
    http://localhost:8000/stocks/get_stock_daily_data/NVDA/?start=1922-01-01&end=2224-10-01 passed
        
    output:
    a response {
                'response':final_message,
                'data':{
                    'dates':[],
                    'close':[],
                    'open':[],
                    'high':[],
                    'low':[],
                    'volume':[],
                        }
    }
     '''
    def render_daily_data(self, stocksymbol, startdate, enddate):
        global_start_timestamp = None
        global_end_timestamp = None
        if startdate is not None:
            unix_starttime = f'{startdate} 00:00:00'
            date_object = datetime.strptime(unix_starttime, '%Y-%m-%d %H:%M:%S')
            global_start_timestamp = int(date_object.timestamp())

        if enddate is not None :
            unix_endtime = f'{enddate} 00:00:00'
            date_object = datetime.strptime(unix_endtime, '%Y-%m-%d %H:%M:%S')
            global_end_timestamp = int(date_object.timestamp())

        path = f'{BASE_DIR}/scrapper/data/daily_update/'
        data = pd.read_json(f'{path}/{stocksymbol}.json').to_dict()

        timestmp = data.get('chart').get('result')[0].get('timestamp', [])
        dates =   [ str(t.split(' ')[0]) for t in timestmp ]
        unix_dates = self.return_unix_timestamps(timestmp)
        new_data = data.get('chart').get('result')[0].get('indicators').get('quote')[0]

        now_timestamp = datetime.now().timestamp()
        global_startindex = None
        global_endindex = None
        final_message = ''

        def get_closer_index_if_date_is_missing(unix_date):
            if unix_date in unix_dates:
                logger.warning(f'{unix_date} from case 1 ')
                return unix_dates.index(unix_date), 'OK'

            for i in range(len(unix_dates) - 1):
                if unix_dates[i] <= unix_date <= unix_dates[i + 1]:
                    message = f"Allocated new date. New date is {timestmp[i+1]} from case 2 ."
                    logger.warning(message)
                    return i + 1, message

        def data_render_on_hit(new_data,final_message,global_startindex,global_endindex):
            final = {
                    'time': timestmp[global_startindex:global_endindex + 1],
                    'close': new_data['close'][global_startindex:global_endindex + 1],
                    'open': new_data['open'][global_startindex:global_endindex + 1],
                    'high': new_data['high'][global_startindex:global_endindex + 1],
                    'low': new_data['low'][global_startindex:global_endindex + 1],
                    'volume': new_data['volume'][global_startindex:global_endindex + 1],
                }
            response = {
                    'response': final_message,
                    'data':final
                }
            return response

        logger.warning(f'{global_start_timestamp} and {global_end_timestamp}')

        ''' Case 2: Start or end date is None '''
        if startdate is None and enddate is None:  
            thirty_days_ago = (datetime.now() - timedelta(days=30)).timestamp()
            starttime = int(thirty_days_ago)
            endtime = unix_dates[-1]
            logger.warning(f'{starttime} and {endtime } from case 2')
            startindex_,message1 = get_closer_index_if_date_is_missing(starttime)
            endindex_,message2 = get_closer_index_if_date_is_missing(endtime)
            global_startindex = startindex_
            global_endindex = endindex_
            message = "Dates were not provided. Defaulting to last 30 days."
            final_message = message1 + message2 + message 
            response = data_render_on_hit(new_data,final_message,global_startindex,global_endindex)
            return response
            
        ''' Case 6: Start date is None but end time is OK '''
        if startdate is None and global_end_timestamp <= now_timestamp:
            startindex_ = 0
            endindex_, message1 = get_closer_index_if_date_is_missing(global_end_timestamp)
            global_startindex = startindex_
            global_endindex = endindex_
            message = f"Start date is None. Providing data from {dates[0]} to {enddate}."
            final_message += message + message1
            logger.warning("assigned values from case 6")
            response = data_render_on_hit(new_data,final_message,global_startindex,global_endindex)
            return response
        
        ''' Case 4: Start time is OK but end date is none '''
        if global_start_timestamp < now_timestamp and enddate is None:
            startindex_, message1 = get_closer_index_if_date_is_missing(global_start_timestamp)
            endindex_ = len(timestmp) - 1
            global_startindex = startindex_
            global_endindex = endindex_
            message = "Start date is OK, but end date is None. Allocating the latest date."
            final_message += message1 + message
            logger.warning("assigned values from case 4")
            response = data_render_on_hit(new_data,final_message,global_startindex,global_endindex)
            return response
        
        ''' Case 9: Both start and end times are out of range  '''
        if global_start_timestamp < unix_dates[0] and global_end_timestamp > unix_dates[-1]:
            logger.warning("assigned values from case 9")
            startindex_, message1 = get_closer_index_if_date_is_missing(unix_dates[0])
            endindex_, message2 = get_closer_index_if_date_is_missing(unix_dates[-1])
            global_startindex = startindex_
            global_endindex = endindex_
            message = f' both {startdate} and {enddate} are out of available data range . providing data from {timestmp[0]} to {timestmp[-1]}'
            final_message += message + message1 + message2
            response = data_render_on_hit(new_data,final_message,global_startindex,global_endindex)
            return response
        ''' Case 1: Both dates are in the future '''
        if global_start_timestamp > now_timestamp and global_end_timestamp > now_timestamp:
            message = f'Give correct dates. {startdate} and {enddate} are in the future. Please request data between {dates[0]} and {dates[-1]}.'
            logger.warning("assigned values from case 1")
            return {'message': message, 'data': None}
        
        ''' Case 3: Start time is OK but end time is in the future '''
        if global_start_timestamp < now_timestamp and now_timestamp < global_end_timestamp:
            startindex_, message1 = get_closer_index_if_date_is_missing(global_start_timestamp)
            endindex_ = len(timestmp) - 1
            global_startindex = startindex_
            global_endindex = endindex_
            message = f"Start date is OK. {enddate} is in the future. Allocating the latest date."
            final_message += message1 + message
            logger.warning(f'assigned values from case 3 ')
            response = data_render_on_hit(new_data,final_message,global_startindex,global_endindex)
            return response
            
        ''' Case 5: Start date is earlier than available data but end time is OK '''
        if global_start_timestamp < unix_dates[0] and global_end_timestamp <= now_timestamp:
            startindex_ = 0
            endindex_, message1 = get_closer_index_if_date_is_missing(global_end_timestamp)
            message = f"Start date is earlier than available data. Providing data from {dates[0]} to {enddate}."
            global_startindex = startindex_
            global_endindex = endindex_
            final_message += message + message1
            logger.warning("assigned values from case 5")
            response = data_render_on_hit(new_data,final_message,global_startindex,global_endindex)
            return response
        
        
        ''' Case 7: Start date is after end date '''
        if global_start_timestamp > global_end_timestamp:
            startindex_, message1 = get_closer_index_if_date_is_missing(global_start_timestamp)
            endindex_, message2 = get_closer_index_if_date_is_missing(global_end_timestamp)
            global_startindex = startindex_
            global_endindex = endindex_
            message = f"{startdate} is later than {enddate}. Did you mean to swap them? Allocating the corrected data."
            final_message += message + message1 + message2
            logger.warning("assigned values from case 7")
            response = data_render_on_hit(new_data,final_message,global_endindex=global_startindex,global_startindex=global_endindex)
            return response
        
        ''' Case 8: Both start and end times are valid '''
        if global_start_timestamp >= unix_dates[0] and global_end_timestamp <= unix_dates[-1]:
            startindex_, message1 = get_closer_index_if_date_is_missing(global_start_timestamp)
            endindex_, message2 = get_closer_index_if_date_is_missing(global_end_timestamp)
            global_startindex = startindex_
            global_endindex = endindex_
            final_message += message1 + message2
            logger.warning("assigned values from case 8")
            response = data_render_on_hit(new_data,final_message,global_startindex,global_endindex)
            return response
        
        else:
            message = f"Unknown issue with the dates {startdate} and {enddate}. Please raise an issue."
            pass

        logger.warning(f'printing {global_startindex} {global_endindex} ',)
        
    def update_prices_for_per_minute(
        self,
        symbol_list,
    ):
        os.makedirs(f'{BASE_DIR}/scrapper/data/per_minute',exist_ok=True)
        period1 = int(datetime.now().timestamp())
        period2 = int((datetime.now() - timedelta(days=7)).timestamp())

        logger.info(f"checking updates for period1={period1} & period2={period2} for stocks per minute _________________")
        for stock in tqdm(symbol_list):
            stock = stock[1].replace(' ','')
            link = f'https://query2.finance.yahoo.com/v8/finance/chart/{stock}?period1={period2}&period2={period1}&interval=1m&includePrePost=true&events=div%7Csplit%7Cearn&&lang=en-US&region=US'
            r = rq.get(
                link,
                headers = self.headers
            )
            path = f'{BASE_DIR}/scrapper/data/per_minute/{stock}/_{period1}_{period2}.json'
            os.makedirs(os.path.dirname(path), exist_ok=True)
            if r.status_code == 200:
                with open(path,'wb') as jsn:
                    jsn.write(r.content)
            else:
                logger.warning("request failed",link,r.status_code)
                continue
            
        filespaths = f'{BASE_DIR}/scrapper/data/per_minute/'
        jsnlistdaily = os.listdir(filespaths)
        
        logger.info('working on collected per minute data _________________________________-')
        for folder in tqdm(jsnlistdaily):
            files = os.listdir(f'{filespaths}/{folder}')
            file = files[-1]
            try:
                jsonf = pd.read_json(f'{filespaths}/{folder}/{file}')
            except:
                logger.warning("cannot read json :",f'{filespaths}/{folder}/{file}')
                continue
            jsondict = jsonf.to_dict()
            timestamp = jsondict.get('chart').get('result')[0].get('timestamp')
            if timestamp is not None:
                new_timestamps = self.return_timestamp(timestamp)
                jsondict['chart']['result'][0]['timestamp'] = new_timestamps
            with open(f'{filespaths}/{folder}/{file}', 'w') as json_file:
                json.dump(jsondict, json_file, indent=4)

        logger.info("per minute update finished _________________________________")
            
    def render_per_minute_data(
        self, 
        stocksymbol, 
        startdate, 
        enddate
    ):
        path = f'{BASE_DIR}/scrapper/data/per_minute/{stocksymbol}/'
        jsons = os.listdir(path)
        megajson = {}
        for j in jsons:
            data = pd.read_json(f'{path}/{j}').to_dict()
            timestmp = data.get('chart').get('result')[0].get('timestamp', [])
            dates =   [ str(t.split(' ')[0]) for t in timestmp ]
            new_data = data.get('chart').get('result')[0].get('indicators').get('quote')[0]

            startindex = None
            endindex = None
            final_message = ''

            if startdate is None or enddate is None:
                message = "Dates were not provided.\n"
                thirty_days_ago = (
                    datetime.now() - timedelta(
                        days=30)
                    ).strftime('%Y-%m-%d')
                startdate = thirty_days_ago
                enddate = dates[-1]
                final_message = final_message+message

            try:
                if startdate in dates:
                    startindex = dates.index(startdate)
                else:
                    message = "Start date not found. Locating later date in dates.   "
                    logger.warning(message)
                    final_message = final_message+message
                    for i in range(3):
                        next_date = (
                            datetime.strptime(
                                startdate, '%Y-%m-%d') + timedelta(
                                    days=i + 1)
                                ).strftime('%Y-%m-%d')
                        if next_date in dates:
                            message = f"Located new start date: {next_date}   "
                            logger.info(message)
                            final_message = final_message+message
                            startdate = next_date
                            startindex = dates.index(startdate)
                            break
                    if startindex is None:
                        logger.debug("Start date not found within next 3 days range.")

                if enddate in dates:
                    endindex = dates.index(enddate)
                else:
                    message = "End date not found. Locating later date in dates.   "
                    logger.warning(message)
                    final_message = final_message+message
                    for i in range(3):
                        next_date = (
                            datetime.strptime(
                                enddate, '%Y-%m-%d') + timedelta(
                                    days=i + 1)
                                ).strftime('%Y-%m-%d')
                        if next_date in dates:
                            message = f"Located new end date: {next_date}   "
                            logger.info(message)
                            final_message = final_message+message
                            enddate = next_date
                            endindex = dates.index(enddate)
                            break
                    if endindex is None:
                        logger.debug("End date not found within behind 3 days range.")

            except ValueError as e:
                logger.debug(f"Error: {e}")
                
            data = {
                    'time': timestmp[startindex:endindex + 1],
                    'close': new_data['close'][startindex:endindex + 1],
                    'open': new_data['open'][startindex:endindex + 1],
                    'high': new_data['high'][startindex:endindex + 1],
                    'low': new_data['low'][startindex:endindex + 1],
                    'volume': new_data['volume'][startindex:endindex + 1],
                }
                
            startforjson = timestmp[0]
            endforjson = timestmp[-1]
            megajson[f'_{startforjson}_{endforjson}'] = data
                
        response = {
                    'response':final_message,
                    'data':megajson
                }
        return response
