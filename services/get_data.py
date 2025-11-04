import requests
from datetime import datetime
import pandas as pd
import numpy as np
from scipy.stats import norm
import os
from dotenv import load_dotenv

load_dotenv() 

class MarketData:

    def __init__(self):
        self.base_url = os.getenv('BASE_URL')
    
    def server_time(self):
        self.base_url = os.getenv("BASE_URL")
        
        endpoint = self.base_url + 'market/time'
        
        r=requests.get(endpoint)
        r.raise_for_status()
        data = r.json()
        
        if data['retCode'] == 0:
#            df = pd.DataFrame(data['result'], columns=['timeSecond','timeNano'])
            time = data['time']
            return time
        else:
            raise Exception(f'Erro ao obter `server time`: {data['retMsg']}')

    def symbols_list(self, category: str='spot'): 
        self.category = category

        url = f'{self.base_url}market/instruments-info'
        params = {"category": self.category}
        
        r = requests.get(url, params=params)
        r.raise_for_status()
        data = r.json()
        
        if data['retCode'] == 0:
            df = pd.DataFrame(data['result']['list'])
            return df
        else:
            raise Exception(f'Erro ao obter símbolos: {data['retMsg']}')

    def kline(self, symbol: str='BTCUSDT', category:str ='spot', interval:str = 'D', limit:int = 1000):
        '''
        interval. 1,3,5,15,30,60,120,240,360,720,D,W,M
        '''
        
        self.symbol = symbol
        self.category = category
        self.interval = interval
        self.limit = limit
        self.base_url = os.getenv('BASE_URL')
        
        
        endpoint = self.base_url + "market/kline"
        
        params = {
            "category": self.category,
            "symbol": self.symbol,
            'interval': self.interval,
            "limit": self.limit            
        }
        
        r = requests.get(endpoint, params=params)
        r.raise_for_status()
        data = r.json()
        
        if data['retCode'] == 0:
            df = pd.DataFrame(data['result']['list'], columns=['Date','Open','High','Low','Close','Volume','Turnover'])
            df = df.drop(columns=['Turnover'], axis=1)
            df[['Date','Open', 'High', 'Low', 'Close', 'Volume']] = df[['Date','Open', 'High', 'Low', 'Close', 'Volume']].astype("float")
            df = df.set_index(df.Date, drop=True)
            df = df.sort_index()
            df.index = pd.to_datetime(df.index, unit='ms').tz_localize('UTC').tz_convert('America/Sao_Paulo')
            df = df.drop(columns=['Date'])
            return df
        
        else:
            raise Exception(f'Erro: {data['retMsg']}')
            
            
    def orderbook(self, category:str = 'spot', symbol:str='BTCUSDT'):
        self.category = category
        self.symbol = symbol
        self.base_url = os.getenv('BASE_URL')
        
        params = {
            'category': self.category,
            'symbol': self.symbol
        }
        
        endpoint = self.base_url + 'market/orderbook'
        
        r = requests.get(endpoint, params=params)
        r.raise_for_status()
        data = r.json()
        
        result = data["result"]

        if data['retCode'] == 0:
            # -------------------- conversões --------------------
            # timestamp em milissegundos → Timestamp UTC do pandas
            ts_utc: pd.Timestamp = pd.to_datetime(result["ts"], unit="ms", utc=True)

            # Primeiro nível do ask e do bid (price e size vêm como strings)
            ask_price, ask_size = map(float, result["a"][0])
            bid_price, bid_size = map(float, result["b"][0])

            # -------------------- montagem do DataFrame --------------------
            row = {
                "date":       ts_utc,          # ou ts_utc.tz_convert(None) se quiser sem timezone
                "ask_size":   ask_size,
                "bid_size":   bid_size,
                "ask_price":  ask_price,
                "bid_price":  bid_price,
            }

            df = pd.DataFrame([row], columns=["date", "ask_size", "bid_size", "ask_price", "bid_price"])
            df.index = pd.to_datetime(df['date'])
            df = df.drop(columns=['date'])
            
            return df
        else:
            raise Exception(f'Erro: {data['retMsg']}')

    
    def tickers(self, category:str='spot', symbol:str='BTCUSDT'):
        self.category = category
        self.symbol = symbol
        self.base_url = os.getenv('BASE_URL')
        
        params = {
            'category': self.category,
            'symbol': self.symbol
        }
        
        endpoint = self.base_url + 'market/tickers'
        
        r = requests.get(endpoint, params=params)
        r.raise_for_status()
        data = r.json()
        
        result = data["result"]

        if data['retCode'] == 0:
            df = pd.DataFrame(result['list'], columns=['Symbol', 'BidPrice','BidSize','AskPrice','AskSize','LastPrice',
                                                       'prevPrice24h','price24hpct','highPrice24h','lowPrice24h','turnover24h',
                                                       'volume24h', 'usdIndexPrice'])
            #df['BidPrice','BidSize','AskPrice','AskSize','LastPrice','prevPrice24h','price24hpct','highPrice24h','lowPrice24h','turnover24h','volume24h', 'usdIndexPrice'] = df['BidPrice','BidSize','AskPrice','AskSize','LastPrice','prevPrice24h','price24hpct','highPrice24h','lowPrice24h','turnover24h','volume24h', 'usdIndexPrice'].astype(float)
            df = df.transpose()
            return df
        else:
            raise Exception(f'Erro: {data['retMsg']}')
        
    def funding_rate(self, category:str='linear', symbol:str='BTCUSDT'):
        self.category = category
        self.symbol = symbol
        self.base_url = os.getenv('BASE_URL')
        
        params = {
            'category': self.category,
            'symbol': self.symbol
        }
        
        endpoint = self.base_url + 'market/funding/history'
        
        r = requests.get(endpoint, params=params)
        r.raise_for_status()
        data = r.json()
        
        result = data["result"]
        
        if data['retCode'] == 0:
            df = pd.DataFrame(result['list'], columns=['symbol','fundinRate','fundingRateTimestamp'])
            return df 
        else:
            raise Exception(f'Erro: {data['retMsg']}')
    
    def recent_trades(self, category:str='linear', symbol:str='BTCUSDT'):
        self.category = category
        self.symbol = symbol
        self.base_url = os.getenv('BASE_URL')
        
        params = {
            'category': self.category,
            'symbol': self.symbol
        }
        
        endpoint = self.base_url + 'market/recent-trade'
        
        r = requests.get(endpoint, params=params)
        r.raise_for_status()
        data = r.json()
        
        result = data["result"]
        
        if data['retCode'] == 0:
            df = pd.DataFrame(result['list'], columns=['execId','symbol','price','size','side','time','isBlockTrade','isRPITrade','seq'])
            return df 
        else:
            raise Exception(f'Erro: {data['retMsg']}')
        
    