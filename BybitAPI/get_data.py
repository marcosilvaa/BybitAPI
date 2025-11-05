import requests
from datetime import datetime
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
from datetime import datetime, timezone
from typing import Optional, Union
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
        
    @staticmethod
    def _to_ms(ts: Optional[Union[int, str, datetime]]):
        if ts is None:
            return None
        if isinstance(ts, int):
            return ts
        if isinstance(ts, datetime):
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            else:
                ts = ts.astimezone(timezone.utc)
            return int(ts.timestamp() * 1_000)
        
        try:
            pd_ts = pd.Timestamp(ts, tz='UTC')
            return int(pd_ts.timestamp() * 1_000)
        except Exception as exc:
            raise ValueError(f"Não foi possível converter '{ts}' para timestamp")
        

    def price_data(
        self, 
        symbol: str='BTCUSDT',
        category:str ='spot',
        interval:str = 'D',
        limit:int = 1000,
        start:str = None,
        end:str = None,
        endpoint:str = 'kline'
        ):
        """
        Obtém candles OHLCV.

        Parameters
        ----------
        symbol : str
            Ex.: ``BTCUSDT``.
        category : str
            ``spot``, ``linear`` ou ``inverse``.
        interval : str
            ``1`` · ``3`` · ``5`` · ``15`` · ``30`` · ``60`` · ``120`` ·
            ``240`` · ``360`` · ``720`` · ``D`` · ``W`` · ``M``.
        limit : int, opcional
            Quantos candles trazer (1‑1000). Valor padrão = 200.
        start, end : int | str | datetime | None
            Timestamp inicial/final **em milissegundos (UTC)** ou objeto ``datetime``.
            Quando ``None`` o intervalo padrão da API (últimos ``limit`` candles) é usado.
        endpoint: str
            ``kline``, ``mark-price-kline``, ``index-price-kline``,``premium-index-price-kline``

        Returns
        -------
        pd.DataFrame
            Índice datetime (UTC → America/Sao_Paulo) e colunas
            ``Open, High, Low, Close, Volume``.
        """
        
        self.symbol = symbol
        self.category = category
        self.interval = interval
        self.limit = limit
        self.base_url = os.getenv('BASE_URL')
        self.start = start
        self.end = end
        self.endpoint = endpoint
        
        if not (1 <= limit <= 1000):
            raise ValueError("limit deve estar entre 1 e 1000")
        
        start_ms = self._to_ms(start)
        end_ms = self._to_ms(end)
        
        if start_ms is not None and end_ms is not None and start_ms > end_ms:
            raise ValueError("Start deve ser anterior ou igual a End")
        
        endpoint = self.base_url + "market/" + self.endpoint
        
        params = {
            "category": self.category,
            "symbol": self.symbol,
            'interval': self.interval,
            "limit": self.limit            
        }
        if start_ms is not None:
            params['start'] = start_ms
        if end_ms is not None:
            params['end'] = end_ms
        
        
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
        
    