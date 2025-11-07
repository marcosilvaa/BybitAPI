import os
import requests
import pandas as pd
from tqdm import tqdm
from typing import Optional, Union
from datetime import datetime, timezone
import time

from dotenv import load_dotenv
load_dotenv() 

class MarketData:
    """
    Wrapper para os endpoins da API Bybit (market data).
    """

    # ------------------------------------------------------------------
    # 1️⃣  Intervalos aceitos pela API (valor → passo para avançar)
    # ------------------------------------------------------------------
    _VALID_INTERVALS = {
        "1":   pd.Timedelta(minutes=1),
        "3":   pd.Timedelta(minutes=3),
        "5":   pd.Timedelta(minutes=5),
        "15":  pd.Timedelta(minutes=15),
        "30":  pd.Timedelta(minutes=30),
        "60":  pd.Timedelta(minutes=60),
        "120": pd.Timedelta(minutes=120),
        "240": pd.Timedelta(minutes=240),
        "360": pd.Timedelta(minutes=360),
        "720": pd.Timedelta(minutes=720),
        "D":   pd.Timedelta(days=1),
        "W":   pd.Timedelta(weeks=1),
        "M":   pd.DateOffset(months=1),         
    }


    def __init__(self):
        self.base_url = os.getenv('BASE_URL_BYBIT')
        
    
    def server_time(self):
        endpoint = self.base_url + 'market/time'    
        r = requests.get(endpoint)
        r.raise_for_status()
        data = r.json()
        if data['retCode'] == 0:
            return data['time']
        else:
            raise Exception(f'Erro ao obter `server time`: {data['retMsg']}')


    def symbols_list(self, category: str='spot'): 
        url = f'{self.base_url}market/instruments-info'
        r = requests.get(url, params={"category": category})
        r.raise_for_status()
        data = r.json()
        if data['retCode'] == 0:
            return pd.DataFrame(data['result']['list'])
        else:
            raise Exception(f'Erro ao obter símbolos: {data['retMsg']}')

        
    @staticmethod
    def _to_ms(ts: Optional[Union[int, str, datetime]]):
        '''
        Converte timestamp em milissegundos (UTC).
        '''
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
        
        if not (1 <= limit <= 1000):
            raise ValueError("limit deve estar entre 1 e 1000")
        
        start_ms = self._to_ms(start)
        end_ms = self._to_ms(end)
        
        if start_ms is not None and end_ms is not None and start_ms > end_ms:
            raise ValueError("Start deve ser anterior ou igual a End")
        
        endpoint_url =  f"{self.base_url}market/{endpoint}"
        params = {
            "category": category,
            "symbol": symbol,
            'interval': interval,
            "limit": limit
        }
        if start_ms is not None:
            params['start'] = start_ms
        if end_ms is not None:
            params['end'] = end_ms

        r = requests.get(endpoint_url, params=params)
        r.raise_for_status()
        data = r.json()
        
        if data['retCode'] == 0:
            df = pd.DataFrame(data['result']['list'], columns=['Date','Open','High','Low','Close','Volume','Turnover'])
            df = df.drop(columns=['Turnover'], axis=1)
            df[['Date','Open', 'High', 'Low', 'Close', 'Volume']] = \
                df[['Date','Open', 'High', 'Low', 'Close', 'Volume']].astype("float")
            df = df.set_index(df.Date, drop=True)
            df.index = pd.to_datetime(df.index, unit='ms').tz_localize('UTC').tz_convert('America/Sao_Paulo')
            df = df.sort_index()
            df = df.drop(columns=['Date'])
            return df
        else:
            raise Exception(f'Erro: {data['retMsg']}')
            
            
    def full_data(
        self,
        symbol: str='BTCUSDT',
        start: str = '2021-01-01 00:00:00-03:00',
        interval: str= 'D',
        category: str='spot',
        pause: float=0.2
    ) -> pd.DataFrame:
        """
        Coleta dados OHLCV completos desde `start` até o presente.

        Parameters
        ----------
        symbol : str, default ``BTCUSDT``.
            Par de negociação a ser coletado.
            
        start : str
            Timestamp inicial (ISO 8601). Obrigatório. API nao aceita start=None.
            Ex.: ``2021-01-01 00:00:00-03:00``.
            
        interval : str, default ``D``.
            Intervalos permitidos:
            ``1`` · ``3`` · ``5`` · ``15`` · ``30`` · ``60`` · ``120`` ·
            ``240`` · ``360`` · ``720`` · ``D`` · ``W`` · ``M``.
        
        category : str, default ``spot``.
            ``spot``, ``linear`` ou ``inverse``.
        
        pause : float, default ``0.2``.
            Tempo de espera (em segundos) entre requisições.
            
        Returns
        -------
        pd.DataFrame
            Índice datetime (UTC → America/Sao_Paulo) e colunas
            ``Open, High, Low, Close, Volume``.
            
        Raises
        ------
        ValueError
            Se ``interval`` não estiver na lista de intervalos permitidos ou
            se ``start`` for ``None``.
        """
        
        # Validações iniciais
        if interval not in self._VALID_INTERVALS:
            valid = ', '.join(sorted(self._VALID_INTERVALS.keys()))
            raise ValueError(
                f"Intervalo inválido: {interval}. Intervalos válidos são: {valid}")
            
        step = self._VALID_INTERVALS[interval]
        
        if start is None:
            raise ValueError("O parâmetro 'start' é obrigatório.")

        # Conversao de data de partida para Timestamp com fuso da API
        
        # O Método prie_data() converte o parametro 'start' para ms UTC
        # usando _to_ms(), que aceita strings com ISO com offset. 
        # Aqui usamos apenas pandas para fazer a aritmetica do incremento
        cur = pd.Timestamp(start)
        # Se a string nao trouxer timzone, assuma UTC (consistente com _to_ms)
        if cur.tzinfo is None:
            cur = cur.tz_localize('UTC')
        # Convertemos para o fuso usado internamenteo pelos DataFrames retornados
        cur = cur.tz_convert('America/Sao_Paulo')
        
        # Loop de coleta
        blocks: list[pd.DataFrame] = []
        try:
            iterator = tqdm(desc='Coletando dados', unit='blocos')
        except Exception:
            iterator = None 
        
        while True:
            # Chamada da API
            df = self.price_data(
                symbol=symbol,
                category=category,
                interval=interval,
                limit=1000,
                start=cur.isoformat(),
                endpoint='kline'
            )
            if df.empty:
                break
            blocks.append(df)
            
            # Se o bloco retornou menos que 1000 linhas, chegou ao fim
            if len(df) < 1000:
                break
            
            # Ultimo timestamp coletado
            ultimo_ts = df.index[-1]

            # Proxima chamada: ultimo candle + passo (para nao duplicar)
            cur = ultimo_ts + step
            
            # Seguranca contra loops infinitos (caso a API devolva o mesmo lote)
            if cur <= ultimo_ts:
                break
            
            # Pausa opicional para respeitar  rate-limit da API
            if pause:
                time.sleep(pause)
                
            if iterator is not None:
                iterator.update(1)
                iterator.set_postfix({'Último': str(ultimo_ts)})
            
        if iterator is not None:
            iterator.close()
        
        # Concatena, elimina duplucatas e devolve o df
        if not blocks:
            return "Nenhum dado coletado", pd.DataFrame()
        
        full = pd.concat(blocks)
        full = full[~full.index.duplicated(keep='first')]
        full = full.sort_index()
        return full 
            
        
    def orderbook(self, category:str = 'spot', symbol:str='BTCUSDT'):
        params = {
            'category': category,
            'symbol': symbol
        }
        endpoint_url = self.base_url + 'market/orderbook'
        r = requests.get(endpoint_url, params=params)
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
        self.base_url = os.getenv('BASE_URL_BYBIT')
        
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
        self.base_url = os.getenv('BASE_URL_BYBIT')
        
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
        self.base_url = os.getenv('BASE_URL_BYBIT')
        
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
        
    