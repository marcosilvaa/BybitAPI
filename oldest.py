
import requests
from datetime import datetime
import pandas as pd
import numpy as np
from scipy.stats import norm

# Função que obtém a lista com os tickers disponíveis
def fetch_available_symbols(category="spot"):
    """
    Função para obter os tickers disponíveis na api da Bybit

    # Args:
        category (str): Categoria do ativo, 'spot' ou 'futures'. 
                        Para o Brasil, dados de 'futures' não estão disponíveis.

    Raises:
        Exception: Problema com a conexão da api. 

    Returns:
        Dataframe: Dataframe com o ticker, moeda base, moeda de cotação, status da moeda
    """
    
    # Definindo as constantes
    url = "https://api.bybit.com/v5/market/instruments-info"
    params = {"category": category}

    # Obtendo json via url da API 
    r = requests.get(url, params=params)
    r.raise_for_status()
    data = r.json()

    # Transformando dados em Dataframe
    if data['retCode'] == 0:
        df = pd.DataFrame(data["result"]["list"])
        return df[["symbol", "baseCoin", "quoteCoin", "status"]]
    else:
        raise Exception(f"Erro ao obter símbolos: {data['retMsg']}")
    
    
def fetch_data(symbol='BTCUSDT', category="spot", interval="D", limit=1000):
    SYMBOL = symbol
    CATEGORY = category
    INTERVAL = interval
    LIMIT = limit

    BASE_URL = "https://api.bybit.com"
    endpoint = BASE_URL + "/v5/market/kline"

    params = {
        "category": CATEGORY,
        "symbol": SYMBOL,
        "interval": INTERVAL,
        "limit": LIMIT
    }
    
    # Faz a requisição
    resp = requests.get(endpoint, params=params)
    resp.raise_for_status()  # Levanta uma exceção se o status code não for 200

    # Processa a resposta e cria DataFrame com os dados obtidos
    data = resp.json()
    if data['retCode'] == 0:

        df = pd.DataFrame(data["result"]["list"], columns=["Date","Open","High","Low","Close","Volume","Turnover"])
        df = df.drop(columns=["Turnover"], axis=1)
        df[['Date','Open', 'High', 'Low', 'Close', 'Volume']] = df[['Date','Open', 'High', 'Low', 'Close', 'Volume']].astype("float")
        df = df.set_index(df.Date, drop=True)
        df = df.sort_index()
        df.index = pd.to_datetime(df.index, unit="ms").tz_localize('UTC').tz_convert('America/Sao_Paulo')
        df = df.drop(columns=["Date"])
        return df
    
    else:
        print(f"Erro: {data['retMsg']}")

def calculate_parametric_var(rates, confidence_level=0.95):
    returns = rates['Close'].pct_change().dropna()
    mean_return = returns.mean()
    std_dev = returns.std()
    var_critical_value = norm.ppf(1-confidence_level)
    var_value = mean_return + var_critical_value * std_dev
    return (var_value * np.sqrt(1)*100).round(4)