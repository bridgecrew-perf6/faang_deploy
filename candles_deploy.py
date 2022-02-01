from unittest.util import _MAX_LENGTH
import tabpy_client
from tabpy.tabpy_tools.client import Client
import pandas as pd
import yfinance as yf

client = tabpy_client.Client("http://localhost:9004/")

def prepare_intradate_df(intraday_data):
    _MAX_LENGTH = 2730
    intraday_data_full_length = (pd.DataFrame(data = {'row_number': [i for i in range(0, _MAX_LENGTH)]}))
    intraday_data = intraday_data.reset_index().reset_index().rename(columns = {'index' : 'row_number'})
    intraday_data = intraday_data.merge(intraday_data_full_length, on = 'row_number', how = 'right')
    return intraday_data

def reshape_data(intraday_data, agg_level):
    ohlcv_dict = {'Open': 'first',
                    'High': 'max',
                    'Low': 'min',
                    'Close': 'last',
                    'Volume': 'sum'}
    intraday_data = intraday_data.resample(agg_level).agg(ohlcv_dict)
    print('*************')
    print('intraday_data')
    print(intraday_data.head())
    print('*************')
    return intraday_data

# Prepare the model to deploy
def get_datetime(_arg1, _arg2, _arg3):
    print('_arg1', _arg1[0])
    print('_arg2', _arg2[0])
    intraday_data = yf.download(tickers = _arg1[0],
                                period = _arg2[0],
                                interval="1m",
                                auto_adjust=True)
    intraday_data = reshape_data(intraday_data, agg_level = _arg3[0])
    intraday_data = prepare_intradate_df(intraday_data) 
    intraday_data.Datetime = intraday_data.Datetime.astype(str).str[:19]
    return intraday_data['Datetime'].tolist()

def get_open(_arg1, _arg2, _arg3):
    print('_arg1', _arg1[0])
    print('_arg2', _arg2[0])
    intraday_data = yf.download(tickers = _arg1[0],
                                period = _arg2[0],
                                interval="1m",
                                auto_adjust=True)
    intraday_data = reshape_data(intraday_data, agg_level = _arg3[0])
    intraday_data = prepare_intradate_df(intraday_data) 
    return intraday_data['Open'].tolist()

def get_close(_arg1, _arg2, _arg3):
    intraday_data = yf.download(tickers = _arg1[0],
                                period = _arg2[0],
                                interval="1m",
                                auto_adjust=True)
    intraday_data = reshape_data(intraday_data, agg_level = _arg3[0])
    intraday_data = prepare_intradate_df(intraday_data) 
    return intraday_data['Close'].tolist()   

def get_high(_arg1, _arg2, _arg3):
    intraday_data = yf.download(tickers = _arg1[0],
                                period = _arg2[0],
                                interval="1m",
                                auto_adjust=True)
    intraday_data = reshape_data(intraday_data, agg_level = _arg3[0])
    intraday_data = prepare_intradate_df(intraday_data) 
    return intraday_data['High'].tolist()      

def get_low(_arg1, _arg2, _arg3):
    intraday_data = yf.download(tickers = _arg1[0],
                                period = _arg2[0],
                                interval="1m",
                                auto_adjust=True)
    intraday_data = reshape_data(intraday_data, agg_level = _arg3[0])
    intraday_data = prepare_intradate_df(intraday_data) 
    return intraday_data['Low'].tolist()   

def get_volume(_arg1, _arg2, _arg3):
    intraday_data = yf.download(tickers = _arg1[0],
                                period = _arg2[0],
                                interval="1m",
                                auto_adjust=True)
    intraday_data = reshape_data(intraday_data, agg_level = _arg3[0])
    intraday_data = prepare_intradate_df(intraday_data) 
    return intraday_data['Volume'].tolist()      

#deploy the model
client.deploy("get_datetime", get_datetime, "get_datetime", override = True)
client.deploy("get_open", get_open, "get_open", override = True)
client.deploy("get_close", get_close, "get_close", override = True)
client.deploy("get_high", get_high, "get_high", override = True)
client.deploy("get_low", get_low, "get_low", override = True)
client.deploy("get_volume", get_volume, "get_volume", override = True)
print(client.get_endpoints())