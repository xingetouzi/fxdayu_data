# encoding:utf-8
# How to use StockData and OandaData to request and save time series data
from fxdayu_data.data import market_data, OandaData
from datetime import datetime


def input_hs_stock():
    sd = market_data(db='HS_Stock')
    sd.save_k_data('000001.D')


def input_hs_300():
    sd = market_data(db='hs300')
    sd.save_stocks('hs300s')


def input_yahoo_daily():
    sd = market_data(db='Yahoo')
    sd.save_yahoo(symbols='0700.hk')


def input_fred():
    from pandas_datareader.data import FredReader

    sd = market_data(db='fred')
    sd.client.inplace(FredReader('GDP').read(), 'GDP')


def oanda_history():
    od = OandaData("D:\\bigfishtrader\\bigfish_oanda.json")
    od.save_history('EUR_USD', granularity='H1', start=datetime(2017, 1, 1))


def oanda_main():
    od = OandaData("D:\\bigfishtrader\\bigfish_oanda.json")
    od.save_main(datetime(2017, 1, 1))


def forex_lab():
    od = OandaData("D:\\bigfishtrader\\bigfish_oanda.json", db='ForexLab')
    symbol = 'EUR_USD'
    od.save('HPR', symbol+'.HPR', instrument=symbol)
    od.save('CLD', symbol+'.CLD', instrument=symbol)
    od.save('COT', symbol+'.COT', instrument=symbol)


def oanda_update():
    od = OandaData("D:\\bigfishtrader\\bigfish_oanda.json")
    od.update_many()


if __name__ == '__main__':
    forex_lab()