# encoding:utf-8
# How to use StockData and OandaData to request and save time series data
from fxdayu_data.data import StockData, OandaData
from datetime import datetime


def input_hs_stock():
    sd = StockData(db='HS_Stock')
    sd.save_k_data('000001.D')


def input_hs_300():
    sd = StockData(db='HS300')
    sd.save_hs300()


def input_yahoo_daily():
    sd = StockData(db='Yahoo')
    sd.save_yahoo(symbols='0700.hk')


def input_fred():
    from pandas_datareader.data import FredReader

    sd = StockData(db='fred')
    sd.save(
        FredReader('GDP').read(),
        'GDP', index=True
    )


def oanda_history():
    od = OandaData("D:\\bigfishtrader\\bigfish_oanda.json")
    od.save_history('EUR_USD', granularity='H1', start=datetime(2017, 1, 1))


def oanda_main():
    od = OandaData("D:\\bigfishtrader\\bigfish_oanda.json")
    od.save_main(datetime(2017, 1, 1))


def forex_lab():
    od = OandaData("D:\\bigfishtrader\\bigfish_oanda.json", db='ForexLab')
    symbol = 'EUR_USD'
    lab = {}
    lab[symbol+'.HPR'] = od.get_hpr(symbol)
    lab[symbol+'.COT'] = od.get_cot(symbol)
    lab[symbol+'.CLD'] = od.get_calender(symbol)

    for key, value in lab.items():
        print value
        od.save(value, key)


def oanda_update():
    od = OandaData("D:\\bigfishtrader\\bigfish_oanda.json")
    od.update_many()
