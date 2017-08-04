from fxdayu_data.data_api import BasicConfig
from fxdayu_data.handler.mongo_handler import read


PRICE = ['open', 'high', 'low', 'close']


class Adjust(BasicConfig):

    def __init__(self):
        self.db = None

    @classmethod
    def db(cls, db):
        obj = cls()
        obj.set(db)
        return obj

    def set(self, db):
        self.db = db

    def cal(self, code, frame):
        table = self.read(code)
        return calculate(table, frame)

    def read(self, code):
        return read(self.db[code], index="start")


def calculate(table, candle):
    start = locate(table, candle.index[0])
    if in_position(table, start):
        end = locate(table, candle.index[-1])
        for start, end, value in locate_range(table, start, end):
            adjust_price(candle, value, start, end)
    else:
        candle[PRICE] *= table.ix[-1, 'adjust']

    return candle



def in_position(table, pos):
    return pos < len(table)


def adjust_price(candle, value, begin, stop):
    begin = candle.index.searchsorted(begin)
    stop = candle.index.searchsorted(stop) if stop is not None else None
    candle.ix[begin:stop, PRICE] *= value


def locate_range(table, start, end):
    for i in range(start-1, end):
        try:
            yield table.index[i], table.index[i+1], table['adjust'].iloc[i]
        except IndexError:
            yield table.index[i], None, table['adjust'].iloc[i]


def locate(table, value):
    return table.index.searchsorted(value)


def main():
    from pymongo import MongoClient

    client = MongoClient("192.168.0.102")
    stock = client['Stock_H']

    adj = Adjust()
    adj.set(client['adjust'])

    code = '000001.XSHE'
    from datetime import datetime

    print adj.cal(code, read(stock[code], start=datetime(2017, 7, 3)))


if __name__ == '__main__':
    # import pandas as pd
    #
    # print pd.Index(range(10)).searchsorted(8.5)

    main()