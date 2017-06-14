# encoding:utf-8
from collections import Iterable

import pandas as pd

from fxdayu_data.data_api import BasicConfig, lru_cache
from fxdayu_data.handler.mongo_handler import ensure_index


try:
    SINGLE = (str, unicode)
except NameError:
    SINGLE = str


def adjust_candle(frame, adjust_factor, price):
        factor = pd.Series(adjust_factor, frame.index).bfill()
        for name in price:
            frame[name] = frame[name] * factor
        return frame


class Candle(BasicConfig):

    COLUMNS = ['datetime', 'open', 'high', 'low', 'close', 'volume']
    PRICE = ['open', 'high', 'low', 'close']

    def __init__(self):
        self.freq = {}
        self.indexer = ensure_index('datetime', self.COLUMNS)
        self.adjust = tuple()

    def read_adjust(self, start, end, projection, after=True):
        handler, col, db = self.adjust
        adjust = handler.read(collection=col, db=db, start=start, end=end, projection=projection)
        if after:
            return adjust
        else:
            for name, item in adjust.iteritems():
                adjust[name] = item[-1] / item
            return adjust

    def set_adjust(self, handler, db, collection):
        self.adjust = (handler, collection, db)

    @lru_cache(128)
    def __call__(self, symbols, freq, fields=None, start=None, end=None, length=None, adjust=None):
        handler, db = self.get(freq)
        fields = self.indexer(fields)

        data = handler.read(symbols, db, start=start, end=end, length=length, projection=fields)
        if not adjust or (len(data) == 0):
            return data
        else:
            price = filter(lambda p: p in self.PRICE, fields)
            if isinstance(data, pd.DataFrame):
                adjust_table = self.read_adjust(
                    data.index[0], data.index[-1],
                    self.indexer(symbols), adjust == 'after')
                for name, item in adjust_table.iteritems():
                    data = adjust_candle(data, item, price)
                return data
            elif isinstance(data, pd.Panel):
                adjust_table = self.read_adjust(
                    data.major_axis[0], data.major_axis[-1],
                    self.indexer(data.items), adjust == 'after')
                for name, item in adjust_table.iteritems():
                    data[name] = adjust_candle(data[name], item, price)
                return data
            else:
                return data

    def set(self, handler, **freq):
        for f, db in freq.items():
            self.freq[f] = (handler, db)

    def get(self, freq):
        return self.freq[freq]
