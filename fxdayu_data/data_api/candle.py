# encoding:utf-8
from collections import Iterable
import six
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


def replace(old, new):
    return lambda code: code.replace(old, new)


class Candle(BasicConfig):

    COLUMNS = ['datetime', 'open', 'high', 'low', 'close', 'volume']
    PRICE = ['open', 'high', 'low', 'close']

    def __init__(self):
        self.freq = {}
        self.indexer = ensure_index('datetime', self.COLUMNS)
        self.adjust = None

    def set_adjust(self, adjust):
        self.adjust = adjust

    def read(self, handler, db, symbol, fields, start, end, length, adjust):
        data = handler.read(symbol, db, start=start, end=end, length=length, projection=fields)
        if adjust:
            return self.adjust.cal(symbol, data)
        else:
            return data

    @lru_cache(128)
    def __call__(self, symbols, freq, fields=None, start=None, end=None, length=None, adjust=None):
        handler, db = self.get(freq)
        fields = self.indexer(fields)

        if isinstance(symbols, six.string_types):
            return self.read(handler, db, symbols, fields, start, end, length, adjust)
        else:
            return pd.Panel.from_dict(
                {symbol: self.read(handler, db, symbol, fields, start, end, length, adjust) for symbol in symbols}
            )

        # data = handler.read(symbols, db, start=start, end=end, length=length, projection=fields)
        # if not adjust or (len(data) == 0):
        #     return data
        # else:
        #     price = filter(lambda p: p in self.PRICE, fields)
        #     if isinstance(data, pd.DataFrame):
        #         adjust_table = self.read_adjust(
        #             data.index[0], data.index[-1],
        #             self.indexer(symbols), adjust == 'after')
        #         for name, item in adjust_table.iteritems():
        #             data = adjust_candle(data, item, price)
        #         return data
        #     elif isinstance(data, pd.Panel):
        #         adjust_table = self.read_adjust(
        #             data.major_axis[0], data.major_axis[-1],
        #             self.indexer(data.items), adjust == 'after')
        #         for name, item in adjust_table.iteritems():
        #             data[name] = adjust_candle(data[name], item, price)
        #         return data
        #     else:
        #         return data

    def set(self, handler, **freq):
        for f, db in freq.items():
            self.freq[f] = (handler, db)

    def get(self, freq):
        return self.freq[freq]


if __name__ == '__main__':
    from fxdayu_data import MongoHandler
    from fxdayu_data.data_api.adjust import Adjust

    handler = MongoHandler.params(host='192.168.0.102')

    candle = Candle()
    candle.set(handler, H="Stock_H")
    candle.set_adjust(Adjust.db(handler.client['adjust']))

    print candle(("000001.XSHE", "000002.XSHE"), "H", length=50, adjust=True)