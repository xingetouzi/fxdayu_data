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
            if "volume" in data.columns:
                volume = data.pop("volume")
                data = self.adjust.cal(symbol, data, adjust)
                data['volume'] = volume
                return data
            else:
                return self.adjust.cal(symbol, data, adjust)
        else:
            return data

    @lru_cache(128)
    def __call__(self, symbols, freq, fields=None, start=None, end=None, length=None, adjust=None, fill=False):
        handler, db = self.get(freq)
        fields = self.indexer(fields)

        if isinstance(symbols, six.string_types):
            return self.read(handler, db, symbols, fields, start, end, length, adjust)
        else:
            return pd.Panel.from_dict(
                {symbol: self.read(handler, db, symbol, fields, start, end, length, adjust) for symbol in symbols}
            )

    def set(self, handler, **freq):
        for f, db in freq.items():
            self.freq[f] = (handler, db)

    def get(self, freq):
        return self.freq[freq]


if __name__ == '__main__':
    print pd.date_range("2016-01-01", "2016-01-31", freq='1min').da
