# encoding:utf-8
from collections import Iterable

import pandas as pd

from fxdayu_data.data_api import BasicConfig, lru_cache
from fxdayu_data.handler.mongo_handler import ensure_index


try:
    SINGLE = (str, unicode)
except Exception:
    SINGLE = str


def adjust_candle(frame, adjust_factor):
        factor = pd.Series(adjust_factor, frame.index).bfill()
        for c in ['open', 'high', 'close', 'low']:
            frame[c] = frame[c] * factor
        return frame


class Candle(BasicConfig):

    COLUMNS = ['datetime', 'open', 'high', 'low', 'close', 'volume']

    def __init__(self):
        self.freq = {}
        self.indexer = ensure_index('datetime', self.COLUMNS)
        self.adjust = None

    def set_adjust(self, handler, db, collection):
        from functools import partial
        self.adjust = partial(handler.read, collection=collection, db=db)

    @lru_cache(128)
    def __call__(self, symbols, freq, fields=None, start=None, end=None, length=None, adjust=None):
        handler, db = self.get(freq)
        fields = self.indexer(fields)

        if not adjust:
            return handler.read(symbols, db, start=start, end=end, length=length, projection=fields)
        else:
            adj_frame = self.adjust(start=start, end=end, length=length, projection=self.indexer(symbols))
            if adjust == "former":
                for name, item in adj_frame.iteritems():
                    adj_frame[name] = item[-1] / item

            if isinstance(symbols, SINGLE):
                data = handler.read(symbols, db, start=start, end=end, length=length, projection=fields)
                return adjust_candle(data, adj_frame.iloc[:, 0])
            elif isinstance(symbols, Iterable):
                dct = {s: handler.read(s, db, start=start, end=end, length=length, projection=fields) for s in symbols}
                for key, value in dct.items():
                    dct[key] = adjust_candle(value, adj_frame[key])
                return pd.Panel.from_dict(dct)
            else:
                data = handler.read(symbols, db, start=start, end=end, length=length, projection=fields)
                return adjust_candle(data, adj_frame.iloc[:, 0])

    def set(self, handler, **freq):
        for f, db in freq.items():
            self.freq[f] = (handler, db)

    def get(self, freq):
        return self.freq[freq]