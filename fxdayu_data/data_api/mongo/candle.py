# encoding:utf-8
from fxdayu_data.data_api.basic.candle import BasicCandle, normalize
import six
import pandas as pd

from fxdayu_data.data_api import lru_cache


def adjust_candle(frame, adjust_factor, price):
        factor = pd.Series(adjust_factor, frame.index).bfill()
        for name in price:
            frame[name] = frame[name] * factor
        return frame


def replace(old, new):
    return lambda code: code.replace(old, new)


class Candle(BasicCandle):

    def __init__(self):
        self.freq = {}
        self.adjust = None

    def set_adjust(self, adjust):
        self.adjust = adjust

    def read(self, handler, db, symbol, fields, start, end, length, adjust):
        data = handler.read(symbol, start=start, end=end, length=length)

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
    def reads(self, symbols, freq, fields=None, start=None, end=None, length=None, adjust=None, fill=False):
        handler, db = self.get(freq)

        if isinstance(symbols, six.string_types):
            return self.read(handler, db, symbols, fields, start, end, length, adjust)
        else:
            return pd.Panel.from_dict(
                {symbol: self.read(handler, db, symbol, fields, start, end, length, adjust) for symbol in symbols}
            )

    def __call__(self, symbols, freq, fields=None, start=None, end=None, length=None, adjust=None):
        return self.reads(*normalize(symbols, freq, fields, start, end, length, adjust))

    def set(self, handler, **freq):
        for f, db in list(freq.items()):
            self.freq[f] = (handler, db)

    def get(self, freq):
        return self.freq[freq]
