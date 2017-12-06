from fxdayu_data.data_api import lru_cache
import pandas as pd
from collections import Iterable
import six


FIELDS = ("open", "high", "low", "close", "volume")


def field(fields):
    if isinstance(fields, six.string_types):
        return (fields,)
    elif isinstance(fields, Iterable):
        return tuple(fields)
    else:
        return FIELDS


def convert_time(time):
    return pd.to_datetime(time) if isinstance(time, six.string_types) else time


def normalize(symbols, freq, fields, start, end, length, adjust):
    return (symbols if isinstance(symbols, six.string_types) else tuple(symbols),
            freq,
            field(fields),
            convert_time(start),
            convert_time(end),
            length,
            adjust)


class Candle(object):

    def __init__(self, adjust=None, **frequencies):
        self.frequencies = frequencies
        self.adjust = adjust

    def set(self, *args, **kwargs):
        self.frequencies.update(dict(args))
        self.frequencies.update(kwargs)

    def set_adjust(self, adjust):
        self.adjust = adjust

    def __call__(self, symbols, freq="D", fields=None, start=None, end=None, length=None, adjust=None):
        return self.read(*normalize(symbols, freq, fields, start, end, length, adjust))

    @lru_cache(128)
    def read(self, symbols, freq, fields, start, end, length, adjust):
        reader = self.frequencies[freq]
        if not isinstance(symbols, tuple):
            return self._read(reader, symbols, fields, start, end, length, adjust)
        else:
            return pd.Panel.from_dict(
                {symbol: self._read(reader, symbol, fields, start, end, length, adjust) for symbol in symbols}
            )

    def _read(self, reader, symbol, fields, start, end, length, adjust):
        if not adjust:
            return reader.read(symbol, fields, start, end, length).round(2)
        else:
            return self.adjust.cal(symbol, reader.read(symbol, fields, start, end, length), adjust).round(2)
