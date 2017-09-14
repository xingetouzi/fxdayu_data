from fxdayu_data.data_api.basic import BasicConfig
from fxdayu_data.data_api import lru_cache
from collections import Iterable
import pandas as pd
import six


def field(fields):
    if isinstance(fields, six.string_types):
        return fields,
    elif isinstance(fields, Iterable):
        return tuple(fields)
    else:
        return fields


def normalize(symbols, fields, start, end, length):
    return (symbols if isinstance(symbols, six.string_types) else tuple(symbols),
            field(fields),
            pd.to_datetime(start) if isinstance(start, six.string_types) else start,
            pd.to_datetime(end) if isinstance(end, six.string_types) else end,
            length)


class Factor(BasicConfig):

    def __init__(self, reader):
        self.reader = reader

    def __call__(self, symbols, fields=None, start=None, end=None, length=None):
        return self.read(*normalize(symbols, fields, start, end, length))

    @lru_cache(128)
    def read(self, symbols, fields, start, end, length):
        if not isinstance(symbols, tuple):
            return self.reader.read(symbols, fields, start, end, length)
        else:
            return pd.Panel.from_dict(
                {s: self.reader.read(s, fields, start, end, length) for s in symbols}
            )
