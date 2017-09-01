from fxdayu_data.data_api.basic import BasicConfig
from fxdayu_data.handler.mongo_handler import ensure_index
import pandas as pd
import six


field = ensure_index("datetime")


def normalize(symbols, fields, start, end, length):
    return (symbols if isinstance(symbols, six.string_types) else tuple(symbols),
            field(fields),
            pd.to_datetime(start) if isinstance(start, six.string_types) else start,
            pd.to_datetime(end) if isinstance(end, six.string_types) else end,
            length)


class BasicFactor(BasicConfig):

    def __call__(self, symbols, fields=None, start=None, end=None, length=None):
        pass
