from fxdayu_data.data_api.basic import BasicConfig
from fxdayu_data.handler.mongo_handler import ensure_index
import pandas as pd
import six


field = ensure_index(
    "datetime",
    ("datetime", "open", "high", "low", "close", "volume")
)


def normalize(symbols, freq, fields, start, end, length, adjust):
    return (symbols if isinstance(symbols, six.string_types) else tuple(symbols),
            freq,
            field(fields),
            pd.to_datetime(start) if isinstance(start, six.string_types) else start,
            pd.to_datetime(end) if isinstance(end, six.string_types) else end,
            length,
            adjust)


class BasicCandle(BasicConfig):

    def set(self, *args, **kwargs):
        pass

    def set_adjust(self, *args, **kwargs):
        pass

    def __call__(self, symbols, freq, fields=None, start=None, end=None, length=None, adjust=None):
        pass
