from fxdayu_data.data_api.basic.factor import BasicFactor
from fxdayu_data.data_api import lru_cache
import pandas as pd
import six


class BLPFactor(BasicFactor):

    def __init__(self, table):
        self.table = table

    @classmethod
    def dir(cls, rootdir):
        from fxdayu_data.data_api.bundle.blp_reader import FactorReader
        return cls(FactorReader(rootdir))

    @lru_cache(128)
    def read(self, symbols, fields=None, start=None, end=None, length=None):
        return self.table.read(symbols, fields, start, end, length)

    def __call__(self, symbols, fields=None, start=None, end=None, length=None):
        if not isinstance(symbols, six.string_types):
            symbols = tuple(symbols)

        if isinstance(fields, six.string_types):
            fields = (fields,)
        elif fields is not None:
            fields = tuple(fields)

        start = pd.to_datetime(start) if isinstance(start, six.string_types) else start
        end = pd.to_datetime(end) if isinstance(end, six.string_types) else end

        return self.read(symbols, fields, start, end, length)
