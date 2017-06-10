# encoding:utf-8
from fxdayu_data.data.data_api import BasicConfig



class Candle(BasicConfig):

    COLUMNS = ['datetime', 'open', 'high', 'low', 'close', 'volume']

    def __init__(self):
        self.freq = {}

    def __call__(self, symbols, freq, fields=None, start=None, end=None, length=None):
        handler, db = self.get(freq)
        if fields is None:
            fields = self.COLUMNS
        elif isinstance(fields, str):
            fields = ['datetime', fields]
        elif 'datetime' not in fields:
            fields = ['datetime'].extend(fields)

        return handler.read(symbols, db, start=start, end=end, length=length, projection=fields)

    def set(self, handler, **freq):
        for f, db in freq.items():
            self.freq[f] = (handler, db)

    def get(self, freq):
        return self.freq[freq]

