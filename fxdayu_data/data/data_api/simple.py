# encoding:utf-8
from fxdayu_data.data.data_api import BasicConfig


class Simple(BasicConfig):

    def __init__(self):
        self.handler = None

    def set(self, handler, db):
        self.handler = handler[db]

    def get(self):
        return self.handler


class Fundamental(Simple):
    def __call__(self, symbols, fields=None, start=None, end=None, length=None):
        if fields is None:
            return self.handler.read(symbols, start=start, end=end, length=length)
        else:
            if isinstance(fields, str):
                fields = ['datetime', fields]
            elif 'datetime' not in fields:
                fields = ['datetime'].extend(fields)

            return self.handler.read(symbols, start=start, end=end, length=length, projection=fields)