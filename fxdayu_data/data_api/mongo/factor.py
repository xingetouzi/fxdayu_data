# encoding:utf-8
from fxdayu_data.data_api import lru_cache
from fxdayu_data.data_api.basic.factor import BasicFactor, normalize


class Factor(BasicFactor):

    def __init__(self, handler=None, db=None):
        try:
            self.handler = handler[db]
        except:
            self.handler = handler

        self.db = db

    def set(self, handler, db):
        self.handler = handler[db]
        self.db = db

    @lru_cache(128)
    def read(self, symbols, fields=None, start=None, end=None, length=None):
        return self.handler.read(symbols, start=start, end=end, length=length)

    def __call__(self, symbols, fields=None, start=None, end=None, length=None):
        return self.read(*normalize(symbols, fields, start, end, length))
