from fxdayu_data.data_api.basic import BasicReader
from fxdayu_data.handler.mongo_handler import read


class MongoReader(BasicReader):

    def __init__(self, db, index="datetime"):
        self.db = db
        self.index = index

    def set(self, db):
        self.db = db

    def read(self, symbol, fields=None, start=None, end=None, length=None):
        return read(self.db[symbol], self.index, start=start, end=end, length=length, projection=fields)
