from fxdayu_data.data_api.basic.adjust import BasicAdjust
from fxdayu_data.handler.mongo_handler import read


class Adjust(BasicAdjust):

    def __init__(self, db, index="start", name="adjust"):
        self._db = db
        self.name = name
        self.index = index

    @classmethod
    def db(cls, db):
        return cls(db)

    def set(self, db):
        self._db = db

    def read(self, code):
        return read(self._db[code], index=self.index, projection=[self.index, self.name])[self.name]
