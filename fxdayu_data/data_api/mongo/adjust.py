from fxdayu_data.data_api.basic.adjust import BasicAdjust
from fxdayu_data.handler.mongo_handler import read


class Adjust(BasicAdjust):

    def __init__(self, db):
        self._db = db
        self.name = "adjust"

    @classmethod
    def db(cls, db):
        return cls(db)

    def set(self, db):
        self._db = db

    def read(self, code):
        return read(self._db[code], index="start")[self.name]
