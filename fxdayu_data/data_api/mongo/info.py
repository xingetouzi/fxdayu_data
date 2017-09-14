from fxdayu_data.data_api.basic.info import BasicInfo

KEY = "key"
VALUE = "value"


class MongoInfo(BasicInfo):

    def __init__(self, db=None):
        self.db = db

    def set(self, db):
        self.db = db

    def codes(self, name):
        return tuple(self.db['codes'].find_one({KEY: name})[VALUE])
