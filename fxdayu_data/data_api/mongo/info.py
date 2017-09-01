from fxdayu_data.data_api.basic.info import BasicInfo

KEY = "key"
VALUE = "value"


class MongoInfo(BasicInfo):

    def __init__(self):
        self.db = None

    def set(self, db):
        self.db = db

    def codes(self, name):
        return tuple(self.db['codes'].find_one({KEY: name})[VALUE])
