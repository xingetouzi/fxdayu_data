# encoding:utf-8
from fxdayu_data.data_api import BasicConfig


KEY = "key"
VALUE = "value"


class BasicInfo(BasicConfig):

    def codes(self, name):
        pass


class MongoInfo(BasicInfo):

    def __init__(self):
        self.db = None

    def set(self, db):
        self.db = db

    def codes(self, name):
        return tuple(self.db['codes'].find_one({KEY: name})[VALUE])



def unfold(code):
    code = "%06.f" % code
    if code.startswith("6"):
        return code + '.XSHG'
    else:
        return code + '.XSHE'


if __name__ == '__main__':
    from pymongo import MongoClient

    client = MongoClient(port=37017)
    # col = client['info']['codes']

    info = MongoInfo()
    info.set(client['info'])
    print info.codes('hs300')