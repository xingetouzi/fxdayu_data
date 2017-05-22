# encoding:utf-8
from fxdayu_data.data.handler.mongo_handler import MongoHandler
from collections import Iterable
import pandas as pd


class CandleReader(object):

    def __init__(self, db_config):
        if isinstance(db_config, (str, unicode)):
            import json
            self.config = json.load(open(db_config))
        else:
            self.config = db_config

        self.handler = MongoHandler(**self.config.get("client"))

    def _read(self, code, db, index='datetime', start=None, end=None, length=None, **kwargs):
        try:
            return self.handler.read(code, db, index, start, end, length, **kwargs)
        except KeyError:
            return pd.DataFrame()

    def _get_db(self, frequency):
        return self.config.get(frequency).get('db')

    def read(self, codes, frequency=None, index='datetime', start=None, end=None, length=None, **kwargs):
        if isinstance(codes, (str, unicode)):
            return self._read(codes, self._get_db(frequency), index, start, end, length, **kwargs)
        elif isinstance(codes, dict):
            return {key: self.read(value, key, index, start, end, length, **kwargs)
                    for key, value in codes.items()}
        elif isinstance(codes, Iterable):
            return {code: self._read(code, self._get_db(frequency), index, start, end, length, **kwargs)
                    for code in codes}
        else:
            return self._read(codes, self._get_db(frequency), index, start, end, length, **kwargs)


if __name__ == '__main__':
    from datetime import datetime

    cr = CandleReader("config.json")
    print cr.read({"min1": ['sh600000', 'sh600036']}, start=datetime(2017, 5, 19))
