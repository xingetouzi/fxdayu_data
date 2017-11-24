from fxdayu_data.data_api.basic.info import BasicInfo
from fxdayu_data.handler.mongo_handler import read
from collections import Iterable
from itertools import chain
import pandas as pd
import six

KEY = "key"
VALUE = "value"


def convert_time(time):
    return pd.to_datetime(time) if isinstance(time, six.string_types) else time


class MongoInfo(BasicInfo):

    def __init__(self, db=None):
        self.db = db

    def set(self, db):
        self.db = db

    def codes(self, name):
        return self.db['codes'].find_one({KEY: name})[VALUE]

    def trade_days(self, start=None, end=None, length=None, is_open=None):
        start = convert_time(start)
        end = convert_time(end)
        if is_open is None:
            f = {}
        else:
            f = {"isOpen": is_open}

        data = read(self.db["trade_days"], start=start, end=end, length=length, filter=f)
        if len(data):
            return data["isOpen"]
        else:
            return data

    def classification(self, code=None, classification=None):
        conditions = list(chain(iter_pairs('code', code), iter_pairs("classification", classification)))
        if len(conditions) == 0:
            _filter = {}
        elif len(conditions) == 1:
            _filter = conditions[0]
        else:
            _filter = {"$or": conditions}
        return read(self.db["classification"], None, filter=_filter)


def iter_pairs(name, value):
    if isinstance(value, six.string_types):
        yield {name: value}
    elif isinstance(value, Iterable):
        for v in value:
            yield {name: v}
    else:
        return