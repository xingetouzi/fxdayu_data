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
        return read(self.db["classification"], None,
                    filter=create_filter(code=code, classification=classification))

    def factor_description(self, name=None, classification=None):
        return read(self.db["factor_description"], None,
                    filter=create_filter(name=name, classification=classification))


def create_filter(**kwargs):
    conditions = list(chain(*[iter_pairs(key, value) for key, value in kwargs.items()]))
    if len(conditions) == 0:
        return {}
    elif len(conditions) == 1:
        return conditions[0]
    else:
        return {"$or": conditions}


def iter_pairs(name, value):
    if isinstance(value, six.string_types):
        yield {name: value}
    elif isinstance(value, Iterable):
        for v in value:
            yield {name: v}
    else:
        return
