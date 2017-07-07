# encoding:utf-8
from pymongo.mongo_client import database
from collections import Iterable
import pandas as pd
import pymongo

from fxdayu_data.handler.base import DataHandler

try:
    SINGLE = (str, unicode)
except NameError:
    SINGLE = str


def create_filter(index, start, end, length, kwargs):
    index_range = {}
    if start:
        index_range["$gte"] = start
    else:
        kwargs.setdefault('sort', []).append((index, -1))
    if end:
        index_range["$lte"] = end
    if length:
        kwargs['limit'] = length
    if len(index_range):
        kwargs.setdefault('filter', {})[index] = index_range
    return kwargs


def ensure_index(index, default=None):
    def indexer(origin):
        if isinstance(origin, SINGLE):
            return index, origin
        elif isinstance(origin, Iterable):
            s = set(origin)
            s.add(index)
            return tuple(s)
        else:
            return default
    return indexer


class MongoHandler(DataHandler):

    def __init__(self, host='localhost', port=27017, users=None, db=None, client=None,  **kwargs):
        if client:
            self.client = client
        else:
            self.client = pymongo.MongoClient(host, port, **kwargs)
        self.db = db

        if isinstance(users, dict):
            for db_name, config in users.items():
                self.client[db_name].authenticate(config['name'], config['password'])

    def __getitem__(self, item):
        return MongoHandler(client=self.client, db=item)

    @classmethod
    def read_config(cls, config):
        import json
        return cls.params(**json.load(open(config)))

    @classmethod
    def params(cls, **kwargs):
        from pymongo import MongoClient

        db = kwargs.pop('db', None)
        users = kwargs.pop('users', None)
        return cls(client=MongoClient(**kwargs), db=db, users=users)

    def _locate(self, collection, db=None):
        if isinstance(collection, database.Collection):
            return collection
        else:
            if db is not None:
                if isinstance(db, database.Database):
                    return db[collection]
                else:
                    return self.client[db][collection]
            else:
                return self.client[self.db][collection]

    def write(self, data, collection, db=None, index=None):
        """

        :param data(DataFrame|list(dict)): 要存的数据
        :param collection(str): 表名
        :param db(str): 数据库名
        :param index(str): 以index值建索引, None不建索引
        :return:
        """
        collection = self._locate(collection, db)
        data = self.normalize(data, index)
        collection.insert_many(data)
        if index:
            collection.create_index(index)
        return {'collection': collection.name, 'start': data[0], 'end': data[-1]}

    def read(self, collection, db=None, index='datetime', start=None, end=None, length=None, **kwargs):
        """

        :param collection(str): 表名
        :param db(str): 数据库名
        :param index(str): 读取索引方式
        :param start(datetime):
        :param end(datetime):
        :param length(int):
        :param kwargs:
        :return:
        """

        if index:
            kwargs = create_filter(index, start, end, length, kwargs)

        if isinstance(collection, SINGLE):
            return self._read(collection, db, index, **kwargs)
        if isinstance(collection, Iterable):
            panel = dict(self._reads(collection, db, index, **kwargs))
            return pd.Panel.from_dict(panel)
        else:
            return self._read(collection, db, index, **kwargs)

    def _reads(self, collections, db, index, **kwargs):
        for col in collections:
            frame = self._read(col, db, index, **kwargs)
            if len(frame):
                yield col, frame

    def _read(self, collection, db, index, **kwargs):
        collection = self._locate(collection, db)
        if index:
            if 'sort' not in kwargs:
                kwargs['sort'] = [(index, 1)]
        data = list(collection.find(**kwargs))

        for key, value in kwargs.get('sort', []):
            if value < 0:
                data.reverse()
        data = pd.DataFrame(data)

        if len(data):
            data.pop('_id')
            if index:
                data.index = data.pop(index)

        return data

    def inplace(self, data, collection, db=None, index='datetime'):
        """
        以替换的方式存(存入不重复)

        :param data(DataFrame|list(dict)): 要存的数据
        :param collection(str): 表名
        :param db(str): 数据库名
        :param index(str): 默认以datetime为索引替换
        :return:
        """

        collection = self._locate(collection, db)
        data = self.normalize(data, index)

        collection.delete_many({index: {'$gte': data[0][index], '$lte': data[-1][index]}})
        collection.insert_many(data)
        collection.create_index(index)
        return {'collection': collection.name, 'start': data[0], 'end': data[-1]}

    def update(self, data, collection, db=None, index='datetime', how='$set', **kwargs):
        collection = self._locate(collection, db)

        if isinstance(data, pd.DataFrame):
            if index in data.columns:
                data.index = data[index]
            for name, doc in data.iterrows():
                collection.update_one({index: name}, {how: doc.to_dict()}, **kwargs)
        else:
            for doc in data:
                collection.update_one({index: doc.pop(index)}, doc, **kwargs)

    def delete(self, filter, collection, db=None):
        collection = self._locate(collection, db)
        collection.delete_many(filter)

    def normalize(self, data, index=None):
        if isinstance(data, pd.DataFrame):
            if index and (index not in data.columns):
                data[index] = data.index
            return [doc[1].to_dict() for doc in data.iterrows()]
        elif isinstance(data, dict):
            key, value = list(map(lambda *args: args, *data.items()))
            return list(map(lambda *args: dict(map(lambda x, y: (x, y), key, args)), *value))
        elif isinstance(data, pd.Series):
            if data.name is None:
                raise ValueError('name of series: data is None')
            name = data.name
            if index is not None:
                return list(map(lambda k, v: {index: k, name: v}, data.index, data))
            else:
                return list(map(lambda v: {data.name: v}, data))
        else:
            return data

    def table_names(self, db=None):
        if not db:
            return self.client[self.db].collection_names()
        else:
            return self.client[db].collection_names()