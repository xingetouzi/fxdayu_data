# encoding:utf-8
from datetime import datetime
from pymongo.mongo_client import database
import pandas as pd
import pymongo


class DataHandler(object):

    def write(self, *args, **kwargs):
        pass

    def read(self, *args, **kwargs):
        pass

    def inplace(self, *args, **kwargs):
        pass

    def update(self, *args, **kwargs):
        pass

    def delete(self, *args, **kwargs):
        pass


class MongoHandler(DataHandler):

    def __init__(self, host='localhost', port=27017, users={}, db=None, **kwargs):
        self.client = pymongo.MongoClient(host, port, **kwargs)
        self.db = self.client[db] if db else None
        for db in users:
            self.client[db].authenticate(users[db]['id'], users[db]['password'])

    def _locate(self, collection, db=None):
        if isinstance(collection, database.Collection):
            return collection
        else:
            if db is None:
                return self.db[collection]
            elif isinstance(db, database.Database):
                return db[collection]
            else:
                return self.client[db][collection]

    def write(self, data, collection, db=None, index=None):
        """

        :param data(DataFrame|list(dict)): 要存的数据
        :param collection(str): 表名
        :param db(str): 数据库名
        :param index(str): 以index值建索引, None不建索引
        :return:
        """
        collection = self._locate(collection, db)

        if isinstance(data, pd.DataFrame):
            if index:
                data[index] = data.index
            data = [doc.to_dict() for name, doc in data.iterrows()]

        collection.insert_many(data)
        if index:
            collection.create_index(index)

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
            if start:
                fter = {index: {'$gte': start}}
                if end:
                    fter[index]['$lte'] = end
                elif length:
                    kwargs['limit'] = length
                kwargs['filter'] = fter
            elif length:
                kwargs['sort'] = [(index, -1)]
                kwargs['limit'] = length
                if end:
                    kwargs['filter'] = {index: {'$lte': end}}
            elif end:
                kwargs['filter'] = {index: {'$lte': end}}

        db = self.db if db is None else self.client[db]

        if isinstance(collection, str):
            return self._read(db[collection], index, **kwargs)
        elif isinstance(collection, database.Collection):
            return self._read(collection, index, **kwargs)
        elif isinstance(collection, (list, tuple)):
            panel = {}
            for col in collection:
                try:
                    if isinstance(col, database.Collection):
                        panel[col.name] = self._read(col, index, **kwargs)
                    else:
                        panel[col] = self._read(db[col], index, **kwargs)
                except KeyError as ke:
                    if index in str(ke):
                        pass
                    else:
                        raise ke
            return pd.Panel.from_dict(panel)

    @staticmethod
    def _read(collection, index=None, **kwargs):
        data = list(collection.find(**kwargs))

        for key, value in kwargs.get('sort', []):
            if value < 0:
                data.reverse()

        data = pd.DataFrame(data)

        if index:
            data.index = data[index]

        if len(data):
            data.pop('_id')

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

        if isinstance(data, pd.DataFrame):
            if index not in data.columns:
                data[index] = data.index
            data = [doc.to_dict() for name, doc in data.iterrows()]

        collection.delete_many({index: {'$gte': data[0][index], '$lte': data[-1][index]}})
        collection.insert_many(data)
        collection.create_index(index)

    def update(self, data, collection, db=None, index='datetime', how='$set'):
        collection = self._locate(collection, db)

        if isinstance(data, pd.DataFrame):
            if index in data.columns:
                data.index = data[index]
            for name, doc in data.iterrows():
                collection.update_one({index: name}, {how: doc.to_dict()})
        else:
            for doc in data:
                collection.update_one({index: doc.pop(index)}, doc)

    def delete(self, filter, collection, db=None):
        collection = self._locate(collection, db)
        collection.delete_many(filter)


