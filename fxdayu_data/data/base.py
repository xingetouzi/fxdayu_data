# encoding:utf-8
from datetime import datetime
import pandas as pd
import pymongo


class MongoHandler(object):
    """
    用于方便地在mongodb中存取时间序列数据
    """

    def __init__(self, host='localhost', port=27017, **setting):
        db = setting.pop('db', None)
        users = setting.pop('user', {})
        self.client = pymongo.MongoClient(host, port, **setting)
        self.db = self.client[db] if db else self.client.database_names()[0]

        for db in users:
            self.client[db].authenticate(users[db]['id'], users[db]['password'])

    def __getitem__(self, item):
        return self.client[item]

    def save(self, data, collection, db=None, index=False):
        if isinstance(data, pd.DataFrame):
            if index:
                data['datetime'] = data.index
            data = [doc.to_dict() for index, doc in data.iterrows()]
        db = self.client[db] if db else self.db

        result = db[collection].delete_many(
            {'datetime': {'$gte': data[0]['datetime'], '$lte': data[-1]['datetime']}}
        )

        db[collection].insert(data)
        db[collection].create_index('datetime')
        return [collection, data[0]['datetime'], data[-1]['datetime'], len(data), result.deleted_count]

    @staticmethod
    def _read(collection, index=True, **kwargs):
        data = list(collection.find(**kwargs))

        for key, value in kwargs.get('sort', []):
            if value < 0:
                data.reverse()

        data = pd.DataFrame(data)

        try:
            data.pop('_id')
            if index:
                data.index = data.pop('datetime')
        except KeyError as ke:
            if '_id' in str(ke):
                raise KeyError("_id lost, unable to find required data, please check you data in %s" % collection.full_name)
            elif 'datetime' in str(ke):
                raise KeyError(
                    "data is not in TimeSeries shape, "
                    "please ensure that all documents in %s "
                    "has a key: datetime and with a value of type<datetime>" % collection.full_name)
            else:
                raise ke
        return data

    def read(self, collection, db=None, start=None, end=None, length=None, **kwargs):
        if start:
            fter = {'datetime': {'$gte': start}}
            if end:
                fter['datetime']['$lte'] = end
            elif length:
                kwargs['limit'] = length
            kwargs['filter'] = fter
        elif length:
            kwargs['sort'] = [('datetime', -1)]
            kwargs['limit'] = length
            if end:
                kwargs['filter'] = {'datetime': {'$lte': end}}
        elif end:
            kwargs['filter'] = {'datetime': {'$lte': end}}

        if isinstance(collection, (str, unicode)):
            db = self.client[db] if db else self.db
            return self._read(db[collection], **kwargs)
        elif isinstance(collection, pymongo.collection.Collection):
            return self._read(collection, **kwargs)
        elif isinstance(collection, (list, tuple)):
            db = self.client[db] if db else self.db
            panel = {}
            for col in collection:
                try:
                    if isinstance(col, (str, unicode)):
                        panel[col] = self._read(db[col], **kwargs)
                    elif isinstance(col, pymongo.collection.Collection):
                        panel[col.name] = self._read(col, **kwargs)
                except KeyError as ke:
                    if 'datetime' in str(ke) or '_id' in str(ke):
                        pass
                    else:
                        raise ke
            return pd.Panel.from_dict(panel)


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


class MongoClient(DataHandler):

    def __init__(self, host='localhost', port=27017, users={}, db=None, **kwargs):
        self.client = pymongo.MongoClient(host, port, **kwargs)
        self.db = db
        for db in users:
            self.client[db].authenticate(users[db]['id'], users[db]['password'])

    def _locate(self, collection, db=None):
        if isinstance(collection, pymongo.collection.Collection):
            return collection
        else:
            if isinstance(db, pymongo.database.DataBase):
                pass
            elif db:
                db = self.client[db]
            else:
                db = self.db

            return db[collection]

    def write(self, data, collection, db=None, index=None):
        collection = self._locate(collection, db)

        if isinstance(data, pd.DataFrame):
            if index:
                data[index] = data.index
            data = [doc.to_dict() for doc in data.iterrows()]

        collection.insert_many(data)
        if index:
            collection.create_index(index)

    def read(self, collection, db=None, index='datetime', start=None, end=None, length=None, **kwargs):
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
        elif isinstance(collection, pymongo.collection.Collection):
            return self._read(collection, index, **kwargs)
        elif isinstance(collection, (list, tuple)):
            panel = {}
            for col in collection:
                if isinstance(col, str):
                    panel[col] = self._read(db[col], index, **kwargs)
                elif isinstance(col, pymongo.collection.Collection):
                    panel[col.name] = self._read(col, index, **kwargs)
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
        collection = self._locate(collection, db)

        if isinstance(data, pd.DataFrame):
            if index and (index not in data.columns):
                data[index] = data.index
            data = [doc.to_dict() for doc in data.iterrows()]

        collection.delete_many({index: {'$gte': data[0][index], '$lte': data[-1][index]}})
        collection.insert_many(data)
        collection.create_index(index)

    def update(self, data, collection, db=None, index='datetime'):
        collection = self._locate(collection, db)

        if isinstance(data, pd.DataFrame):
            if index and (index not in data.columns):
                data[index] = data.index
            for name, doc in data.iterrows():
                collection.update_one({index: doc.pop(index)}, doc)
        else:
            for doc in data:
                collection.update_one({index: doc.pop(index)}, doc)

    def delete(self, filter, collection, db=None):
        collection = self._locate(collection, db)
        collection.delete_many(filter)


