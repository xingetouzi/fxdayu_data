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