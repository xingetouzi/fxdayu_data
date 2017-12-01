# encoding:utf-8
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo import UpdateOne
from functools import partial
import six
from collections import Iterable
import pandas as pd


__all__ = ["write", "inplace", "update", "read", "reads"]


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
        if isinstance(origin, six.string_types):
            return index, origin
        elif isinstance(origin, Iterable):
            s = set(origin)
            s.add(index)
            return tuple(s)
        else:
            return default
    return indexer


def default_type(*types, **kw_types):
    def wrapper(func):
        def wrapped(*args, **kwargs):
            for obj, cls in zip(args, types):
                if not isinstance(obj, cls):
                    raise TypeError("Expected %s, got %s" % (cls, type(obj)))
            for key, cls in list(kw_types.items()):
                try:
                    obj = kwargs[key]
                except KeyError:
                    continue

                if not isinstance(obj, cls):
                    raise TypeError("Expected %s for %s, got %s" % (cls, key, type(obj)))

            return func(*args, **kwargs)
        return wrapped
    return wrapper


def normalize(data, index=None):
    if isinstance(data, pd.DataFrame):
        if index and (index not in data.columns):
            data[index] = data.index
        return [doc[1].to_dict() for doc in data.iterrows()]
    elif isinstance(data, dict):
        key, value = list(map(lambda *args: args, *list(data.items())))
        return list(map(lambda *args: dict(list(map(lambda x, y: (x, y), key, args))), *value))
    elif isinstance(data, pd.Series):
        if data.name is None:
            raise ValueError('name of series: data is None')
        name = data.name
        if index is not None:
            return list(map(lambda k, v: {index: k, name: v}, data.index, data))
        else:
            return list([{data.name: v} for v in data])
    else:
        return data


def read(collection, index='datetime', start=None, end=None, length=None, **kwargs):
    if isinstance(collection, Collection):
        if index:
            kwargs = create_filter(index, start, end, length, kwargs)

            if 'sort' not in kwargs:
                kwargs['sort'] = [(index, 1)]

            projection = kwargs.get("projection", None)
            if projection:
                projection = set(projection)
                projection.add(index)
                kwargs["projection"] = dict.fromkeys(projection, 1)

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

    else:
        raise TypeError("Type of db should be %s not %s" % (Collection, type(collection)))


def reads(db, names=None, index='datetime', start=None, end=None, length=None, **kwargs):
    if isinstance(db, Database):
        if names:
            return pd.Panel.from_dict(
                {name: read(db[name], index, start, end, length, **kwargs) for name in names}
            )
        else:
            return pd.Panel.from_dict(
                {name: read(db[name], index, start, end, length, **kwargs) for name in db.collection_names()}
            )
    else:
        raise TypeError("Type of db should be %s not %s" % (Database, type(db)))


def write(collection, data, index=None):
    data = normalize(data, index)
    result = collection.insert_many(data)
    return result


def create_update(up, index, how, upsert):
    return UpdateOne({index: up[index]}, {how: up}, upsert)


def update(collection, data, index="datetime", how='$set', upsert=True):
    if isinstance(collection, Collection):
        data = normalize(data, index)
        result = collection.bulk_write(
            list(map(partial(create_update, index=index, how=how, upsert=upsert), data))
        )
        return result
    else:
        raise TypeError("Type of db should be %s not %s" % (Collection, type(collection)))


def inplace(collection, data, index='datetime'):
    if isinstance(collection, Collection):
        data = normalize(data, index)

        collection.delete_many({index: {'$gte': data[0][index], '$lte': data[-1][index]}})
        collection.insert_many(data)
        return {'collection': collection.name, 'start': data[0], 'end': data[-1]}
    else:
        raise TypeError("Type of db should be %s not %s" % (Collection, type(collection)))
