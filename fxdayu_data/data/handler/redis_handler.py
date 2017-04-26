from fxdayu_data.data.handler.base import DataHandler
from collections import Iterable
from datetime import datetime
from numpy import float64
import pandas as pd
import redis


class RedisHandler(DataHandler):

    def __init__(self, redis_client=None, transformer=None, **kwargs):
        self.client = redis_client if redis_client else redis.StrictRedis(**kwargs)
        self.transformer = {
            'datetime': lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"),
            'close': float64,
            'open': float64,
            'high': float64,
            'low': float64,
            'volume': float64
        } if transformer is None else transformer

        self.fields = self.transformer.keys()
        self.pubsub = self.client.pubsub()

    def trans(self, key, sequence):
        try:
            trans = self.transformer[key]
        except KeyError:
            return sequence

        if isinstance(sequence, str):
            return trans(sequence)
        elif isinstance(sequence, Iterable):
            return map(trans, sequence)
        else:
            return trans(sequence)

    @staticmethod
    def join(*args):
        return ':'.join(args)

    def read(self, name, index='datetime', start=None, end=None, length=None, fields=None):
        if not fields:
            fields = list(self.fields)
            fields.remove(index)
        loc, main_index = self._read_index(self.join(name, index), self.transformer[index], start, end, length)

        return pd.DataFrame(self.locate_read(name, loc, fields), self.trans(index, main_index))

    @staticmethod
    def search_sorted(index, key, transform, reverse=False):
        if reverse:
            count = len(index)
            for i in reversed(index):
                count -= 1
                if key >= transform(i):
                    return count
            return count
        else:
            count = 0
            for i in index:
                if key <= transform(i):
                    return count
                count += 1
            return count

    def _read_index(self, key_index, transform, start=None, end=None, length=None):
        index = self.client.lrange(key_index, 0, -1)
        if start or end:
            if start:
                s = self.search_sorted(index, start, transform)
                if end:
                    e = self.search_sorted(index, end, transform, True)
                    return [s, e], index[s:e+1]
                elif length:
                    return [s, s+length-1], index[s:s+length]
                else:
                    return [s, -1], index[s:]
            else:
                e = self.search_sorted(index, end, transform, True)
                if length:
                    s = e-length+1
                    if s < 0:
                        s = 0
                    return [s, e], index[s:e+1]
                else:
                    return [0, e], index[0:e+1]
        else:
            if length:
                return [-length, -1], index[-length:]
            else:
                return [0, -1], index

    def write(self, data, name, index='datetime', pipeline=None):
        execute = False
        if pipeline is None:
            pipeline = self.client.pipeline()
            execute = True

        if isinstance(data, pd.DataFrame):
            if index in data.columns:
                pipeline.rpush(self.join(name, index), *data[index])
            else:
                pipeline.rpush(self.join(name, index), *data.index)
            for key, item in data.iteritems():
                pipeline.rpush(self.join(name, key), *item)
        elif isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, (str, int, float, unicode)):
                    pipeline.rpush(self.join(name, key), value)
                elif isinstance(value, Iterable):
                    pipeline.rpush(self.join(name, key), *value)
                else:
                    pipeline.rpush(self.join(name, key), value)
        else:
            return pipeline

        if execute:
            return pipeline.execute()
        else:
            return pipeline

    def inplace(self, data, name, index='datetime', pipeline=None):
        if isinstance(data, pd.DataFrame):
            pipeline = self.client.pipeline()

            return pipeline.execute()

    def update(self, data, name, index='datetime', pipeline=None):
        l_range = self.client.lrange(self.join(name, index), 0, -1)
        trans = self.transformer[index]
        if isinstance(data, dict):
            index_value = data.pop(index)
            loc = self.search_sorted(l_range, index_value, trans)
            if trans(l_range[loc]) == index_value:
                return self.locate_update(data, name, loc, pipeline)
        elif isinstance(data, pd.DataFrame):
            if index in data:
                data.index = data[index]

            execute = False
            if pipeline is None:
                pipeline = self.client.pipeline()
                execute = True
            for index_value, rows in data.iterrows():
                loc = self.search_sorted(l_range, index_value, trans)
                if trans(l_range[loc]) == index_value:
                    self.locate_update(rows.to_dict(), name, loc, pipeline)
            if execute:
                return pipeline.execute()
            else:
                return pipeline

    def locate_update(self, data, name, loc=-1, pipeline=None):
        if pipeline is not None:
            for key, value in data.items():
                pipeline.lset(self.join(self.join(name, key)), loc, value)
            return pipeline
        else:
            pipeline = self.client.pipeline()
            for key, value in data.items():
                pipeline.lset(self.join(self.join(name, key)), loc, value)
            return pipeline.execute()

    def locate_read(self, name, loc, fields=None):
        if fields is None:
            fields = self.fields
        print loc
        if isinstance(loc, int):
            return {f: self.trans(f, self.client.lindex(self.join(name, f), loc)) for f in fields}
        elif isinstance(loc, slice):
            return {f: self.trans(f, self.client.lrange(self.join(name, f), loc.start, loc.stop)) for f in fields}
        elif isinstance(loc, (list, tuple)):
            return {f: self.trans(f, self.client.lrange(self.join(name, f), loc[0], loc[1])) for f in fields}
        else:
            return {f: self.trans(f, self.client.lrange(self.join(name, f), 0, -1)) for f in fields}

    def delete(self, name, fields=None):
        if fields is None:
            fields = self.fields
        return self.client.delete(*map(lambda x: self.join(name, x), fields))

    def subscribe(self, *args, **kwargs):
        self.pubsub.subscribe(*args, **kwargs)

    def listen(self, function):
        for data in self.pubsub.listen():
            function(data['data'])

if __name__ == '__main__':
    import time

    rds = redis.StrictRedis()

    rds.rpush('timelimit', 1)

    # print rds.expireat('timelimit', int(time.time() + 30))