from fxdayu_data.data.handler import DataHandler
import redis


class RedisHandler(DataHandler):

    def __init__(self, redis_client=None):
        self.client = redis_client if redis else redis.StrictRedis()

    def read(self, key, **kwargs):
        pass

    def save(self, data, key):
        pass

    def inplace(self, data, key):
        pass


