# encoding:utf-8
from fxdayu_data.data.handler.mongo_handler import MongoHandler


class HandlerCache(object):

    def __init__(self, config):
        self.config = config
        self.handlers = {}

    @classmethod
    def read_file(cls, fp):
        import json
        return cls(json.load(fp))

    def get(self, name):
        try:
            return self.handlers[name]
        except KeyError:
            pass
