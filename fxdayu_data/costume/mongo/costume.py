from fxdayu_data.costume.basic import Costume
from fxdayu_data.costume.mongo import KEY, VALUE
from fxdayu_data.costume.mongo.target import MongoTarget


class MongoCostume(Costume):

    def __init__(self, collection):
        self.collection = collection
        self._target = MongoTarget(collection)

    def set(self, key, value):
        return self.collection.update_one({KEY: key}, {"$set": {VALUE: value}}, True)

    def get(self, key):
        return self.collection.find_one({KEY: key})[VALUE]

    @property
    def target(self):
        return self._target


def create(general, external):
    from pymongo import MongoClient
    import json
    try:
        client = MongoClient(**json.loads(general))
    except json.JSONDecodeError:
        client = MongoClient(general)
    db, col = external.split(".")
    return MongoCostume(client[db][col])
