from fxdayu_data.costume.basic import BasicTarget
from fxdayu_data.costume.mongo import KEY, VALUE, TYPE
import pandas as pd
import json
from numpy import NaN


class MongoTarget(BasicTarget):

    def __init__(self, collection):
        self.collection = collection
        self.type = "target"

    def get(self, key):
        doc = self.collection.find_one({KEY: key, TYPE: self.type})
        if isinstance(doc, dict):
            return json.loads(doc[VALUE])
        else:
            return doc

    def set(self, key, value):
        return self.collection.update_one(
            {KEY: key, TYPE: self.type},
            {"$set": {VALUE: json.dumps(value), TYPE: self.type}},
            True,
        )

    def inplace(self, frame):
        if isinstance(frame, pd.DataFrame):
            for name, item in frame.iterrows():
                self.set(name, item.replace(0, NaN).dropna().to_dict())