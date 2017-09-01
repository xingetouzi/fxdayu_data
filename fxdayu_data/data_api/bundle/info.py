from fxdayu_data.data_api.basic.info import BasicInfo
import json
import os


class Info(BasicInfo):

    def __init__(self, root=""):
        self.root = root
        self._codes = None

    def set(self, root):
        self.root = root

    def codes(self, name):
        if self._codes:
            return self._codes[name]
        else:
            self._codes = json.load(open(os.path.join(self.root, "codes.json")))
            return self._codes[name]

