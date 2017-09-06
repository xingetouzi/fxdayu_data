# encoding:utf-8
from fxdayu_data.costume.basic import Costume
import pickle
import six
import os


class FileCostume(Costume):

    def __init__(self, root=""):
        self.root = root
        self._target = None

    def absolute(self, key):
        return os.path.join(self.root, key)

    def set(self, key, value):
        if isinstance(value, six.string_types):
            with open(self.absolute(key), "w") as f:
                f.write(value)
        else:
            with open(self.absolute(key), "wb") as f:
                pickle.dump(value, f)

    def get(self, key):
        try:
            with open(self.absolute(key), "rb") as f:
                return pickle.load(f)
        except:
            with open(self.absolute(key), "r") as f:
                return f.read()

    @property
    def target(self):
        if self._target:
            return self._target
        else:
            from fxdayu_data.costume.file.FileTarget import FileTarget
            self._target = FileTarget(self.absolute("target.csv"))
            return self._target


def create(general, external=None):
    return FileCostume(general)