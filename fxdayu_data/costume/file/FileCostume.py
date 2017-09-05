from fxdayu_data.costume.basic import BasicCostume
import pickle
import six
import os


class FileCostume(BasicCostume):

    def __init__(self, root=""):
        self.root = ""
        self._target = None

    def absolute(self, key):
        return os.path.join(self.root, key)

    def set(self, key, value):
        if isinstance(value, six.string_types):
            with open(key, "w") as f:
                f.write(value)
        else:
            with open(key, "w") as f:
                pickle.dump(value, f)

    def get(self, key):
        with open(key, "f") as f:
            try:
                return pickle.loads(f)
            except:
                return f.read()

    @property
    def target(self):
        if self._target:
            return self._target
        else:
            from fxdayu_data.costume.file.FileTarget import FileTarget
            self._target = FileTarget(self.absolute("target.csv"))
            return self._target


def create(root):
    return FileCostume(root)