import os
import importlib
from fxdayu_data.costume.basic import BasicCostume, BasicTarget

env_name = "COSTUME"


class Costume(BasicCostume):

    @classmethod
    def create(cls, how="file:"):
        return create(how)


def create(how="file:"):
    how = os.environ.get(env_name, "file:")
    key, params = how.split(":", 1)
    return importlib.import_module(types[key]).create(params)


types = {'file': "fxdayu_data.costume.file.FileCostume"}

