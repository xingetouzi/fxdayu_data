import os
import importlib
from fxdayu_data.costume.basic import Costume


GENERAL = "COSTUME_GENERAL"
EXTERNAL = "COSTUME_OPTIONAL"
env_name = "COSTUME"
global_name = "costume"
types = {'mongo': "fxdayu_data.costume.mongo.costume"}


costume = Costume()


def create(general="mongo:localhost", external=None):
    general = os.environ.get(GENERAL, general)
    external = os.environ.get(EXTERNAL, external)
    key, params = general.split(":", 1)
    return importlib.import_module(types[key]).create(params, external)


def generate(general="mongo:localhost", external=None):
    globals()[global_name] = create(general, external)
    return costume