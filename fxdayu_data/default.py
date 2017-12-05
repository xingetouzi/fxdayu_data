# encoding:utf-8
MONGOCONFIG = """# encoding:utf-8
from fxdayu_data.data_api import mongo
globals().update(mongo.template(
    url="mongodb://localhost:27017",
))
"""

BUNDLECONFIG = """# encoding:utf-8
from fxdayu_data.data_api import bundle
import os
globals().update(bundle.template(
    os.path.split(globals()["FILE"])[0]
))
"""

defaults = {
    "mongo": MONGOCONFIG,
    "bundle": BUNDLECONFIG
}