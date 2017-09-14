# encoding:utf-8

MONGOCONFIG = """# encoding:utf-8
from pymongo import MongoClient
from fxdayu_data.data_api import mongo
client = MongoClient()
candle = mongo.candle(client, "adjust", H="Stock_H", D="Stock_D")
factor = mongo.factor(client, "factor")
info = mongo.info(client, "info")
"""

BUNDLECONFIG = """# encoding:utf-8
from fxdayu_data.data_api import bundle
import os
home = "/"
candle = bundle.candle(
    os.path.join(home, "ex_cum_factor.bcolz"),
    D=os.path.join(home, "Stock_D.bcolz"),
    H=os.path.join(home, "Stock_H1.bcolz")
)
factor = bundle.factor(os.path.join(home, "factors.bcolz"))
info = bundle.info(os.path.join(home, "info"))
"""

defaults = {
    "mongo": MONGOCONFIG,
    "bundle": BUNDLECONFIG
}