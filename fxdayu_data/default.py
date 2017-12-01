# encoding:utf-8

MONGOCONFIG = """# encoding:utf-8
from pymongo import MongoClient
from fxdayu_data.data_api import mongo

client = MongoClient()

bonus = mongo.bonus(client, "bonus")

candle = mongo.candle(client, bonus, H="Stock_H", D="Stock_D")

factor = mongo.factor(client, "factor")

info = mongo.info(client, "info")
"""

BUNDLECONFIG = """# encoding:utf-8
from fxdayu_data.data_api.bundle import default
import os

ROOT = os.path.split(globals()["FILE"])[0]

bonus = default.bonus(os.path.join(ROOT, "bonus.bcolz"))

candle = default.candle(bonus, D=os.path.join(ROOT, "Stock_D.bcolz"))

factor = default.factor(os.path.join(ROOT, "factor.bcolz"))

info = default.info(os.path.join(ROOT, "info"))
"""

defaults = {
    "mongo": MONGOCONFIG,
    "bundle": BUNDLECONFIG
}