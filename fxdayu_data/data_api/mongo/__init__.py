from fxdayu_data.data_api.mongo.candle import Candle
from fxdayu_data.data_api.mongo.factor import Factor
from fxdayu_data.data_api.mongo.adjust import Adjust
from fxdayu_data.data_api.mongo.info import MongoInfo
from fxdayu_data.data_api.mongo.reader import MongoReader


def candle(client, adj, **kwargs):
    from fxdayu_data.data_api.candle import Candle

    return Candle(
        adjust=Adjust(client[adj]) if adj else None,
        **{freq: MongoReader(client[db]) for freq, db in kwargs.items()}
    )


def factor(client, db):
    from fxdayu_data.data_api.factor import Factor

    return Factor(MongoReader(client[db]))


def info(client, db):
    return MongoInfo(client[db])


def bonus(client, db, index='ex_date'):
    from fxdayu_data.data_api.factor import Factor

    return Factor(MongoReader(client[db], index))


__all__ = ["Candle", "Factor", "Adjust", "MongoInfo"]


if __name__ == '__main__':
    from pymongo import MongoClient

    b = bonus(
        MongoClient("192.168.0.101"),
        "bonus"
    )

    print(b("000157.XSHE"))