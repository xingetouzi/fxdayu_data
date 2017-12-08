from fxdayu_data.data_api.mongo.adjust import Adjust
from fxdayu_data.data_api.mongo.info import MongoInfo
from fxdayu_data.data_api.mongo.reader import MongoReader


def candle(client, adj, **kwargs):
    from fxdayu_data.data_api.candle import Candle

    return Candle(
        adjust=Adjust(client[adj]) if isinstance(adj, str) else adj,
        **{freq: MongoReader(client[db]) for freq, db in kwargs.items()}
    )


def factor(client, db):
    from fxdayu_data.data_api.factor import Factor

    return Factor(MongoReader(client[db]))


def info(client, db):
    return MongoInfo(client[db])


def bonus(client, db, index='ex_date', adjust="ex_cum_factor"):
    from fxdayu_data.data_api.bonus import Bonus

    return Bonus(MongoReader(client[db], index), adjust)


def template(url, **db):
    from pymongo import MongoClient

    client = MongoClient(url)
    db.setdefault("D", "Stock_D")

    _bonus = bonus(client, db.pop("bonus", "bonus"))
    _factor = factor(client, db.pop("factor", "factor"))
    _info = MongoInfo(client[db.pop("info", "info")])
    _candle = candle(client, _bonus, **db)

    return {
        "candle": _candle,
        "bonus": _bonus,
        "factor": _factor,
        "info": _info
    }


__all__ = ["candle", "factor", "adjust", "info", "template"]
