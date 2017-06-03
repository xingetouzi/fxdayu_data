from fxdayu_data.data.collector.oanda_api import OandaAPI
from fxdayu_data.data.handler.mongo_handler import MongoHandler
from fxdayu_data.data.collector.quests import QuestHandler, ARGS
from itertools import product
from functools import partial


api = OandaAPI(access_token="5e74ab5b965db402fc7f0a883f5e5fa9-bb40c1781c0e7fae3376d5aba35e08ac")
handler = MongoHandler()


def save_history(symbol, granularity, **kwargs):
    db = "Oanda_{}".format(granularity)
    if 'start' in kwargs:
        if 'end' in kwargs:
            for data in api.range_history(symbol, granularity=granularity, **kwargs):
                handler.inplace(data, symbol, db)
                print "Save {} in {} from {} to {}".format(symbol, db, data.index[0], data.index[-1])

            return "Save {} in {} from {} to {}".format(symbol, db, kwargs['start'], kwargs['end'])

        elif "count" in kwargs:
            for data in api.long_history(symbol, granularity=granularity, **kwargs):
                handler.inplace(data, symbol, db)
                print "Save {} in {} from {} to {}".format(symbol, db, data.index[0], data.index[-1])

            return "Save {} in {} from {} count {}".format(symbol, db, kwargs['start'], kwargs['count'])

    data = api.get_history(symbol, granularity=granularity, **kwargs)
    return handler.inplace(data, symbol, db)


def save_histories(symbols, granularities, **kwargs):
    QuestHandler().iter_put(
        partial(save_history, **kwargs),
        product(symbols, granularities),
        ARGS
    ).start()


def save(name, function, symbol, **kwargs):
    db = "Oanda_{}".format(name)
    data = function(symbol, **kwargs)
    handler.inplace(data, symbol, db)
    return "Save {} of {} in {} from {} to {}".format(name, symbol, db, data.index[0], data.index[1])


