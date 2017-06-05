from fxdayu_data.data.collector.oanda_api import OandaAPI
from fxdayu_data.data.handler.mongo_handler import MongoHandler
from inspect import isgeneratorfunction
from fxdayu_data.data.collector.quests import QuestHandler, ARGS
from itertools import product
from functools import partial


def save_history(handler, function, symbol, granularity, **kwargs):
    db = "Oanda_{}".format(granularity)
    if isgeneratorfunction(function):
        for data in function(symbol, granularity=granularity, **kwargs):
            handler.inplace(data, symbol, db)
            print "Save {} in {} from {} to {}".format(symbol, db, data.index[0], data.index[-1])
    else:
        data = function(symbol, granularity=granularity, **kwargs)
        handler.inplace(data, symbol, db)


def save_histories(handler, function, symbols, granularities, **kwargs):
    QuestHandler().iter_put(
        partial(save_history, handler, function, **kwargs),
        product(symbols, granularities),
        ARGS
    ).start()


def save(handler, name, function, symbol, **kwargs):
    db = "Oanda_{}".format(name)
    data = function(symbol, **kwargs)
    handler.inplace(data, symbol, db)
    return "Save {} of {} in {} from {} to {}".format(name, symbol, db, data.index[0], data.index[1])


def save_many(handler, symbols, t=5, **functions):
    qh = QuestHandler()
    for name, function in functions.items():
        qh.iter_put(partial(save, handler, name, function), symbols)
    qh.start(t)
