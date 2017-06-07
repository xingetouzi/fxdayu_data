try:
    from fxdayu_data.data.handler.mongo_handler import MongoHandler
except ImportError:
    pass

try:
    from fxdayu_data.data.handler.redis_handler import RedisHandler
except ImportError:
    pass

try:
    from fxdayu_data.data.collector.oanda_api import OandaAPI
except ImportError:
    pass

import fxdayu_data.data.collector.sina_tick as sina_tick
from fxdayu_data.data.collector.quests import QuestHandler, Quest

__all__ = ['MongoHandler', "RedisHandler", "OandaAPI", "sina_tick", "Quest", "QuestHandler"]