from fxdayu_data.data_api import *
from fxdayu_data import MongoHandler


handler = MongoHandler()


candle = Candle()
candle.set(handler, M1="stock_1min", H="Stock_H", D="Stock_D")
candle.set_adjust(handler.client['adjust'])


factor = Factor()
factor.set(handler, "factor")