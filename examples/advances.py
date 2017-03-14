from fxdayu_data.data import MongoHandler
from fxdayu_data.analysis.statistic import count_advances, frame_map, advances
from datetime import datetime
import pandas as pd
import talib
import numpy as np


mh = MongoHandler(db='HS')


def mapper(frame):
    close = pd.DataFrame(frame['close'].dropna(), dtype=np.float64)
    frame['ma'] = talib.abstract.MA(close, timeperiod=50)
    return frame


if __name__ == '__main__':
    candles = mh.read(mh.db.collection_names(), start=datetime(2016, 6, 1))
    candles = frame_map(candles, mapper)

    adv = count_advances(candles, 51)
    print(adv)


    # mh.save(adv, 'advances', 'analysis')
    # print mh.read('advances', 'analysis')


