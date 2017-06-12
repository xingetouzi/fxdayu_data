# encoding:utf-8

import numpy
import pandas as pd
from talib import abstract

from fxdayu_data.handler.mongo_handler import MongoHandler


mh = MongoHandler(port=10001, db='HS')


def advances(candles, hl_period=20, index=-1):
    high, low, up, down, ma_up, ma_down = 0, 0, 0, 0, 0, 0
    left = 0

    hl = slice(1+index-hl_period, index+1) if index != -1 else slice(-hl_period, None)

    for name, candle in candles.iteritems():
        # 读取数据
        try:
            if candle.high[hl].max() == candle.iloc[index].high:
                # new_high
                high += 1
            elif candle.low[hl].min() == candle.iloc[index].low:
                # new_low
                low += 1

            if candle.iloc[index].close > candle.iloc[index-1].close:
                # today_up
                up += 1
            elif candle.iloc[index].close < candle.iloc[index-1].close:
                # today_down
                down += 1

            if candle.iloc[index].close > candle.iloc[index].ma:
                # close_today > ma_50
                ma_up += 1
            elif candle.iloc[index].close < candle.iloc[index].ma:
                # close_today < ma_50
                ma_down += 1
            else:
                left += 1

        except AttributeError as ae:
            print ("%s not supported" % name)
            print (ae.message)
        except IndexError:
            print ("length of %s not enough" % name)

    return {'high': high, 'low': low,
            'up': up, 'down': down,
            'ma_up': ma_up, 'ma_down': ma_down,
            'datetime': candles.major_axis[index], 'left': left}


def count_advances(panel, hl_period=20, start=1, end=None):
    result = []
    for i in range(max(start, hl_period), end if end else len(panel.major_axis)):
        c = advances(panel, hl_period, index=i)
        result.append(c)
        print(c)

    return pd.DataFrame(result).reindex(columns=['datetime', 'high', 'low', 'up', 'down', 'ma_up', 'ma_down', 'left'])


def frame_map(frame, mapper):
    if isinstance(frame, pd.DataFrame):
        return mapper(frame)
    elif isinstance(frame, pd.Panel):
        mapped = {}
        for item, df in frame.iteritems():
            mapped[item] = mapper(df)
        return pd.Panel.from_dict(mapped)


def set_ma(frame):
        close = pd.DataFrame(frame['close'].dropna(), dtype=numpy.float64)
        frame['ma'] = abstract.MA(close, timeperiod=50)
        return frame

