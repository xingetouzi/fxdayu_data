# encoding:utf-8
from data import MongoHandler
from datetime import datetime
import numpy
import pandas as pd
from talib import abstract


mh = MongoHandler(port=10001, db='HS')


def advances(candles, hl_period=20, time=datetime.today()):
    high, low, up, down, ma_up, ma_down = 0, 0, 0, 0, 0, 0

    candles_ = candles[:, :time]

    for name, candle in candles_.iteritems():
        # 读取数据

        try:
            if candle.tail(hl_period).high.max() == candle.iloc[-1].high:
                # new_high
                high += 1
            elif candle.tail(hl_period).low.min() == candle.iloc[-1].low:
                # new_low
                low += 1

            if candle.iloc[-1].close > candle.iloc[-2].close:
                # today_up
                up += 1
            elif candle.iloc[-1].close < candle.iloc[-2].close:
                # today_down
                down += 1

            if candle.iloc[-1].ma > candle.iloc[-2].ma:
                # ma_today > ma_yesterday
                ma_up += 1
            elif candle.iloc[-1].ma < candle.iloc[-2].ma:
                # ma_today < ma_yesterday
                ma_down += 1

        except AttributeError as ae:
            print ("%s not supported" % name)
            print (ae.message)
        except IndexError:
            print ("length of %s not enough" % name)

    return {'high': high, 'low': low,
            'up': up, 'down': down,
            'ma_up': ma_up, 'ma_down': ma_down,
            'datetime': time}


def count_advances(panel):
    result = []
    for dt in panel.major_axis:
        c = advances(panel, time=dt)
        result.append(c)
        print(c)

    return pd.DataFrame(result)


def frame_map(frame, mapper):
    if isinstance(frame, pd.DataFrame):
        return mapper(frame)
    elif isinstance(frame, pd.Panel):
        mapped = {}
        for item, df in frame.iteritems():
            mapped[item] = mapper(df)
        return pd.Panel.from_dict(mapped)


def set_ma(frame):
        close = pd.DataFrame(frame['close'], dtype=numpy.float64)
        frame['ma'] = abstract.MA(close, timeperiod=50)
        return frame


if __name__ == '__main__':
    candles = mh.read(list(mh.db.collection_names()), 'HS', datetime(2016, 1, 1))

    def set_ma(frame):
        close = pd.DataFrame(frame['close'], dtype=numpy.float64)
        frame['ma'] = abstract.MA(close, timeperiod=50)
        return frame

    candles = frame_map(candles, set_ma)
    print(count_advances(candles.iloc[:, 51:]))
