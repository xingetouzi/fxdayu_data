# encoding:utf-8
import tushare
import pandas as pd
from fxdayu_data.data.decorators import value_wrapper
from datetime import datetime


CANDLE_MAP = {'open': 'first',
              'high': 'max',
              'low': 'min',
              'close': 'last'}


def date_wrap(frame):
    frame.index = pd.to_datetime(frame['date'], format='%Y-%m-%d')
    return frame


def time_wrap(frame):
    today = datetime.today().replace(microsecond=0)

    def trans(time):
        s = time.split(':')
        return today.replace(hour=int(s[0]), minute=int(s[1]), second=int(s[2]))

    frame.index = map(trans, frame['time'])

    return frame


def tick2min(frame):
    resampler = frame.resample('1min', label='right', closed='right')
    result = resampler['price'].agg(CANDLE_MAP)
    result['volume'] = resampler['volume'].sum()
    return result


# 获取当天1min数据(来源tushare:A股)
today_1min = value_wrapper(time_wrap, tick2min)(tushare.get_today_ticks)


get_k_data = value_wrapper(date_wrap)(tushare.get_k_data)
