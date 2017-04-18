# encoding:utf-8
import tushare
import pandas as pd
import numpy as np
from fxdayu_data.data.decorators import value_wrapper
from datetime import datetime, time, date, timedelta
import requests
import re


CANDLE_MAP = {'open': 'first',
              'high': 'max',
              'low': 'min',
              'close': 'last'}


HEADERS = {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, sdch",
    "Accept-Language": "zh-CN,zh;q=0.8",
    "Connection": "keep-alive",
    "Host": "hq.sinajs.cn",
    "Referer": "http://finance.sina.com.cn/data/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36}"}


TICK_URL = "http://vip.stock.finance.sina.com.cn/quotes_service/view/CN_TransListV2.php?"
TICK_PARAMS = "symbol={symbol}&num={num}"
TICK_FORMAT = """new Array\('(.*?)', '(.*?)', '(.*?)', '(.*?)'\);"""
TODAY = date.today()
TICK_COLUNM = ['datetime', 'volume', 'price', 'direction']


def time2datetime(t):
    return datetime.combine(TODAY, time(*map(int, t.split(':'))))


def tick_mapper(args):
    return [time2datetime(args[0]),
            int(args[1]),
            float(args[2]),
            args[3]]


def code_mapper(code):
    if code.startswith('6'):
        return 'sh%s' % code
    elif code.startswith('0') or code.startswith('3'):
        return 'sz%s' % code
    else:
        return code


def get_tick_url(**kwargs):
    kwargs['symbol'] = code_mapper(kwargs.get('symbol', ''))
    return TICK_URL + '&'.join(["%s=%s" % item for item in kwargs.items()])


@value_wrapper(lambda text: re.findall(TICK_FORMAT, text, re.S),
               lambda args: map(tick_mapper, reversed(args)),
               lambda ticks: pd.DataFrame(ticks, columns=TICK_COLUNM).set_index('datetime'))
def get_tick(code, session=None, **kwargs):
    url = get_tick_url(symbol=code, **kwargs)
    if not session:
        session = session if session else requests.Session()
        session.headers = HEADERS
    request = requests.Request('get', url)
    response = session.send(request.prepare())
    return response.text


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


class TimeRange(object):

    def __init__(self, t, gap=timedelta(minutes=1)):
        self.gap = gap
        self.tail = t
        self.head = t.replace(second=0) if t.second else t - gap

    def roll(self, t):
        if t <= self.tail and (t > self.head):
            return self.tail
        elif t <= self.head:
            self.head = t.replace(second=0) if t.second else t - self.gap
            self.tail = self.head + self.gap
            return self.tail
        else:
            return t

    @classmethod
    def create_index(cls, index):
        tr = cls(index[-1])
        return pd.DatetimeIndex(reversed(map(tr.roll, reversed(index))))


def tick2min_group(frame):
    if isinstance(frame, pd.DataFrame):
        grouper = frame.groupby(TimeRange.create_index)
        result = grouper['price'].agg(CANDLE_MAP)
        result['volume'] = grouper['volume'].sum()
        return result


# 获取当天1min数据
today_1min = value_wrapper(tick2min_group, lambda f: f.dropna())(get_tick)

