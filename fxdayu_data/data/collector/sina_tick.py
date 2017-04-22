# encoding:utf-8
from fxdayu_data.data.decorators import value_wrapper
from datetime import datetime, date, timedelta, time
from requests import ConnectionError
import pandas as pd
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


TICK_HISTORY = "http://market.finance.sina.com.cn/downxls.php?"
TICK_TODAY = "http://vip.stock.finance.sina.com.cn/quotes_service/view/CN_TransListV2.php?"
TICK_FORMAT = """new Array\('(.*?)', '(.*?)', '(.*?)', '(.*?)'\);"""
HISTORY_FORMAT = "\n%s\n" % "\t".join(['(.*?)']*6)
TODAY = date.today()
TICK_COLUMNS = ['time', 'volume', 'price', 'direction']
HISTORY_TICK_COLUMNS = ['time', 'price', 'change', 'volume', 'amount', 'trend']


def reconnect_wrap(retry=3, wait=0.1, default=None, error=ConnectionError):
    def wrapper(func):
        def reconnect(*args, **kwargs):
            rt = retry
            while rt >= 0:
                try:
                    return func(*args, **kwargs)
                except error:
                    rt -= 1

            try:
                return default()
            except AttributeError:
                return default

        return reconnect
    return wrapper


def time_wrap(frame, dt, column='time'):
    if isinstance(dt, datetime):
        dt = dt.date()

    frame.index = map(lambda t: datetime.combine(dt, str2time(t)), frame[column])

    return frame


def str2time(t):
    return time(*map(int, t.split(':')))


def join_params(**kwargs):
    return '&'.join(['%s=%s' % item for item in kwargs.items()])


def make_url(*args):
    return ''.join(args)


def tick_line_transfer(line):
    line = line.split('\t')
    line[1], line[3] = float(line[1]), float(line[3])
    return line


def item_transfer(**kwargs):
    def transfer(frame):
        for item, function in kwargs.items():
            frame[item] = function(frame[item])
        return frame
    return transfer


def history_text(code, date_):
    url = make_url(TICK_HISTORY, join_params(symbol=code, date=date_.strftime("%Y-%m-%d")))
    response = requests.get(url)
    return response.text


def history_tick(code, date_):
    text = history_text(code, date_)
    tick = pd.DataFrame(re.findall(HISTORY_FORMAT, text, re.S), columns=HISTORY_TICK_COLUMNS)
    tick = item_transfer(price=pd.to_numeric, volume=pd.to_numeric)(tick)
    return time_wrap(tick, date_)


def tick2min(frame):
    try:
        resampler = frame.resample('1min', label='right', closed='right')
        result = resampler['price'].agg(CANDLE_MAP)
        result['volume'] = resampler['volume'].sum()
        return result.dropna()
    except Exception as e:
        print e
        return frame


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
    else:
        return frame


def raw_tick(code, session=None, **kwargs):
    url = make_url(TICK_TODAY, join_params(symbol=code, **kwargs))
    if not session:
        session = session if session else requests.Session()
        session.headers = HEADERS
    request = requests.Request('get', url)
    response = session.send(request.prepare())
    return response.text


def get_tick(code, session=None, **kwargs):
    text = raw_tick(code, session, **kwargs)
    text = re.findall(TICK_FORMAT, text, re.S)
    today = date.today()
    tick = pd.DataFrame(list(reversed(text)), columns=TICK_COLUMNS)
    tick = item_transfer(volume=pd.to_numeric, price=pd.to_numeric)(tick)
    return time_wrap(tick, today)


# 获取当天1min数据
today_1min = reconnect_wrap(default=pd.DataFrame)(value_wrapper(tick2min_group, lambda f: f.dropna())(get_tick))
# 获取历史1min数据
history_1min = reconnect_wrap(default=pd.DataFrame)(
    value_wrapper(tick2min, item_transfer(volume=lambda v: 100*v))(history_tick)
)


def search(index):
    c = 0
    for dt in index:
        if dt.minute > 30:
            return c
        c += 1


def sh_slice(f):
    c = search(f.index)
    return f.iloc[c:]


def sz_slice(f):
    c = search(f.index)
    index = f.index
    index.set_value(index, index[-1], index[-1].replace(minute=0, second=0))
    return f.iloc[c:]


def get_slice(code):
    if code.startswith('sh') or code.startswith('6'):
        return sh_slice
    else:
        return sz_slice


if __name__ == '__main__':
    print sz_slice(history_1min('sz000001', datetime(2017, 4, 20)))