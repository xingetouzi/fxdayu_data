# encoding:utf-8
from datetime import datetime, date, time
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
TICK_COLUMNS = ['time', 'volume', 'price', 'direction']
HISTORY_TICK_COLUMNS = ['time', 'price', 'change', 'volume', 'amount', 'trend']


def reconnect_wrap(retry=3, wait=0.1, error=ConnectionError):
    def wrapper(func):
        def reconnect(*args, **kwargs):
            rt = retry
            while rt >= 0:
                try:
                    return func(*args, **kwargs)
                except error as e:
                    print e
                    rt -= 1
            raise e

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


def item_transfer(frame, **kwargs):
    for item, function in kwargs.items():
        frame[item] = function(frame[item])
    return frame


# 获取历史tick数据(str)
@reconnect_wrap()
def history_text(code, date_):
    url = make_url(TICK_HISTORY, join_params(symbol=code, date=date_.strftime("%Y-%m-%d")))
    response = requests.get(url)
    return response.content


# 获取历史tick数据(DataFrame)
def history_tick(code, date_):
    text = history_text(code, date_)
    tick = pd.DataFrame(re.findall(HISTORY_FORMAT, text, re.S), columns=HISTORY_TICK_COLUMNS)
    tick = item_transfer(tick, price=pd.to_numeric, volume=lambda s: pd.to_numeric(s)*100)
    return time_wrap(tick, date_)


# 获取历史1min数据(DataFrame)
def history_1min(code, date):
    tick = history_tick(code, date)
    return tick2min(tick)


# tick数据合成一分钟线
def tick2min(frame):
    try:
        resampler = frame.resample('1min', label='right', closed='right')
        result = resampler['price'].agg(CANDLE_MAP)
        result['volume'] = resampler['volume'].sum()
        return result.dropna()
    except Exception as e:
        print e
        return frame


# 获取当日tick(str)
@reconnect_wrap()
def raw_tick(code, session=None, **kwargs):
    url = make_url(TICK_TODAY, join_params(symbol=code, **kwargs))
    if not session:
        session = session if session else requests.Session()
        session.headers = HEADERS
    request = requests.Request('get', url)
    response = session.send(request.prepare())
    return response.content


# 获取当日tick(DataFrame)
def get_tick(code, session=None, **kwargs):
    text = raw_tick(code, session, **kwargs)
    text = re.findall(TICK_FORMAT, text, re.S)
    today = date.today()
    tick = pd.DataFrame(list(reversed(text)), columns=TICK_COLUMNS)
    tick = item_transfer(tick, volume=pd.to_numeric, price=pd.to_numeric)
    return time_wrap(tick, today)


# 获取当日1min(DafaFrame)
def today_1min(code, **kwargs):
    tick = get_tick(code, **kwargs)
    return tick2min(tick).dropna()


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
