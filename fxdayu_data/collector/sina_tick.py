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

TODAY_HEADERS = {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, sdch",
    "Accept-Language": "zh-CN,zh;q=0.8",
    "Connection": "keep-alive",
    "Host": "hq.sinajs.cn",
    "Referer": "http://finance.sina.com.cn/data/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36}"
}

HIS_HEADERS = {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, sdch",
    "Accept-Language": "zh-CN,zh;q=0.8",
    "Connection": "keep-alive",
    "Referer": "http://finance.sina.com.cn/data/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36}"
}

TICK_HISTORY = "http://market.finance.sina.com.cn/downxls.php?"
TICK_TODAY = "http://vip.stock.finance.sina.com.cn/quotes_service/view/CN_TransListV2.php?"
TICK_FORMAT = """new Array\('(.*?)', '(.*?)', '(.*?)', '(.*?)'\);"""
HISTORY_FORMAT = "\n%s\n" % "\t".join(['(.*?)']*6)
TICK_COLUMNS = ['time', 'volume', 'price', 'direction']
HISTORY_TICK_COLUMNS = ['time', 'price', 'change', 'volume', 'amount', 'trend']


REJECTION = u"拒绝访问".encode('utf-8')
NODATA = u"当天没有数据".encode("gbk")


class SinaBreak(Exception):
    pass


class SinaNoData(Exception):
    pass


def reconnect_wrap(retry=3, wait=0.1, error=ConnectionError):
    def wrapper(func):
        def reconnect(*args, **kwargs):
            rt = retry
            while rt >= 0:
                try:
                    return func(*args, **kwargs)
                except error as e:
                    print(e)
                    rt -= 1
            raise e

        return reconnect
    return wrapper


def time_wrap(frame, dt, column='time'):
    if isinstance(dt, datetime):
        dt = dt.date()

    frame.index = map(lambda t: datetime.combine(dt, str2time(t)), frame[column])

    return frame.sort_index()


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
def history_text(code, date_, **kwargs):
    url = make_url(TICK_HISTORY, join_params(symbol=code, date=date_.strftime("%Y-%m-%d")))
    response = requests.get(url, headers=HIS_HEADERS, **kwargs)
    return check_response(response.content)


def check_response(content):
    if REJECTION in content:
        raise SinaBreak()
    elif NODATA in content:
        raise SinaNoData()
    else:
        return content


def text2tick(content, date_):
    tick = pd.DataFrame(re.findall(HISTORY_FORMAT, content, re.S), columns=HISTORY_TICK_COLUMNS)
    tick = item_transfer(tick, price=pd.to_numeric, volume=lambda s: pd.to_numeric(s)*100)
    return time_wrap(tick, date_)


# 获取历史tick数据(DataFrame)
def history_tick(code, date_, **kwargs):
    text = history_text(code, date_, **kwargs)
    return text2tick(text, date_)


# 获取历史1min数据(DataFrame)
def history_1min(code, date, **kwargs):
    tick = history_tick(code, date, **kwargs)
    return tick2min(tick)


# tick数据合成一分钟线
def tick2min(frame):
    try:
        resampler = frame.resample('1min', label='right', closed='right')
        result = resampler['price'].agg(CANDLE_MAP)
        result['volume'] = resampler['volume'].sum()
        return result.dropna()
    except Exception as e:
        print(e)
        return frame


# 获取当日tick(str)
@reconnect_wrap()
def raw_tick(code, session=None, **kwargs):
    url = make_url(TICK_TODAY, join_params(symbol=code, **kwargs))
    if not session:
        session = session if session else requests.Session()
        session.headers = TODAY_HEADERS
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
    return f.drop(filter(drop, f.index))


def sz_slice(f):
    index = f.index.tolist()
    index[-1] = index[-1].replace(minute=0, second=0)
    f.index = index

    return sh_slice(f)


def get_slice(code):
    if code.startswith('sh') or code.startswith('6'):
        return sh_slice
    else:
        return sz_slice


_0930_ = time(9, 30)
_1130_ = time(11, 30)
_1300_ = time(13)
_1500_ = time(15)


def drop(timestamp):
    t = timestamp.time()
    if t > _0930_:
        if t <= _1130_:
            return False
        elif t > _1300_:
            if t <= _1500_:
                return False
    return True

