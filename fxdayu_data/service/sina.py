# encoding:utf-8
import requests
import re
import pandas as pd
from datetime import datetime, timedelta
from fxdayu_data.data.redis_handler import RedisHandler
import time
import threading


LIVE_DATA_COLS = ['name', 'open', 'pre_close', 'price', 'high', 'low', 'bid', 'ask', 'volume', 'amount',
                  'b1_v', 'b1_p', 'b2_v', 'b2_p', 'b3_v', 'b3_p', 'b4_v', 'b4_p', 'b5_v', 'b5_p',
                  'a1_v', 'a1_p', 'a2_v', 'a2_p', 'a3_v', 'a3_p', 'a4_v', 'a4_p', 'a5_v', 'a5_p', 'date', 'time', 's']


class QuoteSaver(RedisHandler):

    def __init__(self, redis_client=None, transformer=None, **kwargs):
        super(QuoteSaver, self).__init__(redis_client, transformer, **kwargs)
        self.instance = {}

    def on_quote(self, quotation):
        if isinstance(quotation, pd.DataFrame):
            pipeline = self.client.pipeline()
            dt = datetime.now()
            for name, value in quotation.iteritems():
                dt = datetime.strptime(value.date+' '+value.time, "%Y-%m-%d %H:%M:%S")
                self.single_update(name, dt, float(value.price), float(value.volume), pipeline)
            pipeline.publish('HS_stocks', dt)
            pipeline.execute()

    def single_update(self, name, dt, price, volume, pipeline):
        try:
            instance = self.instance[name]
        except KeyError:
            instance = StockInstance(name, price, dt, volume)
            self.instance[name] = instance
            self.write(instance.show(), name, pipeline=pipeline)

        update, value = instance.on_quote(dt, price, volume)
        if update:
            self.locate_update(value, name, pipeline=pipeline)
        else:
            self.locate_update({'datetime': dt.replace(second=0, microsecond=0)}, name, pipeline=pipeline)
            self.write(value, name, pipeline=pipeline)

    def publish(self, channel, message):
        self.client.publish(channel, message)


class StockInstance(object):

    def __init__(self, code, price, timestamp, volume):
        self.code = code
        self.datetime = timestamp
        self.open = price
        self.close = price
        self.high = price
        self.low = price
        self.last_volume = volume
        self.volume_total = volume

    def show(self):
        return {'datetime': self.datetime,
                'high': self.high,
                'low': self.low,
                'open': self.open,
                'close': self.close,
                'volume': self.volume}

    def new(self, timestamp, price, volume):
        self.last_volume += self.volume
        self.datetime = timestamp
        self.open = price
        self.close = price
        self.high = price
        self.low = price
        self.volume_total = volume

    @property
    def volume(self):
        return self.volume_total - self.last_volume

    def on_quote(self, timestamp, price, volume):
        if timestamp.minute != self.datetime.minute:
            self.new(timestamp, price, volume)
            return False, {'datetime': self.datetime,
                           'high': self.high,
                           'low': self.low,
                           'open': self.open,
                           'close': self.close,
                           'volume': self.volume}
        else:
            self.volume_total = volume
            dct = {'datetime': timestamp, 'close': price, 'volume': self.volume}
            if price > self.high:
                dct['high'] = price
                self.high = price
            elif price < self.low:
                dct['low'] = price
                self.low = price

            return True, dct


class SinaQuote(object):

    url = "http://hq.sinajs.cn/?func=getData._hq_cron();&list="

    def __init__(self, codes, sleep=5):
        self.codes = codes
        self.sleep = sleep
        self.session = requests.Session()
        self.session.headers = {"Accept": "*/*",
                                "Accept-Encoding": "gzip, deflate, sdch",
                                "Accept-Language": "zh-CN,zh;q=0.8",
                                "Connection": "keep-alive",
                                "Host": "hq.sinajs.cn",
                                "Referer": "http://finance.sina.com.cn/data/",
                                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 "
                                              "(KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36}"}
        self._quoting = False
        self.quote_url = self.url + ','.join(self.codes)
        self.thread = None

    def __iter__(self):
        if not self._quoting:
            self._quoting = True
        return self

    def next(self):
        if self._quoting:
            return self.quote(self.quote_url)
        else:
            raise StopIteration()

    def quote(self, url, **kwargs):
        text = self.session.get(url, **kwargs).text
        return pd.DataFrame(self.standardlize(text), index=LIVE_DATA_COLS)

    @staticmethod
    def standardlize(text):
        return {code: value.split(',') for code, value in re.findall('hq_str_(.*?)="(.*?)"', text, re.S)}

    @staticmethod
    def coder(code):
        if code.startswith('6'):
            return 'sh' + code
        elif code.startswith('0') or code.startswith('3'):
            return 'sz' + code
        else:
            return code

    def start(self, handler):
        if self._quoting:
            return
        else:
            self._quoting = True
            self.thread = threading.Thread(target=self.stream, args=(handler, ))
            self.thread.start()

    def stop(self):
        if self._quoting:
            self._quoting = False
            if isinstance(self.thread, threading.Thread):
                self.thread.join()

    def stream(self, handler):
        while self._quoting:
            try:
                quotation = self.next()
            except Exception as e:
                print e
                continue

            handler(quotation)
            time.sleep(self.sleep)


class QuotesHandler(object):

    def __init__(self, db=None, **kwargs):
        self._quoters = {}
        if isinstance(db, QuoteSaver):
            self.db = db
        elif isinstance(db, dict):
            self.db = QuoteSaver(**db)
        else:
            self.db = QuoteSaver()

    def request(self, name, *codes):
        quoter = SinaQuote(codes)
        self._quoters[name] = quoter
        quoter.start(self.db)


if __name__ == '__main__':
    pass