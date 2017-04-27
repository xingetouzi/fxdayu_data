# encoding:utf-8
try:
    from Queue import Queue, Empty
except ImportError:
    from queue import Queue, Empty
from datetime import time as d_time
from datetime import datetime, timedelta
from logging import config
import json
import logging
import threading
import time
import re
import pandas as pd
import requests

from fxdayu_data.data.handler.redis_handler import RedisHandler
from fxdayu_data.data.collector.sina_tick import today_1min, reconnect_wrap


LIVE_DATA_COLS = ['name', 'open', 'pre_close', 'price', 'high', 'low', 'bid', 'ask', 'volume', 'amount',
                  'b1_v', 'b1_p', 'b2_v', 'b2_p', 'b3_v', 'b3_p', 'b4_v', 'b4_p', 'b5_v', 'b5_p',
                  'a1_v', 'a1_p', 'a2_v', 'a2_p', 'a3_v', 'a3_p', 'a4_v', 'a4_p', 'a5_v', 'a5_p', 'date', 'time', 's']


def code_transfer(code):
    if code.startswith('6'):
        return 'sh' + code
    elif code.startswith('0') or code.startswith('3'):
        return 'sz' + code
    else:
        return code


class QuoteSaver(RedisHandler):

    def __init__(self, redis_client=None, transformer=None, **kwargs):
        super(QuoteSaver, self).__init__(redis_client, transformer, **kwargs)

    def publish(self, channel, message):
        self.client.publish(channel, message)

    def expire(self, name, ttl):
        fields = self.fields
        pl = self.client.pipeline()
        for field in fields:
            pl.expire('%s:%s' % (name, field), ttl)
        pl.execute()

    def expireat(self, name, timestamp):
        fields = self.fields
        pl = self.client.pipeline()
        for field in fields:
            pl.expireat('%s:%s' % (name, field), timestamp)
        pl.execute()


class StockInstance(object):

    def __init__(self, code, timestamp, price=0, volume=0, **kwargs):
        self.code = code
        self.datetime = timestamp
        self.open = kwargs.get('open', price)
        self.close = kwargs.get('close', price)
        self.high = kwargs.get('high', price)
        self.low = kwargs.get('low', price)
        self.last_volume = kwargs.get('volume', volume)
        self.volume_total = kwargs.get('volume', volume)

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
        if timestamp > self.datetime:
            self.new(timestamp.replace(second=0)+timedelta(minutes=1), price, volume)
            return False, self.show()
        else:
            self.volume_total = volume
            dct = {'close': price, 'volume': self.volume}
            if price > self.high:
                dct['high'] = price
                self.high = price
            elif price < self.low:
                dct['low'] = price
                self.low = price

            return True, dct

    def refresh(self, **kwargs):
        for key, value in kwargs.items():
            self.__setattr__(key, value)


class StockMemory(object):
    def __init__(self, code, db):
        self.code = code
        self.db = db
        self.instance = None

    def recharge(self):
        code = self.code
        candle = self.db.read(code)
        if len(candle):
            last_time = candle.index[-1]
            delta = datetime.now() - last_time
            min1 = today_1min(code, num=int(delta.seconds/3.2))
            min1 = min1[min1.index >= last_time]
            if len(min1) > 1:
                doc = min1.iloc[0].to_dict()
                doc['datetime'] = min1.index[0]
                self.db.locate_update(doc, code)
                self.db.write(min1.iloc[1:], code)
                volume = candle.iloc[:-1]['volume'].sum() + min1.iloc[:-1]['volume'].sum()
                self.prepare_instance(min1.index[-1], volume, **min1.iloc[-1].to_dict())
            else:
                self.prepare_instance(last_time, candle.iloc[:-1]['volume'].sum(), **candle.iloc[-1].to_dict())

        else:
            min1 = today_1min(code)
            if len(min1):
                self.db.write(min1, code)
                self.db.expireat(code, timestamp=int(time.mktime(datetime.today().replace(hour=18).timetuple())))
                self.prepare_instance(min1.index[-1], min1['volume'].iloc[:-1].sum(), **min1.iloc[-1].to_dict())

    def prepare_instance(self, dt, last_volume=0, **kwargs):
        self.instance = StockInstance(self.code, dt, **kwargs)
        if last_volume:
            self.instance.last_volume = last_volume

    def on_quote(self, timestamp, price, volume, pipeline=None):
        if self.instance:
            update, value = self.instance.on_quote(timestamp, price, volume)
            if update:
                self.db.locate_update(value, self.code, pipeline=pipeline)
            else:
                self.db.write(value, self.code, pipeline=pipeline)

    def shutdown(self):
        self.instance = None


class SinaQuote(object):

    url = "http://hq.sinajs.cn/?func=getData._hq_cron();&list="

    def __init__(self, name, codes, sleep=5.0):
        self.name = name
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
        self.quote_url = self.url + ','.join(codes)
        self.thread = None
        self.count = 0

    @property
    def quoting(self):
        return self._quoting

    def __iter__(self):
        if not self._quoting:
            self._quoting = True
        return self

    def next(self):
        if self._quoting:
            return self.quote(self.quote_url)
        else:
            raise StopIteration()

    @reconnect_wrap(default=pd.DataFrame)
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
                del self.thread

    def stream(self, handler):
        while self._quoting:
            try:
                quotation = self.next()
            except Exception as e:
                print e
                logging.error(e.message)
                continue

            handler(quotation)

            self.count += 1
            if self.count > 60:
                logging.info("received 60 quotes")
                self.count = 0

            time.sleep(self.sleep)


class QuotesManager(object):

    class Quest:
        def __init__(self, function, *args, **kwargs):
            self.function = function
            self.args = args
            self.kwargs = kwargs

        def run(self):
            return self.function(*self.args, **self.kwargs)

    def __init__(self, db=None, start=True, listen=None, **kwargs):
        self._quoters = {}
        self.lock = threading.RLock()
        self.instance = {}
        self.memories = {}
        self.queue = Queue()
        if isinstance(db, QuoteSaver):
            self.db = db
        elif isinstance(db, dict):
            self.db = QuoteSaver(**db)
        elif isinstance(db, str):
            self.db = QuoteSaver(**json.load(open(db)))
        else:
            self.db = QuoteSaver()

        self._trading = False
        self.main = threading.Thread(target=self.handle_quest)

        if listen:
            if isinstance(listen, dict):
                self.listen(**listen)
            elif isinstance(listen, str):
                listen = json.load(open(listen))
                self.listen(**listen)

    def quote(self, name=None, codes=None, quoter=None):
        if not quoter:
            if name and codes:
                print name, codes
            else:
                raise ValueError("Neither of name and codes should be None if quoter is None")
            try:
                quoter = self._quoters[name]
            except KeyError:
                quoter = SinaQuote(name, codes)
                self._quoters[name] = quoter
        else:
            if quoter.name not in self._quoters:
                self._quoters[quoter.name] = quoter

        if self._trading:
            quoter.start(self.on_quote)

        for code in codes:
            try:
                memory = self.memories[code]
            except KeyError:
                memory = self.memories.setdefault(code, StockMemory(code, self.db))

            self.queue.put(
                self.Quest(memory.recharge)
            )

    def start(self):
        if not self._trading:
            self._trading = True
            self.main.start()

    def stop(self):
        if self._trading:
            self._trading = False
            for quoter in self._quoters.values():
                quoter.stop()
            self.main.join()

    def handle_quest(self):
        while self._trading:
            try:
                quest = self.queue.get(timeout=5)
            except Empty:
                continue

            quest.run()

    def listen(self, **kwargs):
        for name, codes in kwargs.items():
            self.queue.put(
                self.Quest(self.quote, name, codes)
            )

    def on_quote(self, quotation):
        pl = self.db.client.pipeline()
        for name, value in quotation.iteritems():
            try:
                dt = datetime.strptime(value.date+' '+value.time, "%Y-%m-%d %H:%M:%S")
                self.memories[name].on_quote(dt, float(value.price), float(value.volume), pl)
            except Exception as e:
                print e
                logging.error(e.message)
        pl.publish('tick', datetime.now())
        pl.execute()


class Monitor(object):

    class Timer:
        def __init__(self, gap=timedelta(minutes=5)):
            self.gap = gap
            self.last = datetime.now()

        def tick(self, timestamp):
            if timestamp - self.last >= self.gap:
                self.last = timestamp
                return True
            else:
                return False

    def __init__(self, listen=None, start=True, db=None, log=None):
        self.listen = listen
        self.manager = None
        self.db = db
        self._monitoring = False
        self.timer = self.Timer()

        self.morning_start = d_time(9, 30)
        self.morning_end = d_time(11, 30)
        self.noon_start = d_time(13)
        self.noon_end = d_time(15)

        self.watcher = threading.Thread(target=self.watch)

        if isinstance(log, str):
            config.fileConfig(log)

    def start(self):
        if not self._monitoring:
            self._monitoring = True
            self.watcher.start()
            logging.warning("Activate service")

    def stop(self):
        if self._monitoring:
            self._monitoring = False
            self.watcher.join()
            logging.warning("Deactivate service")

    def watch(self):
        while self._monitoring:
            if self.is_trading_time():
                self.start_quote(self.listen)
            else:
                self.stop_quote()

            if self.timer.tick(datetime.now()):
                logging.info('monitor running')

    def is_trading_time(self):
        now = datetime.now().time()
        if (now < self.morning_end and (now > self.morning_start)) \
                or (now < self.noon_end and (now > self.noon_start)):
            return True
        else:
            return False

    def start_quote(self, listen):
        if not isinstance(getattr(self, 'manager', None), QuotesManager):
            self.manager = QuotesManager(self.db, listen=listen)
            self.manager.start()

    def stop_quote(self):
        if isinstance(getattr(self, 'manager', None), QuotesManager):
            self.manager.stop()
            del self.manager


if __name__ == '__main__':
    monitor = Monitor(listen="sina_stock.json", log="logging.conf")
    monitor.start()