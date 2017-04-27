# encoding:utf-8
import tushare
import pandas as pd
from pandas_datareader.data import YahooDailyReader, DataReader
from fxdayu_data.data.collector.base import DataCollector
from fxdayu_data.data.handler.mongo_handler import MongoHandler
from fxdayu_data.data.collector.sina_tick import today_1min, history_1min, get_slice
from datetime import date, datetime, timedelta


def coder(code):
    if code.startswith("6"):
        return "sh%s" % code
    elif code.startswith("3") or code.startswith("0"):
        return "sz%s" % code
    else:
        return code


class StockData(DataCollector):
    trans_map = {
        'yahoo': {'Date': 'datetime',
                  'Close': 'close',
                  'High': 'high',
                  'Low': 'low',
                  'Open': 'open',
                  'Volume': 'volume'}
    }

    def __init__(self, host='localhost', port=27017, db='HS', user=None, **kwargs):
        super(StockData, self).__init__(MongoHandler(host, port, user, db, **kwargs))

    def save_k_data(self, code=None, start='', end='', ktype='D', autype='qfq', **kwargs):
        frame = tushare.get_k_data(
            code, start, end,
            ktype, autype, **kwargs
        )
        date = frame.pop('date')

        try:
            frame['datetime'] = pd.to_datetime(date, format='%Y-%m-%d')
        except TypeError:
            frame['datetime'] = pd.to_datetime(date, format='%Y-%m-%d %H:%M')

        frame.pop('code')

        self.client.inplace(frame, '.'.join((code, ktype)))
        return code, 'saved'

    def update(self, col_name):
        """

        :param col_name: 表名
        :return:
        """
        doc = self.client.read(col_name, length=1)
        code, ktype = col_name.split('.')
        try:
            self.save_k_data(code, start=doc.index[0].strftime('%Y-%m-%d %H:%M'), ktype=ktype)
        except IndexError as ie:
            print (col_name, 'already updated')

    def update_all(self):
        for collection in self.client.table_names():
            self.update(collection)

    def save_stocks(self, pool, start='', end='',
                    ktype='D', autype='qfq', **kwargs):
        stock_pool = getattr(tushare, 'get_%s' % pool)()
        self.multi_process(
            self.save_k_data,
            [([code, start, end, ktype, autype], kwargs) for code in stock_pool['code']]
        )

    def save_yahoo(self, symbols=None, db='yahoo', **kwargs):
        """

        :param symbols:
        :param db:
        :param kwargs:
        :return:
        """
        data = YahooDailyReader(symbols, **kwargs).read()
        return self.client.inplace(
            data.rename_axis(self.trans_map['yahoo'], 1),
            '.'.join((symbols, kwargs.get('interval', 'd'))), db
        )

    def update_yahoo(self, db='yahoo'):
        for col in self.client.table_names(db):
            last = self.client.read(col, db, length=1)
            col = str(col)
            index = col.rfind('.')
            code, interval = col[:index], col[index+1:]
            print self.save_yahoo(code, db, start=last.index[0], interval=interval)

    def save_1min(self, collection, start, end=None, db=None, transfer=None):
        if transfer:
            code = transfer(collection)
        else:
            code = collection
        print 'saving %s' % code
        for frame in self._get_1min(code, start, end):
            print self.client.inplace(frame, collection, db)
        print 'finish %s' % code

    @staticmethod
    def _get_1min(code, start, end=None):
        today = datetime.today()
        if not end:
            end = today
        else:
            if end > today:
                end = today

        slicer = get_slice(code)

        while start.date() < end.date():
            if start.isoweekday() < 6:
                frame = history_1min(code, start)
                if len(frame):
                    yield slicer(frame)
            start += timedelta(1)

        if today.isoweekday() < 6:
            if end.date() == today.date() and (today.hour > 16):
                frame = today_1min(code)
                if len(frame):
                    yield slicer(frame)

    def update_1min(self, collection, db=None, transfer=None):
        current = self.client.read(collection, db, length=1)
        last = current.index[0]
        self.save_1min(collection, last+timedelta(1), db=db, transfer=transfer)

    def update_1mins(self, collections=None, db=None, transfer=None):
        if not collections:
            collections = self.client.table_names(db)

        if len(collections) < 10:
            for collection in collections:
                self.update_1min(collection, db, transfer)
        else:
            self.multi_process(self.update_1min, [[col, db, transfer] for col in collections])


if __name__ == '__main__':
    sd = StockData(host='192.168.0.103', port=30000, db='TradeStock')
    import pymongo
    for code in pymongo.MongoClient()['HS'].collection_names():
        code = coder(code)[:-2]
        print code
        sd.save_1min(code, datetime(2017, 1, 1))