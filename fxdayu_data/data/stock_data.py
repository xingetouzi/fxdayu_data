# encoding:utf-8
import tushare
import pandas as pd
from pandas_datareader.data import YahooDailyReader, DataReader
from collector import DataCollector
from fxdayu_data.data.handler import MongoHandler


class StockData(DataCollector):
    trans_map = {
        'yahoo': {'Date': 'datetime',
                  'Close': 'close',
                  'High': 'high',
                  'Low': 'low',
                  'Open': 'open',
                  'Volume': 'volume'}
    }

    def __init__(self, host='localhost', port=27017, db='HS', user={}, **kwargs):
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