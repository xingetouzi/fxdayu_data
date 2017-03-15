import tushare
import pandas as pd
from pandas_datareader.data import YahooDailyReader, DataReader
from collector import DataCollector
from fxdayu_data.data.base import MongoHandler


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

        format_ = '%Y-%m-%d'
        if len(frame['date'].values[-1]) > 11:
            format_ = ' '.join((format_, '%H:%M'))

        frame['datetime'] = pd.to_datetime(
            frame.pop('date'),
            format=format_
        )

        frame.pop('code')

        self.client.inplace(frame, '.'.join((code, ktype)))
        return code, 'saved'

    def update(self, col_name):
        doc = self.client.db[col_name].find_one(sort=[('datetime', -1)])
        code, ktype = col_name.split('.')
        try:
            self.save_k_data(code, start=doc['datetime'].strftime('%Y-%m-%d %H:%M'), ktype=ktype)
        except IndexError:
            print (col_name, 'already updated')

    def update_all(self):
        for collection in self.client.db.collection_names():
            self.update(collection)

    def save_stocks(self, pool, start='', end='',
                    ktype='D', autype='qfq', **kwargs):
        stock_pool = getattr(tushare, 'get_%s' % pool)()
        for code in stock_pool['code']:
            print self.save_k_data(
                code, start, end,
                ktype, autype, **kwargs
            )

    def save_yahoo(self, symbols=None, db='yahoo', **kwargs):
        data = YahooDailyReader(symbols, **kwargs).read()
        self.client.inplace(
            data.rename_axis(self.trans_map['yahoo'], 1),
            '.'.join((symbols, kwargs.get('interval', 'd'))), db
        )
