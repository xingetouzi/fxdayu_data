import tushare
import pandas as pd
from pandas_datareader.data import YahooDailyReader
from collector import DataCollector


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
        super(StockData, self).__init__(host=host, port=port, db=db, user=user, **kwargs)

    def save_k_data(
            self, code=None, start='', end='',
            ktype='D', autype='qfq', index=False,
            retry_count=3, pause=0.001
    ):
        frame = tushare.get_k_data(
            code, start, end,
            ktype, autype, index,
            retry_count, pause
        )

        format_ = '%Y-%m-%d'
        if len(frame['date'].values[-1]) > 11:
            format_ = ' '.join((format_, '%H:%M'))

        frame['datetime'] = pd.to_datetime(
            frame.pop('date'),
            format=format_
        )

        frame.pop('code')

        self.save(frame, '.'.join((code, ktype)))
        print (code, 'saved')

    def update(self, col_name):
        doc = self.db[col_name].find_one(sort=[('datetime', -1)])
        code, ktype = col_name.split('.')
        try:
            self.save_k_data(code, start=doc['datetime'].strftime('%Y-%m-%d %H:%M'), ktype=ktype)
        except IndexError:
            print (col_name, 'already updated')

    def update_all(self):
        for collection in self.db.collection_names():
            self.update(collection)

    def save_hs300(
            self, start='', end='',
            ktype='D', autype='qfq', index=False,
            retry_count=3, pause=0.001
    ):
        hs300 = tushare.get_hs300s()
        for code in hs300['code']:
            self.save_k_data(
                code, start, end,
                ktype, autype, index,
                retry_count, pause
            )

    def save_yahoo(self, symbols=None, start=None, end=None, retry_count=3,
                   pause=0.001, session=None, adjust_price=False, ret_index=False,
                   chunksize=25, interval='d', db='yahoo'):
        data = YahooDailyReader(symbols, start, end, retry_count, pause, session,
                                adjust_price, ret_index, chunksize, interval).read()
        data['datetime'] = data.index

        self.save(data.rename_axis(self.trans_map['yahoo'], 1), '.'.join((symbols, interval)), db)
