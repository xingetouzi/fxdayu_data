import tushare
import pandas as pd
from pandas_datareader.data import YahooDailyReader
from collector import DataCollector


# def dt_trans(function):
#         def wrapper(*args, **kwargs):
#             data = function(*args, **kwargs)
#             data.index = pd.to_datetime(data['date'], format='%Y-%m-%d')
#             return data
#
#         return wrapper
#
#
# class TsWrapper(object):
#
#     def __getattr__(self, item):
#         function = getattr(tushare, item)
#         return dt_trans(function)
#
#
# ts = TsWrapper()


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
            ktype='D', autype='qfq',
            **kwargs
    ):
        hs300 = tushare.get_hs300s()
        for code in hs300['code']:
            self.save_k_data(
                code, start, end,
                ktype, autype, **kwargs
            )

    def save_yahoo(self, symbols=None, db='yahoo', **kwargs):
        data = YahooDailyReader(symbols, **kwargs).read()
        self.save(
            data.rename_axis(self.trans_map['yahoo'], 1),
            '.'.join((symbols, kwargs.get('interval', 'd'))),
            db, index=True
        )
