from fxdayu_data.data import MongoHandler
from datetime import datetime, timedelta
import pandas as pd
from collections import Iterable, defaultdict


OANDA_MAPPER = {'open': 'openMid',
                'high': 'highMid',
                'low': 'lowMid',
                'close': 'closeMid'}


RESAMPLE_MAP = {'high': 'max',
                'low': 'min',
                'close': 'last',
                'open': 'first',
                'volume': 'sum'}


def mapper(fields):
    if isinstance(fields, str):
        return RESAMPLE_MAP[fields]
    elif isinstance(fields, (list, tuple)):
        return {field: RESAMPLE_MAP[field] for field in fields}
    else:
        return RESAMPLE_MAP


class TimeEdge():
    def __init__(self, edge):
        self.edge = edge
        self.start = None
        self.end = None

    def range(self, end):
        self.end = end
        self.start = self.edge(end)
        return end

    def __call__(self, x):
        self.range(x[-1])
        return pd.DatetimeIndex(
            reversed(map(self.scheduler, reversed(x)))
        )

    def scheduler(self, t):
        if t < self.end and (t > self.start):
            return self.end
        else:
            return self.range(t)


class MarketDataFreq(object):

    def __init__(self, client=None, host='localhost', port=27017, users=None, db=None, **kwargs):
        self.client = client if client else MongoHandler(host, port, users, db, **kwargs)
        self.read = self.client.read
        self.write = self.client.write
        self.inplace = self.client.inplace
        self.initialized = False
        self._panels = {}
        self.mapper = {}
        self._db = self.client.db
        self.sample_factor = {'min': 1, 'H': 60, 'D': 240, 'W': 240*5, 'M': 240*5*31}
        self.grouper = {
            'W': TimeEdge(lambda x: x.replace(hour=0, minute=0)-timedelta(days=x.weekday())),
            'H': TimeEdge(lambda x: x.replace(minute=30, hour=x.hour if x.minute > 30 else x.hour-1) if x.hour < 12
                          else x.replace(minute=0, hour=x.hour if x.minute != 0 else x.hour-1))
        }
        self.fields = list(RESAMPLE_MAP.keys())
        self._time = datetime.now()
        self._frequency = '1min'

    @property
    def time(self):
        return self._time

    def set_time(self, time):
        self._time = time

    def init(self, symbols, start=None, end=None, db=None, frequency='1min'):
        self._db = defaultdict(lambda: db)
        self._frequency = frequency

        def initialize(_symbol, _db):
            result = self._read_db(_symbol, ['open', 'high', 'low', 'close', 'volume'], start, end, None, _db)
            if len(result):
                self._panels[_symbol] = result
                self._db[_symbol] = _db

        if isinstance(symbols, str):
            initialize(symbols, db)
        elif isinstance(symbols, dict):
            for db_, symbol in symbols.items():
                for s in symbol:
                    initialize(s, db_)
        elif isinstance(symbols, Iterable):
            for symbol in symbols:
                initialize(symbol, db)

        self.initialized = True

    def _read_db(self, symbol, fields, start, end, length, db):
        if fields is None:
            fields = ['datetime', 'open', 'high', 'low', 'close', 'volume']
        elif isinstance(fields, str):
            fields = ['datetime', fields]
        elif 'datetime' not in fields:
            fields = list(fields)
            fields.append('datetime')

        mapper = self.mapper.get(db, {})
        trans_map = {item[1]: item[0] for item in mapper.items()}

        try:
            result = self.client.read(
                symbol, db, 'datetime', start, end, length,
                projection=fields
            )
        except KeyError:
            return pd.DataFrame()

        return result.rename_axis(trans_map, axis=1)

    def current(self, symbol=None, frequency=None):
        return self.history(symbol, frequency, length=1)

    def history(self, symbol=None, frequency=None, fields=None, start=None, end=None, length=None, db=None):
        if symbol is None:
            symbol = self._panels.keys()

        if fields is None:
            fields = self.fields

        if frequency is None or (frequency == self._frequency):
            if isinstance(symbol, (str, unicode)):
                return self._find_candle(symbol, fields, start, end, length)
            else:
                return self._dimension({s: self._find_candle(s, fields, start, end, length) for s in symbol},
                                       length, fields)
        else:
            n, w = self.f_period(frequency)
            grouper = self.grouper.get(w, None)
            agg = mapper(fields)
            if isinstance(symbol, (str, unicode)):
                return self.resample(symbol, frequency, fields, start, end, length, n, w, grouper, agg)
            else:
                return self._dimension(
                    {s: self.resample(
                        s, frequency, fields, start, end, length, n, w, grouper, agg
                    ) for s in symbol},
                    length, fields
                )

    @staticmethod
    def _dimension(dct, length, fields):
        if length == 1:
            if isinstance(fields, str):
                return pd.Series(dct)
            else:
                return pd.DataFrame(dct)
        elif isinstance(fields, str):
            return pd.DataFrame(dct)
        else:
            return pd.Panel(dct)

    @staticmethod
    def search_axis(axis, time):
        index = axis.searchsorted(time)
        if index < len(axis):
            if axis[index] <= time:
                return index
            else:
                return index - 1
        else:
            return len(axis) - 1

    def major_slice(self, axis, now, start, end, length):
        last = self.search_axis(axis, now)

        if end:
            end = self.search_axis(axis, end)
            if end > last:
                end = last
        else:
            end = last

        if length:
            if length == 1:
                return end
            elif start:
                if start + length <= end+1:
                    return slice(start, start+length)
                else:
                    return slice(start, end+1)
            else:
                end += 1
                if end < length:
                    raise KeyError("data required out of range")
                return slice(end-length, end)
        elif start:
            return slice(start, end+1)
        else:
            return slice(0, end+1)

    def _find_candle(self, symbol, fields, start, end, length):
        try:
            frame = self._panels[symbol]
            time_slice = self.major_slice(frame.index, self.time, start, end, length)
            return frame.iloc[time_slice][fields]
        except KeyError:
            if not end or end > self.time:
                end = self.time
            if isinstance(fields, str):
                result = self._read_db(symbol, fields, start, end, length, self._db[symbol])[fields]
            else:
                result = self._read_db(symbol, fields, start, end, length, self._db[symbol])

            if length != -1:
                return result
            else:
                return result.iloc[0] if len(result) else result

    def resample(self, symbol, frequency, fields, start, end, length, n, w, grouper, agg):
        frame = self._find_candle(symbol, fields, start, end, length*n*self.sample_factor[w] if length else None)
        if grouper is not None:
            return frame.groupby(grouper).agg(agg)
        else:
            return frame.resample(frequency, label='right', closed='right').agg(agg).dropna()

    @staticmethod
    def f_period(frequency):
        n = ''
        w = ''
        for f in frequency:
            if f.isdigit():
                n += f
            else:
                w += f

        return (int(n) if len(n) else 1), w

    @property
    def all_time(self):
        all_ = []
        for item in self._panels.items():
            all_.extend(filter(lambda x: x not in all_, item[1].index))

        return sorted(all_)

    def can_trade(self, symbol=None):
        if symbol:
            try:
                return self.time in self._panels[symbol].index
            except KeyError:
                try:
                    data = self.client.read(symbol, self._db[symbol], end=self.time, length=1)
                    if data.index[0] == self.time:
                        return True
                except KeyError:
                    return False

                return False
        else:
            trades = []
            for s, frame in self._panels.items():
                if self.time in frame.index:
                    trades.append(s)
            return trades


if __name__ == '__main__':
    md = MarketDataFreq(host='192.168.0.103', port=30000, db='TradeStock')
    md.init('sh600000', datetime(2017, 1, 1))
    print md.history('sh600000', 'H')