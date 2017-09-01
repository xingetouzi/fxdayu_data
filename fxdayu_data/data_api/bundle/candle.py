from fxdayu_data.data_api.basic.candle import BasicCandle
from collections import Iterable
from fxdayu_data.data_api import lru_cache
import pandas as pd
import six


class BLPCandle(BasicCandle):

    def __init__(self,
                 columns=("open", "high", "low", "close", "volume"),
                 adjust=None,
                 **kwargs):
        self.tables = kwargs
        self.columns = columns
        self.adjust = adjust

    @classmethod
    def dir(cls, **kwargs):
        for name, value in kwargs.items():
            kwargs[name] = DateCandleTable(value)

        return cls(**kwargs)

    def set(self, **kwargs):
        self.tables.update(kwargs)

    def set_adjust(self, adjust):
        self.adjust = adjust

    def _read(self, table, symbol, fields, start, end, length, adjust):
        try:
            result = table.read(symbol, start, end, length, fields)
        except:
            return pd.DataFrame()

        if not adjust:
            return result
        else:
            if "volume" in result.columns:
                volume = result.pop("volume")
                data = self.adjust.cal(symbol, result, adjust)
                data['volume'] = volume
                return data
            else:
                return self.adjust.cal(symbol, result, adjust)

    @lru_cache(128)
    def read(self, symbols, freq, fields, start, end, length, adjust):
        table = self.tables[freq]
        if isinstance(symbols, six.string_types):
            return self._read(table, symbols, fields, start, end, length, adjust)
        else:
            return pd.Panel.from_dict(
                {symbol: self._read(table, symbol, fields, start, end, length, adjust) for symbol in symbols}
            )

    def __call__(self, symbols, freq, fields=None, start=None, end=None, length=None, adjust=None):
        if not isinstance(symbols, six.string_types):
            symbols = tuple(symbols)

        if isinstance(fields, six.string_types):
            fields = (fields,)
        elif isinstance(fields, Iterable):
            fields = tuple(fields)

        return self.read(symbols, freq, fields, start, end, length, adjust)


from fxdayu_data.data_api.bundle.blp_reader import BLPTable
from datetime import datetime


def catch(date, num):
    return date - date % num


def reduce(func, iterable, initial):
    for i in iterable:
        initial = func(initial, i)
        yield initial


gap = (1, 100, 10000, 1000000, 100000000, 10000000000, 100000000000000)
loop = range(len(gap)-1)


def int2date(num):
    t = tuple(reduce(catch, gap, num))
    t = [int((t[i]-t[i+1])/gap[i]) for i in loop]
    t.reverse()
    return datetime(*t)


def time2int(time):
    if isinstance(time, datetime):
        return time.second + time.minute*100 + time.hour*10000 + \
               time.day*1000000 + time.month*100000000 + time.year*10000000000
    else:
        return time


def int2float(num):
    return num/10000.0


class DateCandleTable(BLPTable):

    def __init__(self, rootdir):
        super(DateCandleTable, self).__init__(rootdir, "datetime")

    def read(self, names, start=None, end=None, length=None, columns=None):
        return super(DateCandleTable, self).read(names, time2int(start), time2int(end), length, columns)

    def _read_line(self, index, column):
        if column != "volume":
            return list(map(int2float, super(DateCandleTable, self)._read_line(index, column)))
        else:
            return super(DateCandleTable, self)._read_line(index, column)

    def _read_index(self, index):
        return list(map(int2date, super(DateCandleTable, self)._read_index(index)))

#
# if __name__ == '__main__':
#     from fxdayu_data.data_api.bundle.adjust import BLPAdjust
#
#     candle = BLPCandle.dir(D="X:\\Users\caimeng\.fxdayu\data\Stock_D.bcolz",
#                            H="X:\\Users\caimeng\.fxdayu\data\Stock_H1.bcolz")
#     candle.set_adjust(BLPAdjust.dir("X:\\Users\caimeng\.fxdayu\data\ex_cum_factor.bcolz"))
#
#     print(candle("000001.XSHE", "D",  length=100, adjust="after"))
