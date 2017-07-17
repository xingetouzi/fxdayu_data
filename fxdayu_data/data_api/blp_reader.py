import bcolz
import numpy as np
import pandas as pd
from collections import Iterable
from datetime import datetime, timedelta
import six


class BLPTable(object):

    def __init__(self, rootdir, index):
        self.table = bcolz.ctable(rootdir=rootdir, mode='r')
        self.line_map = self.table.attrs.attrs['line_map']
        self.index = self.table.cols[index]

    def read(self, names, start=None, end=None, length=None, columns=None):
        if columns is None:
            columns = self.table.names
        if isinstance(columns, six.string_types):
            columns = (columns,)

        if isinstance(names, six.string_types):
            return self._read(names, start, end, length, columns)
        elif isinstance(names, Iterable):
            return pd.Panel.from_dict(
                {name: self._read(name, start, end, length, columns) for name in names}
            )

    def _read(self, name, start, end, length, columns):
        index_slice = self._index_slice(name, start, end, length)

        return pd.DataFrame(
            {key: self._read_line(index_slice, key) for key in columns},
            index=self._read_index(index_slice)
        )

    def _read_line(self, index, column):
        return self.table.cols[column][index]

    def _read_index(self, index):
        return self.index[index]

    def _index_slice(self, name, start, end, length):
        head, tail = self.line_map[name]
        index = self.index[head:tail]
        if start:
            s = index.searchsorted(start)
            if end:
                e = index.searchsorted(end, 'right')
                return slice(head+s, head+e)
            elif length:
                return slice(head+s, head+s+length)
            else:
                return slice(head+s, tail)
        elif end:
            e = index.searchsorted(end, 'right')
            if length:
                return slice(head+e-length, head+e)
            else:
                return slice(head, head+e)
        elif length:
            return slice(tail-length, tail)
        else:
            return slice(head, tail)


class MapTable(BLPTable):
    def __init__(self, rootdir, index, index2blp=None, blp2index=None, **columns_map):
        super(MapTable, self).__init__(rootdir, index)
        self.index2blp = index2blp
        self.blp2index = blp2index
        self.columns_map = columns_map

    def read(self, names, start=None, end=None, length=None, columns=None):
        if self.index2blp:
            start = self.index2blp(start)
            end = self.index2blp(end)

        return super(MapTable, self).read(names, start, end, length, columns)

    def _read_line(self, index, column):
        array = self.table.cols[column][index]
        if column in self.columns_map:
            return np.array(map(self.columns_map[column], array))
        else:
            return array

    def _read_index(self, index):
        if self.blp2index:
            return np.array(map(self.blp2index, self.index[index]))
        else:
            return self.index[index]
    #
    # def _read(self, name, start, end, length, columns):
    #     result = super(MapTable, self)._read(name, start, end, length, columns)
    #     if self.blp2index:
    #         result.index = result.index.map(self.blp2index)
    #     for key in result.columns:
    #         if key in self.columns_map:
    #             result[key] = result[key].apply(self.columns_map[key])
    #     return result


def date2int(date):
    if isinstance(date, datetime):
        return int(date.strftime('%Y%m%d'))
    else:
        return date


def num2date(num):
    return datetime.strptime(str(num), '%Y%m%d') + timedelta(hours=15)


def price2float(price):
    return price/10000.0


class ClassifiedTable(MapTable):

    def __init__(self, rootdir, index, index2blp=None, blp2index=None, **columns_map):
        super(ClassifiedTable, self).__init__(rootdir, index, index2blp, blp2index, **columns_map)
        self.market_index = self.table.attrs['index']
        self.classify = self.table.attrs['classify']

    def find(self, name):
        if name in self.market_index:
            return self.market_index[name]
        elif name in self.classify:
            return self.classify[name]
        else:
            raise KeyError('Cannot find %s' % name)

    def read(self, names, start=None, end=None, length=None, columns=None):
        if isinstance(names, six.string_types):
            if '.' not in names:
                names = self.find(names)

        return super(ClassifiedTable, self).read(names, start, end, length, columns)

    def _read(self, name, start, end, length, columns):
        try:
            result = super(ClassifiedTable, self)._read(name, start, end, length, columns)
        except KeyError:
            return pd.DataFrame(columns=columns)

        return result[~result.index.duplicated()]


class DateCandleTable(ClassifiedTable):

    COLUMNS = ['open', 'high', 'low', 'close', 'volume']
    price_mapper = {"high": price2float,
                    "low": price2float,
                    "open": price2float,
                    "close": price2float}

    def __init__(self, rootdir):
        super(DateCandleTable, self).__init__(rootdir, 'date',  date2int, num2date, **self.price_mapper)

    def read(self, names, start=None, end=None, length=None, columns=None):
        if columns is None:
            columns = self.COLUMNS
        elif isinstance(columns, six.string_types):
            if not (columns == 'volume'):
                columns = (columns, 'volume')
        else:
            if 'volume' not in columns:
                columns = list(columns)
                columns.append('volume')

        return super(DateCandleTable, self).read(names, start, end, length, columns)

    def _read(self, name, start, end, length, columns):
        result = super(DateCandleTable, self)._read(name, start, end, length, columns)
        return result[result['volume'] != 0]


class FrameTable(BLPTable):

    def __init__(self, rootdir, index, value, index2blp=None, blp2index=None, value_map=None):
        super(FrameTable, self).__init__(rootdir, index)
        self.value = self.table.cols[value]
        self.index2blp = index2blp
        self.blp2index = blp2index
        self.value_map = value_map

    def read(self, names, start=None, end=None, length=None, columns=None):
        if self.index2blp:
            start = self.index2blp(start)
            end = self.index2blp(end)

        if isinstance(names, six.string_types):
            return self._read(names, start, end, length, columns)
        else:
            return pd.DataFrame({name: self._read(name, start, end, length, columns) for name in names})

    def _read(self, name, start, end, length, columns):
        index_slice = self._index_slice(name, start, end, length)
        return pd.Series(
            self._read_line(index_slice, None),
            self._read_index(index_slice),
            name=name
        )

    def _read_line(self, index, column):
        if self.value_map:
            return np.array(map(self.value_map, self.value[index]))
        else:
            return self.value[index]

    def _read_index(self, index):
        if self.blp2index:
            return np.array(map(self.blp2index, self.index[index]))
        else:
            return self.index[index]


class DateAdjustTable(FrameTable):

    DateFormat = "%Y%m%d"
    GAP = timedelta(hours=15)

    def __init__(self, rootdir):
        super(DateAdjustTable, self).__init__(
            rootdir,
            index='date',
            value='adjust',
            index2blp=lambda date: int(date.strftime(self.DateFormat)) if isinstance(date, datetime) else date,
            blp2index=lambda num: datetime.strptime(str(num), self.DateFormat) + self.GAP,
        )

    def _index_slice(self, name, start, end, length):
        index_slice = super(DateAdjustTable, self)._index_slice(name, start, end, length)
        if index_slice.start > self.line_map[name][0]:
            return slice(index_slice.start-1, index_slice.stop)
        else:
            return index_slice


def convert_date_to_int(dt):
    if isinstance(dt, datetime):
        t = dt.year * 10000 + dt.month * 100 + dt.day
        t *= 1000000
        return t
    else:
        return dt

gap = timedelta(hours=15)


def convert_num_to_date(num):
    return datetime.strptime(str(num), "%Y%m%d000000") + gap


class FactorReader(ClassifiedTable):

    def __init__(self, rootdir):
        super(FactorReader, self).__init__(rootdir, 'tradeDate', convert_date_to_int, convert_num_to_date)
        self.columns = tuple(self.table.attrs['factors'])
        self.ratio = self.table.attrs['default_ratio']

    def read(self, names, start=None, end=None, length=None, columns=None):
        if columns is None:
            columns = self.columns

        return super(FactorReader, self).read(names, start, end, length, columns)

    def _read(self, name, start, end, length, columns):
        return super(FactorReader, self)._read(name, start, end, length, columns).replace(0, np.nan) / self.ratio
