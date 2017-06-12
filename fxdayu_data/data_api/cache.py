# encoding:utf-8
from collections import Iterable

import pandas as pd


class OutRangeException(Exception):
    pass


class CandleCache(object):

    def __init__(self):
        self.dct = {}
        self.panel = pd.Panel()

    def read(self, code=None, fields=None, start=None, end=None, length=None):
        if isinstance(code, (str, unicode)):
            return self.read_frame(code, fields, start, end, length)
        elif isinstance(code, Iterable):
            return self.read_panel(code, fields, start, end, length)
        elif code is None:
            return self.read_panel(slice(None), fields, start, end, length)
        else:
            return self.read_frame(code, fields, start, end, length)

    def read_frame(self, code, fields, start=None, end=None, length=None):
        frame = self.dct[code]

        if len(frame):
            _slice = self._slice(frame.index, start, end, length)
            if fields:
                return frame.ix[_slice, fields]
            else:
                return frame.iloc[_slice]
        else:
            return self.read_panel(code, fields, start, end, length)

    def read_panel(self, codes, fields, start=None, end=None, length=None):
        _slice = self._slice(self.panel.major_axis, start, end, length)

        if fields is None:
            fields = slice(None)
        return self.panel.ix[codes, _slice, fields]

    @staticmethod
    def _slice(index, start, end, length):
        if start:
            s = index.searchsorted(start)
            if end:
                e = index.searchsorted(end, side="right")
                return slice(s, e)
            elif length:
                if length > 1:
                    return slice(s, s+length)
                else:
                    return s
            else:
                return slice(s, None)
        elif end:
            e = index.searchsorted(end, side="right")
            if length:
                if length > 1:
                    return slice(e-length, e)
                else:
                    if e:
                        return e-1
                    else:
                        return e
            else:
                return slice(None, e)
        else:
            e = len(index)
            if length:
                if length > 1:
                    return slice(e-length, e)
                else:
                    if e:
                        return e-1
                    else:
                        return e
            else:
                return slice(None, e)

    def put(self, frames):
        for name, frame in frames.items():
            f = self.dct.get(name, None)
            if f is not None:
                self.dct[name] = pd.concat((f, frame))
            else:
                self.dct[name] = frame

        self.panel = pd.Panel.from_dict(self.dct)

    def pop(self, end):
        for name, frame in self.dct.items():
            self.dct[name] = frame[frame.index > end]

        self.panel = self.panel.iloc[:, self._slice(self.panel.major_axis, end, None, None), :]


if __name__ == '__main__':
    from fxdayu_data.tools.random_data import random_panel
    from datetime import datetime

    minor = ['open', 'high']
    item = ['000001', '000002']
    index1 = pd.date_range(datetime(2016, 1, 1), datetime(2016, 1, 31))
    index2 = pd.date_range(datetime(2016, 2, 1), datetime(2016, 2, 28))
    index3 = pd.date_range(datetime(2016, 3, 1), datetime(2016, 3, 31))

    pl1 = random_panel(item, index1, minor)
    pl2 = random_panel(item, index2, minor)
    pl3 = random_panel(item, index3, minor)

    cache = CandleCache()
    cache.put({name: frame for name, frame in pl1.iteritems()})
    cache.put({name: frame for name, frame in pl2.iteritems()})
    cache.put({name: frame for name, frame in pl3.iteritems()})
    cache.pop(datetime(2016, 1, 31))

    print cache.read(fields='high', length=4)

