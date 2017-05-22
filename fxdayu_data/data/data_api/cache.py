# encoding:utf-8
import pandas as pd
from collections import Iterable


class OutRangeException(Exception):
    pass


class CandleCache(object):

    FIELDS = ("open", "high", "low", "close", "volume")

    def __init__(self):
        self.dct = {}
        self.panel = None

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
