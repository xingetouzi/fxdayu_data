from fxdayu_data.tools.convert import IntTime
from fxdayu_data.handler.bundle import BLPTable
from datetime import datetime
import numpy as np
import pandas as pd


it = IntTime(1, 100, 100, 100, 100, 100, 1000000)
TT = list(reversed(it.d))


def convert_ints(sequence):
    return [value/10000 for value in sequence]


def convert_times(sequence):
    return list(map(it.trans, sequence))


def time2int(time):
    return sum(map(mult, time.timetuple(), TT))


def mult(a, b):
    return a*b


def convert_frame(frame):
    frame.index = convert_times(frame.index)
    return frame.replace(0, np.NaN)/10000.0


BONUS_MAP = dict.fromkeys(['announcement_date', 'closure_date', 'payable_date'], convert_times)
BONUS_MAP.update(dict.fromkeys(['cash_before_tax', 'ex_cum_factor', 'round_lot', 'split_factor'], convert_ints))


def convert_bonus(frame):
    return pd.DataFrame(
        {name: BONUS_MAP[name](item) for name, item in frame.iteritems()},
        convert_times(frame.index)
    ).replace(0, np.NaN)


class ConvertTable(BLPTable):

    def read(self, name, fields=None, start=None, end=None, length=None):
        if isinstance(start, datetime):
            start = time2int(start)

        if isinstance(end, datetime):
            end = time2int(end)

        return super(ConvertTable, self).read(name, fields, start, end, length)

    def _read(self, name, columns, start, end, length):
        return convert_frame(super(ConvertTable, self)._read(name, columns, start, end, length))


class BonusTable(ConvertTable):

    def _read(self, name, columns, start, end, length):
        return convert_bonus(BLPTable._read(self, name, columns, start, end, length))


def factor(path, index="datetime"):
    from fxdayu_data.data_api.factor import Factor

    reader = ConvertTable(path, index)
    return Factor(reader)


def bonus(path, index="ex_date", adjust="ex_cum_factor"):
    from fxdayu_data.data_api.bonus import Bonus

    reader = BonusTable(path, index)
    return Bonus(reader, adjust)


def candle(bonus, index="datetime", **kwargs):
    from fxdayu_data.data_api.candle import Candle

    return Candle(
        bonus,
        **{freq: ConvertTable(path, index) for freq, path in kwargs.items()}
    )


def info(path):
    from fxdayu_data.data_api.bundle.info import FileInfo

    return FileInfo(path)


