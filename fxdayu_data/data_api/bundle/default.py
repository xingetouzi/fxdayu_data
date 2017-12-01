from fxdayu_data.tools.convert import IntTime
import numpy as np
import pandas as pd


it = IntTime(1, 100, 100, 100, 100, 100, 1000000)


def convert_ints(sequence):
    return [value/10000 for value in sequence]


def convert_times(sequence):
    return list(map(it.trans, sequence))


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


def factor(path, index="datetime"):
    from fxdayu_data.data_api.factor import Factor
    from fxdayu_data.handler.bundle import HandlerTable

    reader = HandlerTable(path, index, convert_frame)
    return Factor(reader)


def bonus(path, index="ex_date", adjust="ex_cum_factor"):
    from fxdayu_data.data_api.bonus import Bonus
    from fxdayu_data.handler.bundle import HandlerTable

    reader = HandlerTable(path, index, convert_bonus)
    return Bonus(reader, adjust)


def candle(bonus, index="datetime", **kwargs):
    from fxdayu_data.data_api.candle import Candle
    from fxdayu_data.handler.bundle import HandlerTable

    return Candle(
        bonus,
        **{freq: HandlerTable(path, index, convert_frame) for freq, path in kwargs.items()}
    )


def info(path):
    from fxdayu_data.data_api.bundle.info import FileInfo

    return FileInfo(path)

