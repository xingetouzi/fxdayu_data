from fxdayu_data.data_api.factor import Factor
import pandas as pd



class Bonus(Factor):

    def __init__(self, reader, adjust="ex_cum_factor"):
        super(Bonus, self).__init__(reader)
        self.adjust = adjust

    def cal(self, code, frame, atype="after"):
        if len(frame):
            series = self.read_adjust(code)
            if atype != "after":
                series = series/series.iloc[-1]

            if "volume" in frame:
                v = frame.pop("volume")
                return pd.concat((calculate(series, frame), v), 1)
            else:
                return calculate(series, frame)
        else:
            return frame

    def read_adjust(self, code):
        return self.reader.read(code, (self.adjust,))[self.adjust]


def calculate(series, candle):
    start = locate(series, candle.index[0])
    if in_position(series, start):
        end = locate(series, candle.index[-1])
        for start, end, value in locate_range(series, start, end):
            adjust_price(candle, value, start, end)
    else:
        if len(series):
            candle *= series.iloc[-1]

    return candle


def in_position(series, pos):
    return pos < len(series)


def adjust_price(candle, value, begin, stop):
    begin = candle.index.searchsorted(begin)
    stop = candle.index.searchsorted(stop) if stop is not None else None
    candle.iloc[begin:stop] *= value


def locate_range(series, start, end):
    for i in range(start-1, end):
        try:
            yield series.index[i], series.index[i+1], series.iloc[i]
        except IndexError:
            yield series.index[i], None, series.iloc[i]


def locate(series, value):
    return series.index.searchsorted(value)