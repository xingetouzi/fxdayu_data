from fxdayu_data.data_api.basic.info import BasicInfo
from fxdayu_data.tools.convert import convert_time
import pandas as pd
import json
import os


CODES = "codes.json"
TRADE_DAYS = "trade_days.csv"


class FileInfo(BasicInfo):

    def __init__(self, home, codes=CODES, trade_days=TRADE_DAYS):
        self.home = home
        self._codes = self.abs(codes)
        self._trade_days = self.abs(trade_days)

    def abs(self, path):
        return path if os.path.isabs(path) else os.path.join(self.home, path)

    def codes(self, name):
        return self.read_codes()[name]

    def read_codes(self):
        with open(self._codes) as f:
            return json.load(f)

    def classification(self, code=None, classification=None):
        pass

    def trade_days(self, start=None, end=None, length=None, is_open=None):
        days = self.read_trade_days()
        if is_open is not None:
            days = days[days["isOpen"]==is_open]

        if end:
            e = days.index.searchsorted(convert_time(end))
        else:
            e = len(days)

        if start:
            s = days.index.searchsorted(convert_time(start))
        else:
            s = 0

        if not (start and end):
            if length:
                if start:
                    e = s + length
                else:
                    s = e - length

        return days.iloc[s:e, 0]

    def read_trade_days(self):
        return pd.read_csv(self._trade_days, index_col="datetime").rename_axis(pd.to_datetime)


if __name__ == '__main__':
    info = Info("D:\\bundle.2017-11-30\info")
    print(info.codes('hs300'))