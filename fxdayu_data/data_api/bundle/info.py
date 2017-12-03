from fxdayu_data.data_api.basic.info import BasicInfo
from fxdayu_data.tools.convert import convert_time
import pandas as pd
import json
import os


CODES = "codes.json"
TRADE_DAYS = "trade_days.csv"
FACTOR_DESCRIPTION = "factor_description.xlsx"


class FileInfo(BasicInfo):

    def __init__(self, home):
        self.home = home
        self._codes = self.abs(CODES)
        self._trade_days = self.abs(TRADE_DAYS)
        self._description = self.abs(FACTOR_DESCRIPTION)

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

    def factor_description(self, name=None, classification=None):
        dct = {}
        if name:
            dct['name'] = name
        if classification:
            dct["classification"] = classification

        description = self.description()

        return finds_by(description, "name", **dct)

    def description(self):
        return pd.read_excel(self._description)


def finds_by(frame, subset, **kwargs):
    if len(kwargs) == 0:
        return frame
    elif len(kwargs) == 1:
        return find_by(frame, *kwargs.popitem())
    else:
        return pd.concat([find_by(frame, *item) for item in kwargs.items()]).drop_duplicates(subset)


def find_by(frame, name, values):
    if isinstance(frame, pd.DataFrame):
        frame.reset_index()
        return frame.set_index(name, False).loc[values].reset_index(None, True)
