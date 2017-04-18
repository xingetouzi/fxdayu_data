# encoding:utf-8
import pandas as pd


def mask(frame, condition):
    if isinstance(frame, pd.DataFrame):
        return pd.Series(
            map(condition, frame.iterrows()),
            index=frame.index
        )