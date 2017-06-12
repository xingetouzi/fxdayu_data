import pandas as pd
import numpy as np


def random_panel(item, major, minor):
    l, w, h = len(major), len(minor), len(item)
    return pd.Panel(
        np.random.random_sample(l*w*h).reshape([h, l, w]),
        item, major, minor
    )


def random_frame(index, columns):
    l, w = len(index), len(columns)
    return pd.DataFrame(
        np.random.random_sample(l*w).reshape([l, w]),
        index, columns
    )