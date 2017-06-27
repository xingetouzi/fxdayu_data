# encoding:utf-8
import pandas as pd
import numpy as np


# 以输入的3个向量为轴生成随机Panel
def random_panel(item, major, minor):
    l, w, h = len(major), len(minor), len(item)
    return pd.Panel(
        np.random.random_sample(l*w*h).reshape([h, l, w]),
        item, major, minor
    )


# 以输入的2个向量为轴生成随机DataFrame
def random_frame(index, columns):
    l, w = len(index), len(columns)
    return pd.DataFrame(
        np.random.random_sample(l*w).reshape([l, w]),
        index, columns
    )


# 将具有相同结构的DataFrame合成MultiIndexDataFrame
def pipeline(*args, **kwargs):
    from itertools import chain
    return pd.concat(
        chain(map(lambda f: f.stack(), args),
              map(lambda item: item[1].stack()._set_name(item[0]), kwargs.items())),
        axis=1
    )