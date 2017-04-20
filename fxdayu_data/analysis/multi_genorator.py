import pandas as pd
import numpy as np
from fxdayu_data.data.decorators import value_wrapper
import itertools


def match(iterable, *iters):
    for ex in map(itertools.product, iterable, *iters):
        for e in ex:
            yield e


def multi_from_frame(frame):
    names = []
    items = []
    for name, item in frame.iteritems():
        names.append(name)
        items.append(item.values)

    return pd.MultiIndex.from_arrays(items, names=names)


multi_match = value_wrapper(list, pd.MultiIndex.from_tuples)(match)


def frame_2_multi_series(frame, names=None):
    if isinstance(frame, pd.DataFrame):
        value = np.concatenate(frame.values)
        index = pd.MultiIndex.from_product((frame.index, frame.columns), names=names)
        return pd.Series(value, index)

    else:
        raise TypeError('type of frame should be pandas.DataFrame')


def frames_2_multi_frame(names=None, **frames):
    name, frame = frames.popitem()
    index = pd.MultiIndex((frame.index, frame.columns), names=names)
    dct = {name: np.concatenate(frame.values)}
    shape = frame.shape
    for name_, frame_ in frames.items():
        if frame_.shape == shape:
            dct[name_] = np.concatenate(frame_.values)
        else:
            raise ValueError(
                'shape not match: shape of %s is %s but shape of %s is %s' % (name_, frame_.shape, name, shape)
            )

    return pd.DataFrame(dct, index)


def tsf_2_multi_frame(time_frame, columns=None, names=None):
    first = []
    second = []
    values = []
    for time, frame in time_frame:
        if isinstance(frame, pd.DataFrame):
            first.append([time])
            second.append(frame.index)
            values.append(frame.values)
        else:
            raise TypeError('type of frame in time_frame should be pandas.DataFrame')

    index = multi_match(first, second)
    if names:
        index = index.set_names(names)
    frame = np.concatenate(values)
    return pd.DataFrame(frame, index, columns)


def make_multi_frame(data, **kwargs):
    return pd.DataFrame(
        {name: np.concatenate(func(data).values) for name, func in kwargs.items()},
        pd.MultiIndex.from_product([data.index, data.columns])
    )


def panel_2_multi_frame(panel, names=None):
    if isinstance(panel, pd.Panel):
        return pd.DataFrame(
            {minor: np.concatenate(panel.minor_xs(minor).values) for minor in panel.minor_axis},
            pd.MultiIndex.from_product((panel.major_axis, panel.items), names=names)
        )
    else:
        raise (TypeError("Type of panel should be pandas.Panel"))


def roll_panel(panel, window=1, axis=1):
    if isinstance(panel, pd.Panel):
        slicer = [slice(None), slice(None), slice(None)]
        index = getattr(panel, ['items', 'major_axis', 'minor_axis'][axis])
        for i in range(window, len(index)):
            slicer[axis] = slice(i-window, i)
            yield index[i-1], panel.iloc[tuple(slicer)]
    else:
        raise (TypeError("Type of panel should be pandas.Panel"))
