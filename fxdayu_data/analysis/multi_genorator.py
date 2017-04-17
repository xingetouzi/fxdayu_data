import pandas as pd
from fxdayu_data.data.decorators import value_wrapper


def exhaust(iterable, *iters):
    if len(iters):
        for i in iterable:
            for e in exhaust(*iters):
                e.insert(0, i)
                yield e
    else:
        for i in iterable:
            yield [i]


def match(iterable, *iters):
    for ex in map(exhaust, iterable, *iters):
        for e in ex:
            yield e


def multi_from_frame(frame):
    names = []
    items = []
    for name, item in frame.iteritems():
        names.append(name)
        items.append(item.values)

    return pd.MultiIndex.from_arrays(items, names=names)


multi_exhaust = value_wrapper(list, pd.MultiIndex.from_tuples)(exhaust)
multi_match = value_wrapper(list, pd.MultiIndex.from_tuples)(match)
