from datetime import timedelta, datetime

import tushare
import pandas as pd

from fxdayu_data.collector.sina_tick import today_1min, history_1min, get_slice


def ts_code(code):
    if code.startswith('sh'):
        return code[2:]
    elif code.startswith('sz'):
        return code[2:]
    else:
        return code


def get_k_data(code, *args, **kwargs):
    data = tushare.get_k_data(ts_code(code), *args, **kwargs)
    data.index = pd.to_datetime(data.pop('date')) + timedelta(hours=15)
    data.pop('code')
    return data


def save_k_data(handler, code, *args, **kwargs):
    data = get_k_data(code, *args, **kwargs)
    return handler.inplace(data, code)


def update_k_data(handler, code, **kwargs):
    data = handler.read(code, length=1)
    start = data.index[-1]
    kwargs['start'] = start.strftime("%Y-%m-%d")
    return save_k_data(handler, code, **kwargs)


def save_k_datas(handler, codes, t=5, **kwargs):
    from fxdayu_data.collector.quests import QuestHandler
    from functools import partial

    func = partial(save_k_data, handler, **kwargs)
    QuestHandler().put_iter(func, codes).start(t)


def update_k_all(handler, t=5, **kwargs):
    from fxdayu_data.collector.quests import QuestHandler
    from functools import partial

    func = partial(update_k_data, handler, **kwargs)
    QuestHandler().put_iter(func, handler.table_names()).start(t)


def iter_1min(code, start, end=None):
        today = datetime.today()
        if not end:
            end = today
        else:
            if end > today:
                end = today

        slicer = get_slice(code)

        while start.date() < end.date():
            if start.isoweekday() < 6:
                frame = history_1min(code, start)
                if len(frame):
                    yield slicer(frame)
            start += timedelta(1)

        if today.isoweekday() < 6:
            if end.date() == today.date() and (today.hour >= 16):
                frame = today_1min(code)
                if len(frame):
                    yield slicer(frame)


def save_1min(handler, code, start, end=None):
    for data in iter_1min(code, start, end):
        print handler.inplace(data, code)


def update_1min(handler, code, end=datetime.now()):
    delta = timedelta(1)
    last = handler.read(code, length=1).index[-1]
    save_1min(handler, code, last+delta, end)


def save_1mins(handler, codes, t=5, *args, **kwargs):
    from fxdayu_data.collector.quests import QuestHandler
    from functools import partial

    func = partial(save_1min, handler, *args, **kwargs)
    QuestHandler().put_iter(func, codes).start(t)


def update_1mins(handler, codes=None, t=5, *args, **kwargs):
    from fxdayu_data.collector.quests import QuestHandler
    from functools import partial

    if codes is None:
        codes = handler.table_names()

    func = partial(update_1min, handler, *args, **kwargs)
    QuestHandler().put_iter(func, codes).start(t)

