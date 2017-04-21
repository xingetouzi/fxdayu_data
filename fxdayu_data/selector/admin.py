# encoding:utf-8
from collections import OrderedDict
from fxdayu_data.selector.data_api import create_api


def sort_selector(selectors):
    dct = OrderedDict()

    for s in sorted(selectors):
        dct.setdefault(s.rule, []).append(s)

    return dct


class Admin(object):
    def __init__(self, *selectors):
        self.sorted = sort_selector(selectors)
        self.selectors = selectors

    def on_time(self, time, context, data):
        return []

    def run(self, symbols, start, end):
        import pandas as pd
        context = object()
        api = create_api(symbols, start, end, self.selectors)
        results = []
        index = []
        for time in api.all_time:
            result = self.on_time(time, context, api)
            if len(result):
                index.append(time)
                results.append(result)
        return pd.Series(results, index)
    

class IntersectionAdmin(Admin):
    def on_time(self, time, context, data):
        for rule, selectors in self.sorted.items():
            if rule.match(time):
                pool = data.can_trade()
                for selector in selectors:
                    pool = selector.execute(pool, data)
                context.pool = pool
                return pool
        else:
            return []


class UnionAdmin(Admin):
    def on_time(self, time, context, data):
        for rule, selectors in self.sorted.items():
            if rule.match(time):
                pool = data.can_trade()
                result = []
                for selector in selectors:
                    result = filter(lambda x: x not in result, selector.execute(pool))
                context.pool = result
                return result
        else:
            return []
