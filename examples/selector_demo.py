from fxdayu_data.selector.selector import Selector, SimpleRule
from fxdayu_data.selector.admin import IntersectionAdmin
import itertools
from datetime import datetime
from talib import abstract


rule = SimpleRule('Friday', isoweekday=5, hour=15, minute=0)


class SingleD(Selector):

    frequency = 'D'

    def __init__(self):
        super(SingleD, self).__init__(rule)


class DayMA(SingleD):

    fast = 10
    slow = 20

    def execute(self, pool, context, data):
        out = []
        for code in pool:
            candle = data.history(code, frequency=self.frequency, length=self.slow+1)
            try:
                fast = abstract.MA(candle, timeperiod=self.fast)
                slow = abstract.MA(candle, timeperiod=self.slow)
                if fast[-1] > slow[-1] and (fast[-2] < slow[-2]):
                    out.append(code)
            except Exception as e:
                print e
                print candle
        print 'MA', out
        return out


class Single1Min(Selector):

    frequency = '1min'

    def __init__(self):
        super(Single1Min, self).__init__(rule)


class DayATR(SingleD):

    period = 10
    length = 5

    def execute(self, pool, context, data):
        out = []
        for code in pool:
            candle = data.history(code, frequency=self.frequency, length=self.period+self.length)
            try:
                atr = abstract.ATR(candle, timeperiod=self.period)
                if atr[-1] > atr.mean():
                    out.append(code)
            except Exception as e:
                print e
                print candle

        print 'ATR', out
        return out

    def select(self, codes, data):
        for code in codes:
            candle = data.history(code, frequency=self.frequency, length=self.period+self.length)
            atr = abstract.ATR(candle, timeperiod=self.period)
            if atr[-1] > atr.mean():
                yield code


if __name__ == '__main__':
    import json

    hs300 = json.load(open('sina_stock.json'))['HS300']

    ia = IntersectionAdmin(DayATR(), DayMA())

    result = ia.run(map(str, hs300), datetime(2016, 1, 1), end=datetime(2016, 12, 30))
    print result
