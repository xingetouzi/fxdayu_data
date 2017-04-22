from fxdayu_data.selector.selector import Selector, SimpleRule, CustomRule
from fxdayu_data.selector.admin import IntersectionAdmin
from datetime import datetime


class Single(Selector):

    frequency = 'D'

    def __init__(self):
        super(Single, self).__init__(CustomRule('friday', lambda time: True if 20 < time.minute < 30 else False))

    def execute(self, pool, context, data):
        print data.history('000001.SZ', length=5)
        return ['00000z.SZ']


if __name__ == '__main__':
    ia = IntersectionAdmin(Single())

    result = ia.run(['000001.SZ', '600000.SZ'], datetime(2016, 1, 1), end=datetime(2016, 1, 30))

