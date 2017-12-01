from datetime import datetime
import pandas as pd


class IntTime(object):

    def __init__(self, start, *mod):
        self.start = start
        self.mod = mod
        self.d = list(self.expand())[:-1]
        self.md = list(zip(self.mod, self.d))
        self.md.reverse()

    def _trans(self, num):
        for m, d in self.md:
            yield int(num/d%m)

    def trans(self, num):
        try:
            return datetime(*self._trans(num))
        except:
            return pd.NaT

    def expand(self):
        start = self.start
        yield start
        for num in self.mod:
            start *= num
            yield start