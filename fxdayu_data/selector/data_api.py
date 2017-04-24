# encoding:utf-8
from fxdayu_data.data.market_data import MarketDataFreq


def create_api(symbols, start, end, selectors):
    config = read_config()
    min_freq = str(min([Frequency(s.frequency) for s in selectors]))
    db_config = config.get('frequency', {}).get(min_freq, {})
    api = MarketDataFreq(**db_config)
    api.init(symbols, start, end, frequency=min_freq)
    return api


def write_config(**kwargs):
    import json
    json.dump(kwargs, open('db_config.json', 'w'))


def read_config():
    import json
    return json.load(open('db_config.json', 'r'))


def update_config(**kwargs):
    import json
    with open('db_config.json', 'r') as f:
        config = json.load(f)
        config.update(kwargs)

    json.dump(config, open('db_config.json', 'w'))


class Frequency(object):

    sorts = ['min', 'H', 'D', 'W']

    def __init__(self, freq):
        self.num, self.pos = self.split(freq)
        self.freq = freq

    def __str__(self):
        return self.freq

    @classmethod
    def split(cls, word):
        w = ''
        n = ''
        for letter in word:
            if letter.isdigit():
                n += letter
            else:
                w += letter
        return int(n) if len(n) else 1, cls.sorts.index(w)

    def __lt__(self, other):
        if self.pos < other.pos:
            return True
        elif self.pos == other.pos:
            if self.num < other.num:
                return True
            else:
                return False
        else:
            return False