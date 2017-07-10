from fxdayu_data.data_api import BasicConfig
from fxdayu_data.data_api.blp_reader import DateCandleTable, DateAdjustTable


class BLPCandle(BasicConfig):

    def __init__(self):
        self.tables = {}
        self.adjust = None

    def __call__(self, symbols, freq, fields=None, start=None, end=None, length=None, adjust=None):
        candle = self.tables[freq].read(symbols, start, end, length, fields)
        return candle

    def set(self, rootdir, freq):
        self.tables[freq] = DateCandleTable(rootdir)

    def set_adjust(self, rootdir):
        self.adjust = DateAdjustTable(rootdir)
