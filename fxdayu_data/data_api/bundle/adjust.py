from fxdayu_data.data_api.basic.adjust import BasicAdjust
from fxdayu_data.data_api.bundle.blp_reader import MapTable
from datetime import datetime


class BLPAdjust(BasicAdjust):

    def __init__(self, table, value="ex_cum_factor"):
        self.table = table
        self.value = value
        self.col = (value,)

    @classmethod
    def dir(cls, rootdir):
        return cls(
            MapTable(rootdir, "start_date", blp2index=int2datetime)
        )

    def read(self, code):
        return self.table.read(code, fields=self.col)[self.value]


def int2datetime(num):
    num /= 1000000
    day = num % 100
    num -= day
    month = num % 10000
    num -= month
    year = num / 10000
    return datetime(int(year), int(month/100), int(day))



if __name__ == '__main__':
    t = MapTable("E:\\rqalpha\\bundle\ex_cum_factor.bcolz", "start_date", blp2index=int2datetime)
    print(BLPAdjust(t).read("000001.XSHE"))