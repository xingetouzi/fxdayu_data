from fxdayu_data.data_api.bundle.adjust import BLPAdjust, MapTable, int2datetime
from fxdayu_data.data_api.bundle.candle import BLPCandle, DateCandleTable
from fxdayu_data.data_api.bundle.factor import BLPFactor
from fxdayu_data.data_api.bundle.info import Info


def candle(adj=None, **kwargs):
    from fxdayu_data.data_api.candle import Candle

    return Candle(
        BLPAdjust(MapTable(adj, "start_date", blp2index=int2datetime)),
        **{freq: DateCandleTable(path) for freq, path in kwargs.items()}
    )


def factor(path):
    from fxdayu_data.data_api.bundle.blp_reader import FactorReader
    from fxdayu_data.data_api.factor import Factor

    return Factor(FactorReader(path))


def info(path):
    return Info(path)


__all__ = ["BLPAdjust", "BLPFactor", "BLPCandle", "Info"]

