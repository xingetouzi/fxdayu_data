from functools import partial
from datetime import datetime

import pandas as pd
import oandapy

from fxdayu_data.tools.decorators import value_wrapper


def time_index(frame, source='timestamp'):
    frame.index = map(datetime.fromtimestamp, frame[source])
    return frame


def set_time_index(frame, source='time', **kwargs):
    frame.index = pd.to_datetime(frame.pop(source), **kwargs)
    return frame


def data_transfer(**kwargs):
    def transfer(result):
        for doc in result:
            for key, func in kwargs.items():
                doc[key] = func(doc[key])
        return result

    return transfer


class OandaAPI(oandapy.API):

    def __init__(self, environment="practice", access_token=None, headers=None):
        super(OandaAPI, self).__init__(environment, access_token, headers)

    @classmethod
    def read_config(cls, config):
        import json
        return cls(**json.load(open(config)))

    @value_wrapper(pd.DataFrame, set_time_index)
    def get_history(self, instrument, **params):
        if isinstance(params.get('start', None), datetime):
            params['start'] = params['start'].strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        if isinstance(params.get('end', None), datetime):
            params['end'] = params['end'].strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        params.setdefault('candleFormat', 'midpoint')
        params.setdefault('dailyAlignment', 0)
        params.setdefault('alignmentTimezone', 'UTC')

        return super(oandapy.API, self).get_history(instrument=instrument, **params)['candles']

    def range_history(self, instrument, start, end, **params):
        try:
            yield self.get_history(instrument, start=start, end=end, **params)
        except oandapy.OandaError as oe:
            if '5000' in str(oe):
                data = self.get_history(instrument, start=start, count=4999, **params)
                yield data

                start = data.index[-1]
                params['includeFirst'] = 'false'
                for data in self.range_history(instrument, start=start, end=end, **params):
                    yield data
            else:
                raise oe

    def long_history(self, instrument, start, count, **kwargs):
        if count < 5000:
            yield self.get_history(instrument, start=start, count=count)
        else:
            data = self.get_history(instrument, start=start, count=4999, **kwargs)
            yield data

            kwargs['includeFirst'] = 'false'
            start = data.index[-1]
            for data in self.long_history(instrument, start, count-4999, **kwargs):
                yield data

    @value_wrapper(pd.DataFrame, time_index)
    def get_eco_calendar(self, instrument, period=31536000):
        return super(OandaAPI, self).get_eco_calendar(instrument=instrument, period=period)

    @value_wrapper(data_transfer(ncl=int, ncs=int, oi=int, price=float), pd.DataFrame, partial(time_index, source='date'))
    def get_commitments_of_traders(self, instrument, period=31536000):
        return super(OandaAPI, self).get_commitments_of_traders(instrument=instrument, period=period)[instrument]

    @value_wrapper(partial(pd.DataFrame, columns=['timestamp', 'long_position_ratio', 'exchange_rate']), time_index)
    def get_historical_position_ratios(self, instrument, period=31536000):
        return super(
            OandaAPI, self
        ).get_historical_position_ratios(
            instrument=instrument, period=period
        )['data'][instrument]['data']
