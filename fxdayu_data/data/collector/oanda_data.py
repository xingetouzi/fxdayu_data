from fxdayu_data.data.collector.base import DataCollector
from fxdayu_data.data.decorators import value_wrapper
from fxdayu_data.data.handler.mongo_handler import MongoHandler
from datetime import datetime
import pandas as pd
import json
import oandapy


def time_index(transfer=datetime.fromtimestamp, source='timestamp', *a, **k):
    def shape(result):
        for doc in result:
            doc['datetime'] = transfer(doc[source], *a, **k)
        return result
    return shape


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

    @value_wrapper(time_index(datetime.strptime, 'time', '%Y-%m-%dT%H:%M:%S.%fZ'), pd.DataFrame)
    def get_history(self, instrument, **params):
        if isinstance(params.get('start', None), datetime):
            params['start'] = params['start'].strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        if isinstance(params.get('end', None), datetime):
            params['end'] = params['end'].strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        params.setdefault('candleFormat', 'midpoint')
        params.setdefault('dailyAlignment', 0)
        params.setdefault('alignmentTimezone', 'UTC')

        if params.pop('all', True):
            try:
                return super(oandapy.API, self).get_history(instrument=instrument, **params)['candles']
            except oandapy.OandaError as oe:
                if '5000' in str(oe):
                    if 'start' in params:
                        return self._get_history(instrument, **params)
                    else:
                        raise ValueError('Requires start if count > 5000')

        else:
            return super(oandapy.API, self).get_history(instrument=instrument, **params)['candles']

    def _get_history(self, instrument, **params):
        if 'count' in params:
            target = params['count']
            params['count'] = 5000
            data = super(oandapy.API, self).get_history(instrument=instrument, **params)['candles']
            count = target - len(data)
            params['includeFirst'] = 'false'
            while count > 5000:
                params['start'] = data[-1]['time']
                data.extend(super(oandapy.API, self).get_history(instrument=instrument, **params)['candles'])
                count = target - len(data)

            if count:
                params['count'] = count
                params['start'] = data[-1]['time']
                data.extend(super(oandapy.API, self).get_history(instrument=instrument, **params)['candles'])

            return data

        end = params.pop('end', None)
        params['count'] = 4999
        data = super(oandapy.API, self).get_history(instrument=instrument, **params)['candles']
        print(data[-1])
        try:
            params['start'] = data[-1]['time']
            params['includeFirst'] = 'false'
            params['end'] = end
            params.pop('count')
            next_data = super(oandapy.API, self).get_history(instrument=instrument, **params)['candles']
        except oandapy.OandaError as oe:
            if '5000' in str(oe):
                next_data = self._get_history(instrument, **params)
            else:
                print(oe)
                return data

        data.extend(next_data)
        return data

    @value_wrapper(time_index(), pd.DataFrame)
    def get_eco_calendar(self, instrument, period=31536000):
        return super(OandaAPI, self).get_eco_calendar(instrument=instrument, period=period)

    @value_wrapper(time_index(source='date'), data_transfer(ncl=int, ncs=int, oi=int, price=float), pd.DataFrame)
    def get_commitments_of_traders(self, instrument, period=31536000):
        return super(OandaAPI, self).get_commitments_of_traders(instrument=instrument, period=period)[instrument]

    @value_wrapper(time_index(), pd.DataFrame)
    def get_historical_position_ratios(self, instrument, period=31536000):
        data = super(OandaAPI, self).get_historical_position_ratios(instrument=instrument, period=period)
        columns = ['timestamp', 'long_position_ratio', 'exchange_rate']

        return [dict(list(map(lambda key, value: (key, value), columns, doc)))
                for doc in data['data'][instrument]['data']]


class OandaData(DataCollector):

    API_MAP = {
        'HPR': 'get_historical_position_ratios',
        'CLD': 'get_eco_calendar',
        'COT': 'get_commitments_of_traders'
    }

    MAIN_CURRENCY = [
        'EUR_USD', 'AUD_USD', 'NZD_USD', 'GBP_USD', 'USD_CAD', 'USD_JPY'
    ]

    default_period = [
        'M15', 'M30', 'H1', 'H4', 'D', 'M'
    ]

    def __init__(self, oanda_info, host='localhost', port=27017, db='Oanda', user=None, **kwargs):
        """

        :param oanda_info: dict, oanda account info {'environment': 'practice', 'access_token': your access_token}
        :return:
        """

        super(OandaData, self).__init__(MongoHandler(host, port, user, db, **kwargs))

        if isinstance(oanda_info, str):
            with open(oanda_info) as info:
                oanda_info = json.load(info)
                info.close()

        self.api = OandaAPI(environment=oanda_info.get('environment', 'practice'),
                            access_token=oanda_info.get('access_token', None))

    def save_history(self, instrument, **kwargs):
        result = self.api.get_history(instrument, **kwargs)
        return self.client.inplace(
            result,
            '.'.join((instrument, kwargs.get('granularity', 'S5'))),
        )

    def save_many(self, instruments, granularity, start, end=None, t=5):
        if isinstance(instruments, list):
            if isinstance(granularity, list):
                self._save_many(
                    start, end, t,
                    [(i, g) for i in instruments for g in granularity]
                )

            else:
                self._save_many(
                    start, end, t,
                    [(i, granularity) for i in instruments]
                )

        else:
            if isinstance(granularity, list):
                self._save_many(
                    start, end, t,
                    [(instruments, g) for g in granularity]
                )

            else:
                self.save_history(instruments, granularity=granularity, start=start, end=end)

    def _save_many(self, start, end, t, i_g):
        for i, g in i_g:
            self.queue.put({
                'instrument': i,
                'granularity': g,
                'start': start,
                'end': end
            })

        self.start(self.save_history, t)
        self.stop()
        self.join()

    def save_main(self, start=datetime(2010, 1, 1), end=datetime.now()):
        self.save_many(self.MAIN_CURRENCY, self.default_period, start, end)

    def update(self, col_name):
        doc = self.client.db[col_name].find_one(sort=[('datetime', -1)], projection=['time'])
        if doc is None:
            raise ValueError('Unable to find the last record or collection: %s, '
                             'please check your DataBase' % col_name)

        i, g = col_name.split('.')
        if g in self.API_MAP:
            return self.save(g, col_name, instrument=i)
        else:
            try:
                return self.save_history(i, granularity=g, start=doc['time'], end=datetime.now(), includeFirst='false')
            except IndexError:
                return '%s already updated to recent' % col_name

    def update_candle(self, i, g, **kwargs):
        return self.save_history(i, granularity=g, **kwargs)

    def update_many(self, col_names=None, t=5):

        if col_names is None:
            col_names = self.client.table_names()

        for col_name in col_names:
            self.queue.put({'col_name': col_name})

        self.start(self.update, t)
        self.stop()
        self.join()

    def save(self, api, collection=None, db=None, **kwargs):
        if collection is None:
            collection = kwargs['instrument'] + '.' + api
        return self.client.inplace(getattr(self.api, self.API_MAP[api])(**kwargs), collection, db)


if __name__ == '__main__':
    api = OandaAPI(access_token="5e74ab5b965db402fc7f0a883f5e5fa9-bb40c1781c0e7fae3376d5aba35e08ac")

