from collector import DataCollector
from datetime import datetime
import pandas as pd
import json
import oandapy


def frame_shape(create_frame, time_index=lambda frame: list(map(datetime.fromtimestamp, frame['timestamp']))):
    def wrapper(func):
        def wp(*args, **kwargs):

            data = func(*args, **kwargs)
            data = create_frame(data, *args, **kwargs)
            data['datetime'] = time_index(data)
            return data
        return wp
    return wrapper


class OandaAPI(oandapy.API):
    @staticmethod
    def time_index(frame):
        return

    def get_history(self, instrument, **params):
        if isinstance(params.get('start', None), datetime):
            params['start'] = params['start'].strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        if isinstance(params.get('end', None), datetime):
            params['end'] = params['end'].strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        params.setdefault('candleFormat', 'midpoint')
        params.setdefault('dailyAlignment', 0)
        params.setdefault('alignmentTimezone', 'UTC')

        data = super(oandapy.API, self).get_history(instrument=instrument, **params)

        for candle in data['candles']:
            candle['datetime'] = datetime.strptime(candle['time'], '%Y-%m-%dT%H:%M:%S.%fZ')

        return pd.DataFrame(data['candles'])

    def get_eco_calendar(self, instrument, **params):
        return super(OandaAPI, self).get_eco_calendar(instrument=instrument, **params)

    def get_commitments_of_traders(self, instrument, **params):
        return super(OandaAPI, self).get_commitments_of_traders(instrument=instrument, **params)[instrument]

    def get_historical_position_ratios(self, instrument, **params):
        data = super(OandaAPI, self).get_historical_position_ratios(instrument=instrument, **params)
        columns = ['timestamp', 'long_position_ratio', 'exchange_rate']

        return [dict(list(map(lambda key, value: (key, value), columns, doc)))
                for doc in data['data'][instrument]['data']]


class OandaData(DataCollector):

    def __init__(self, oanda_info, host='localhost', port=27017, db='Oanda', user={}, **kwargs):
        """

        :param oanda_info: dict, oanda account info {'environment': 'practice', 'access_token': your access_token}
        :return:
        """

        super(OandaData, self).__init__(host=host, port=port, db=db, user={}, **kwargs)

        if isinstance(oanda_info, str):
            with open(oanda_info) as info:
                oanda_info = json.load(info)
                info.close()

        self.api = oandapy.API(oanda_info['environment'], oanda_info['access_token'])
        self.time_format = '%Y-%m-%dT%H:%M:%S.%fZ'
        self.default_period = [
            'M15', 'M30', 'H1', 'H4', 'D', 'M'
        ]
        self.MAIN_CURRENCY = [
            'EUR_USD', 'AUD_USD', 'NZD_USD', 'GBP_USD', 'USD_CAD', 'USD_JPY'
        ]

    def get_history(self, instrument, **kwargs):
        data_type = kwargs.pop('data_type', 'dict')
        if isinstance(kwargs.get('start', None), datetime):
            kwargs['start'] = kwargs['start'].strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        if isinstance(kwargs.get('end', None), datetime):
            kwargs['end'] = kwargs['end'].strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        kwargs.setdefault('candleFormat', 'midpoint')
        kwargs.setdefault('dailyAlignment', 0)
        kwargs.setdefault('alignmentTimezone', 'UTC')
        print('requiring', kwargs)

        result = self.api.get_history(instrument=instrument, **kwargs)

        for candle in result['candles']:
            candle['datetime'] = datetime.strptime(candle['time'], '%Y-%m-%dT%H:%M:%S.%fZ')

        if data_type == 'DataFrame':
            result['candles'] = pd.DataFrame(result['candles'])

        return result

    def save_history(self, instrument, **kwargs):
        try:
            result = self.get_history(instrument, **kwargs)
        except oandapy.OandaError as oe:
            print (oe.message)
            if oe.error_response['code'] == 36:
                return self.save_div(instrument, **kwargs)
            else:
                raise oe

        saved = self.save(
            result['candles'],
            '.'.join((result['instrument'], result['granularity'])),
        )

        return saved

    def save_div(self, instrument, **kwargs):
        if 'start' in kwargs:
            end = kwargs.pop('end', None)
            kwargs['count'] = 5000
            saved = self.save_history(instrument, **kwargs)

            kwargs.pop('count')
            if end:
                kwargs['end'] = end
            kwargs['start'] = saved[2]
            next_saved = self.save_history(instrument, **kwargs)
            saved[3] += next_saved[3]
            saved[4] += next_saved[4]
            saved[2] = next_saved[2]
            return saved
        else:
            raise ValueError('In save data mode, start is required')

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
        doc = self.db[col_name].find_one(sort=[('datetime', -1)], projection=['time'])
        if doc is None:
            raise ValueError('Unable to find the last record or collection: %s, '
                             'please check your DataBase' % col_name)

        i, g = col_name.split('.')
        if g == 'COT':
            return self.save(self.get_cot(i), col_name)
        elif g == 'HPR':
            return self.save(self.get_hpr(i), col_name)
        elif g == 'CLD':
            return self.save(self.get_calender(i), col_name)
        else:
            return self.save_history(i, granularity=g, start=doc['time'], includeFirst=False)

    def update_candle(self, i, g, **kwargs):
        return self.save_history(i, granularity=g, **kwargs)

    def update_many(self, col_names=[], t=5):
        if len(col_names) == 0:
            col_names = self.db.collection_names()

        for col_name in col_names:
            self.queue.put({'col_name': col_name})

        self.start(self.update, t)
        self.stop()
        self.join()

    def get_hpr(self, instrument, period=31536000, **kwargs):
        hpr = self.api.get_historical_position_ratios(instrument=instrument, period=period, **kwargs)
        hpr = pd.DataFrame(
            hpr['data'][instrument]['data'],
            columns=['timestamp', 'long_position_ration', 'exchange_rate']
        )
        hpr['datetime'] = list(map(datetime.fromtimestamp, hpr['timestamp']))
        return hpr

    def get_cot(self, instrument, **kwargs):
        cot = self.api.get_commitments_of_traders(instrument=instrument, **kwargs)
        cot = pd.DataFrame(cot[instrument])
        cot['datetime'] = list(map(datetime.fromtimestamp, cot['date']))
        return cot

    def get_calender(self, instrument, period=31536000):
        cal = self.api.get_eco_calendar(instrument=instrument, period=period)
        cal = pd.DataFrame(cal)
        cal['datetime'] = list(map(datetime.fromtimestamp, cal['timestamp']))
        return cal

if __name__ == '__main__':
    api = OandaAPI(access_token="5f04b28550036468bc5940e3e901e096-fcce6a0acfa58c9602991783b01a9fc4")
    # print api.get_history(instrument='EUR_USD', granularity='D', start=datetime(2017, 1, 1), end=datetime.now())
    print api.get_historical_position_ratios(instrument='EUR_USD')