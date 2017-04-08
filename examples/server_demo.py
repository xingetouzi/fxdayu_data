from fxdayu_data.service.stock_stream import MarketDataServer, StockStreamer
from fxdayu_data.service.data_client import DataClient
from fxdayu_data.data import MongoHandler
import threading
from datetime import datetime
import time


mh = MongoHandler(host='192.168.0.103', port=30000, db='Stock')


class QuoteStreamer(StockStreamer):
    def __init__(self, code, connection_pool=None):
        super(QuoteStreamer, self).__init__(code, connection_pool)
        self.iter = mh.read(self.code, start=datetime(2016, 1, 1), end=datetime(2016, 1, 10)).iterrows()
        self.start()

    def _stream(self):
        while self._running:
            try:
                self.steaming()
                time.sleep(0.1)
            except StopIteration:
                break

        print self.client.read(self.code)

    def steaming(self):
        dt, values = next(self.iter)
        dct = values.to_dict()
        dct['datetime'] = dt
        self.client.write(dct, self.code)
        self._rds.publish(self.code, dt)


def server():
    mds = MarketDataServer(stream_class=QuoteStreamer)
    mds.serve_forever()


def on_time(result):
    print result


def client():
    dc = DataClient()
    dc.register_handler('on_time', on_time)
    dc.request(('127.0.0.1', 8080), '000001.SZ')


server_thread = threading.Thread(target=server)
client_thread = threading.Thread(target=client)

if __name__ == '__main__':
    server_thread.start()
    client_thread.start()

    client_thread.join()
    server_thread.join()
    # qs = QuoteStreamer('000001.SZ')

