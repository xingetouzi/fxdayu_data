# encoding:utf-8
from SocketServer import ThreadingTCPServer, StreamRequestHandler
from fxdayu_data.data.redis_handler import RedisHandler
from fxdayu_data.service.base import Streamer, StreamerHandler, LOCAL
from threading import Thread
from fxdayu_data.service.catch_up import today_1min
import redis
import json
import socket
from threading import RLock


class StockStreamer(Streamer):

    def __init__(self, code, connection_pool=None):
        self.code = code
        if connection_pool is None:
            connection_pool = redis.ConnectionPool()
        self._rds = redis.StrictRedis(connection_pool=connection_pool)
        self.client = RedisHandler(self._rds)
        self._socket = []
        self._running = False
        self._thread = Thread(target=self._stream)
        self._link = 1

    def on_request(self, request):
        self._link += 1

    def cancel_request(self, request):
        self._link -= 1
        if self._link == 0:
            self.stop()

    def start(self):
        if not self._running:
            self._running = True
            self._thread.start()

    def stop(self):
        if self._running:
            self._running = False
            self._thread.join()

    def _stream(self):
        while self._running:
            self.steaming()

    def steaming(self):
        pass


class StockStreamHandler(StreamerHandler):

    def __init__(self, db_connection=None, stream_class=StockStreamer):
        self.conn_pool = db_connection if db_connection else redis.ConnectionPool()
        self.stream_class = stream_class
        self._streamers = {}
        self._sockets = {}

    def set_streamer(self, code, client):
        streamer = self._streamers.get(code, None)
        if streamer is None:
            streamer = self.stream_class(code, self.conn_pool)
            self._streamers[code] = streamer
        streamer.on_request(client)

        self._sockets.setdefault(client, []).append(code)

    def get_streamer(self, code):
        return self._streamers.get(code, None)

    def config(self):
        config = self.conn_pool.connection_kwargs
        config['host'] = socket.gethostbyname(socket.gethostname())
        return config

    def socket_alive(self):
        for sk, codes in self._sockets.copy().items():
            try:
                sk.send('alive')
            except Exception as e:
                for code in codes:
                    self._streamers[code].cancel_request(sk)
                self._sockets.pop(sk)
                print e


class DataRequestHandler(StreamRequestHandler):

    def handle(self):
        request = self.request.recv(1024)
        info = json.loads(request)
        code = info['code']
        if isinstance(code, (list, tuple)):
            for c in code:
                self.server.set_streamer(c, self.request)
        else:
            self.server.set_streamer(code, self.request)
            code = [code]

        config = self.server.config()
        print code
        self.request.send(json.dumps({'code': code, 'config': config}))


class MarketDataServer(ThreadingTCPServer, StockStreamHandler):

    def __init__(self, server_address=LOCAL, RequestHandlerClass=DataRequestHandler, bind_and_activate=True, **kwargs):
        ThreadingTCPServer.__init__(self, server_address, RequestHandlerClass, bind_and_activate)
        StockStreamHandler.__init__(self, **kwargs)


if __name__ == '__main__':
    server = MarketDataServer()
    server.serve_forever()