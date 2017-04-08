# encoding:utf-8
from SocketServer import ThreadingTCPServer, StreamRequestHandler
from fxdayu_data.data.redis_handler import RedisHandler
from fxdayu_data.service.base import Streamer, StreamerHandler
from threading import Thread
from fxdayu_data.service.catch_up import today_1min
import redis
import json
import socket


class StockStreamer(Streamer):

    def __init__(self, code, connection_pool):
        self.code = code
        self._rds = redis.StrictRedis(connection_pool=connection_pool)
        self.client = RedisHandler(self._rds)
        self._socket = {}
        self._running = False
        self._thread = Thread(target=self._stream)

    def on_request(self, request):
        config = {'port': int(self._rds.config_get('port')['port']),
                  'host': socket.gethostbyname(socket.gethostname())}
        request.send(
            json.dumps({'code': self.code,
                        'config': config,
                        'type': 'config'})
        )
        print 'send', self.code

    def start(self):
        self._running = True
        self._thread.start()

    def stop(self):
        self._running = False
        self._thread.join()

    def _stream(self):
        while self._running:
            print today_1min(self.code)


class StockStreamHandler(StreamerHandler):

    def __init__(self, db_connection=None):
        self.conn_pool = db_connection if db_connection else redis.ConnectionPool()
        self._streamers = {}

    def set_streamer(self, code, client):
        streamer = self._streamers.get(code, None)
        if streamer is None:
            streamer = StockStreamer(code, self.conn_pool)
            self._streamers[code] = streamer

    def get_streamer(self, code):
        return self._streamers.get(code, None)

    def config(self):
        config = self.conn_pool.connection_kwargs
        config['host'] = socket.gethostbyname(socket.gethostname())
        return config


address = ('', 8080)


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
        self.request.send(json.dumps({'code': code, 'config': config}))


class MarketDataServer(ThreadingTCPServer, StockStreamHandler):

    def __init__(self, server_address, RequestHandlerClass, bind_and_activate=True):
        ThreadingTCPServer.__init__(self, server_address, RequestHandlerClass, bind_and_activate)
        StockStreamHandler.__init__(self)


if __name__ == '__main__':
    server = MarketDataServer(address, DataRequestHandler)
    server.serve_forever()