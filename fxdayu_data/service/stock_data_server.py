# encoding:utf-8
from SocketServer import ThreadingTCPServer, StreamRequestHandler
from datetime import datetime, timedelta
import socket
import time
import json

address = ("127.0.0.1", 8080)


class Streamer(object):
    def __init__(self, code, client):
        self.code = code
        self.client = client
        self.db = '.'.join((code, client, 'db'))

    @property
    def db_info(self):
        return self.db


class DataRequestHandler(StreamRequestHandler):

    def handle(self):
        request = self.request.recv(1024)
        info = json.loads(request)
        streamer = Streamer(**info)
        self.request.send(streamer.db_info)
        self.server.set_stream(streamer.code, streamer)


class MarketDataServer(ThreadingTCPServer):

    def __init__(self, server_address, RequestHandlerClass, bind_and_activate=True):
        ThreadingTCPServer.__init__(self, server_address, RequestHandlerClass, bind_and_activate)
        self._stock_stream = {}

    def set_stream(self, name, stream):
        self._stock_stream[name] = stream

    def get_stream(self, name):
        return self._stock_stream.get(name, None)

if __name__ == '__main__':
    server = MarketDataServer(address, DataRequestHandler)
    print server
    server.serve_forever()