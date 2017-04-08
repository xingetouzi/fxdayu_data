# encoding:utf-8
from fxdayu_data.data.redis_handler import RedisHandler
import socket
import json
import threading
import redis


class RemoteRedis(object):

    def __init__(self):
        if not hasattr(self, 'client'):
            self.client = redis.StrictRedis()
        self.pubsub = None
        self.stream = threading.Thread(target=self.streaming)
        self.listening = False
        self.handlers = {}
        self.result_type = {'config': self.listen}

    def register_handler(self, name, handler):
        self.handlers[name] = handler

    def request(self, address, *codes):
        sk = socket.socket()
        sk.connect(address)
        sk.send(json.dumps({'code': codes}))

        result = sk.recv(1024)

        self.listen(json.loads(result))

    def listen(self, result):
        if self.pubsub is None:
            self.client = redis.StrictRedis(**result['config'])
            self.pubsub = self.client.pubsub()

        self.pubsub.subscribe(*result['code'])

        if not self.listening:
            self.start()

    def streaming(self):
        listen = self.pubsub.listen()

        while self.listening:
            result = next(listen)
            for handler in self.handlers.values():
                handler(result)

    def start(self):
        self.listening = True
        self.stream.start()

    def stop(self):
        self.listening = False
        self.stream.join()


class DataClient(RedisHandler, RemoteRedis):

    def __init__(self, redis_client=None, transformer=None, **kwargs):
        super(DataClient, self).__init__(redis_client, transformer, **kwargs)
        RemoteRedis.__init__(self)


if __name__ == '__main__':
    dc = DataClient()
    dc.request(('127.0.0.1', 8080), '000001', '000002', '000003')