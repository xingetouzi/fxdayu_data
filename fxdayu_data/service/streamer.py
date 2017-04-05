import redis
from threading import Thread


class Streamer(object):

    def info(self):
        pass

    def start(self):
        pass

    def stop(self):
        pass


class StockStreamer(Streamer):

    def __init__(self, code, request):
        self.code = code
        self._socket = request
        self._running = False
        self._thread = Thread(target=self._stream)

    def start(self):
        self._running = True
        self._thread.start()

    def stop(self):
        self._running = False
        self._thread.join()

    def _stream(self):
        while self._running:
            pass
