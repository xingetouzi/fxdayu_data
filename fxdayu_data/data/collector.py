from threading import Thread
try:
    from Queue import Queue, Empty
except ImportError:
    from queue import Queue, Empty


class DataCollector(object):
    def __init__(self, client):
        self.client = client

        self._running = False
        self.queue = Queue()
        self._threads = {}

    def run(self, function):
        while self._running or self.queue.qsize():
            try:
                params = self.queue.get(timeout=1)
            except Empty:
                continue
            result = function(**params)
            if result is not None:
                print(result)

    def start(self, function, t=5):
        self._running = True
        for i in range(0, t):
            thread = Thread(target=self.run, args=[function])
            thread.start()
            self._threads[thread.name] = thread

    def join(self):
        for name, thread in self._threads.items():
            thread.join()

        while len(self._threads):
            self._threads.popitem()

    def stop(self):
        self._running = False