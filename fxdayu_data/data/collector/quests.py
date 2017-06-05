# encoding:utf-8
try:
    from Queue import Queue, Empty
except ImportError:
    from queue import Queue, Empty
from threading import Thread
from functools import partial


SINGLE = lambda i: ((i,), {})
ARGS = lambda i: (i, {})
KWARGS = lambda i: ((), i)
AK = lambda i: i


class Quest:

    def __init__(self, function, *args, **kwargs):
        self.function = function
        self.args = args
        self.kwargs = kwargs

    def run(self):
        return self.function(*self.args, **self.kwargs)


class QuestHandler(object):

    def __init__(self):
        self.queue = Queue()
        self._threads = {}
        self._running = False

    @classmethod
    def iter_run(cls, function, iters, how=SINGLE, t=5):
        cls().iter_put(function, iters, how).start(t)

    def partial_put(self, function, partials, iters, how):
        args, kwargs = how(partials)
        func = partial(function, *args, **kwargs)
        self.iter_put(func, iters, how)
        return self

    def iter_put(self, function, iters, how=SINGLE):
        for item in iters:
            args, kwargs = how(item) 
            self.queue.put(
                Quest(function, *args, **kwargs)
            )
        return self

    def put_quest(self, function, *args, **kwargs):
        self.queue.put(Quest(function, *args, **kwargs))
        return self

    def put(self, quest):
        self.queue.put(quest)
        return self

    @property
    def running(self):
        return self._running

    def start(self, n=5):
        self._running = True
        for i in range(n):
            thread = Thread(target=self.run)
            self._threads[thread.getName()] = thread
            thread.start()

    def join(self):
        for name, thread in self._threads.items():
            thread.join()

    def stop(self):
        self._running = False
        self.join()
        while len(self._threads):
            self._threads.popitem()

    def run(self):
        while self._running:
            try:
                quest = self.queue.get(timeout=1)
            except Empty:
                break

            if isinstance(quest, Quest):
                print quest.run()