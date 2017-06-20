# encoding:utf-8
try:
    from Queue import Queue, Empty
except ImportError:
    from queue import Queue, Empty
from threading import Thread


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
    def run_quests(cls, quests, t=5):
        qh = cls()
        for quest in quests:
            qh.put(quest)
        qh.start(t)

    @classmethod
    def run_iter(cls, function, iters, how=SINGLE, t=5):
        cls().put_iter(function, iters, how).start(t)

    def put_iter(self, function, iters, how=SINGLE):
        for item in iters:
            args, kwargs = how(item)
            self.put_function(function, *args, **kwargs)
        return self

    def put_function(self, function, *args, **kwargs):
        self.queue.put(Quest(function, *args, **kwargs))

    def put(self, quest):
        self.queue.put(quest)

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
        self.clear()

    def clear(self):
        for name, thread in self._threads.copy().items():
            if not thread.is_alive:
                self._threads.pop(name)

    def run(self):
        while self._running:
            try:
                quest = self.queue.get(timeout=1)
            except Empty:
                break

            if isinstance(quest, Quest):
                try:
                    print(quest.run())
                except Exception as e:
                    print "Exception: {}\n" \
                          "Function: {}\n" \
                          "args: {}\n"\
                          "kwargs: {}".format(e, quest.function, quest.args, quest.kwargs)