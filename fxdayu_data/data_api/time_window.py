# encoding:utf-8


class TimeWindow(object):

    def __init__(self, start, end, gap):
        self.start = start
        self.end = end
        self.gap = gap
        self.left = start
        self.right = start + gap

    def __iter__(self):
        return self

    @property
    def window(self):
        return self.left, self.right

    def roll(self):
        self.left = self.right
        self.right += self.gap

    def next(self):
        window = self.window

        if window[1] < self.end:
            self.roll()
            return window
        elif window[0] < self.end:
            self.roll()
            return window[0], self.end
        else:
            raise StopIteration()
