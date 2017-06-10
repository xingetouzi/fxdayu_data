# encoding:utf-8


def ensure_index(index, default=None):
    def generate(iterable):
        yield index
        for i in iterable:
            yield i

    def indexer(origin):
        if isinstance(origin, str):
            return index, origin
        elif origin is None:
            return default
        elif index not in origin:
            return list(generate(origin))
        else:
            return origin

    return indexer

