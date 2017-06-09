# encoding:utf-8


FILE = 'config.py'
api = None


def init_config():
    try:
        execfile(FILE, globals(), globals())
    except Exception as e:
        print e
        return


def set_file(file_path):
    global FILE
    FILE = file_path
    init_config()


def candle(symbols, freq, fields=None, start=None, end=None, length=None):
    pass


def fundamental(symbols, fields, start=None, end=None, length=None):
    pass


init_config()