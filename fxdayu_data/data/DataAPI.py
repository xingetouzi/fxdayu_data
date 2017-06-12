# encoding:utf-8


__all__ = ["init_config", "set_file", "candle", "fundamental"]
FILE = 'config.py'


def init_config():
    try:
        execfile(FILE, globals(), globals())
    except IOError as ioe:
        print(ioe)
        return


def set_file(file_path):
    global FILE
    FILE = file_path
    init_config()


def candle(symbols, freq, fields=None, start=None, end=None, length=None, adjust=None):
    """

    :param symbols: str | list[str]
    :param freq:  str
    :param fields: str | list[str]
    :param start: datetime.datetime
    :param end: datetime.datetime
    :param length: int
    :param adjust: str | None
    :return: pd.DataFrame or pd.Panel
    """
    pass


def fundamental(symbols, fields, start=None, end=None, length=None):
    """

    :param symbols: str | list[str]
    :param fields: str | list[str]
    :param start: datetime.datetime
    :param end: datetime.datetime
    :param length: int
    :return: pd.DataFrame or pd.Panel
    """
    pass


init_config()