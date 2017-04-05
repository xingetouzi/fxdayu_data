# encoding:utf-8


class DataServer(object):

    def listen(self, *args, **kwargs):
        """
        监听请求
        :param args:
        :param kwargs:
        :return:
        """
        raise NotImplementedError("Should implement listen()")


class DataClient(object):

    def request(self, *args, **kwargs):
        """
        请求数据
        :param args:
        :param kwargs:
        :return:
        """
        raise NotImplementedError("Should implement request()")

    def get(self, *args, **kwargs):
        """
        获取数据
        :param args:
        :param kwargs:
        :return:
        """
        raise NotImplementedError("Should implement get()")

