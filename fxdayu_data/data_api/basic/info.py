# encoding:utf-8
from fxdayu_data.data_api.basic import BasicConfig


class BasicInfo(BasicConfig):

    def codes(self, name):
        pass

    def trade_days(self, start=None, end=None, length=None, is_open=None):
        pass

    def classification(self, code=None, classification=None):
        pass

    def factor_description(self, name=None, classification=None):
        pass