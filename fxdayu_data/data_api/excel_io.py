# encoding:utf-8
from fxdayu_data.data_api import BasicConfig, lru_cache
import pandas as pd


class Excel(BasicConfig):
    def __init__(self):
        self.paths = {}

    @lru_cache(128)
    def __call__(self, name, *sheetname, **kwargs):
        path = self.paths[name]
        ef = pd.ExcelFile(path)
        if len(ef.sheet_names) == 1:
            return ef.parse(**kwargs)
        else:
            if len(sheetname) == 1:
                return ef.parse(sheetname=sheetname[0], **kwargs)
            elif len(sheetname) > 1:
                return {sheet: ef.parse(sheet, **kwargs) for sheet in sheetname}
            else:
                return {sheet: ef.parse(sheet, **kwargs) for sheet in ef.sheet_names}

    def set(self, name, path):
        self.paths[name] = path

    def sets(self, dir_path, *args, **kwargs):
        for arg in args:
            self.paths[arg.split('.')[0]] = '{}{}'.format(dir_path, arg)

        for key, value in list(kwargs.items()):
            self.paths[key] = '{}{}'.format(dir_path, value)

    def set_dir(self, dir_path):
        import os
        for dirpath, dirnames, filenames in os.walk(dir_path):
            self.sets(dirpath, *[name for name in filenames if name.endswith('.xlsx')])