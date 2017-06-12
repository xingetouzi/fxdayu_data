from setuptools import setup

setup(
    name="fxdayu_data",
    version="0.1.11",
    packages=["fxdayu_data",
              "fxdayu_data.data",
              "fxdayu_data.data.handler",
              "fxdayu_data.data.collector",
              "fxdayu_data.data.data_api"],
    requires=["pymongo", "redis", "tushare", "requests", "backports.functools_lru_cache"]
)