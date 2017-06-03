from setuptools import setup

setup(
    name="fxdayu_data",
    version="0.0.1",
    packages=["fxdayu_data",
              "fxdayu_data.data",
              "fxdayu_data.data.handler",
              "fxdayu_data.data.collector"],
    requires=["pymongo", "redis", "tushare", "requests"]
)