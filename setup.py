from setuptools import setup

setup(
    name="fxdayu_data",
    version="0.0.1",
    packages=["fxdayu_data",
              "fxdayu_data.data",
              "fxdayu_data.data.handler",
              "fxdayu_data.data.collector"],
    requires=["pymongo>=3.4.0", "redis>=2.10.5", "tushare>=0.6.8", "requests>=2.12.4"]
)