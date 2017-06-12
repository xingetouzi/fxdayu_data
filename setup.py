from setuptools import setup


REQUIRES = ['numpy==1.12.0',
            'pandas==0.18.1',
            'requests==2.12.4',
            'redis==2.10.5',
            'pymongo>=3.4.0',
            'lxml==3.7.3',
            'tushare==0.7.5',
            'pandas-datareader==0.2.1',
            'backports.functools_lru_cache>=1.4']


setup(
    name="fxdayu_data",
    version="0.1.11",
    packages=["fxdayu_data",
              "fxdayu_data.data",
              "fxdayu_data.data.handler",
              "fxdayu_data.data.collector",
              "fxdayu_data.data.data_api"],
    install_requires=REQUIRES
)