from setuptools import setup


REQUIRES = ['numpy>=1.11.0',
            'pandas>=0.18.1',
            'requests>=2.12.4',
            'redis>=2.10.5',
            'pymongo>=3.3.0',
            'lxml>=3.7.3',
            'tushare>=0.7.5',
            'pandas-datareader>=0.2.1',
            'backports.functools_lru_cache>=1.4']


setup(
    name="fxdayu_data",
    version="0.1.14",
    packages=["fxdayu_data",
              "fxdayu_data.handler",
              "fxdayu_data.collector",
              "fxdayu_data.data_api",
              "fxdayu_data.tools"],
    install_requires=REQUIRES,
    entry_points={"console_scripts": ["DataAPI = fxdayu_data.api_console:config"]}
)