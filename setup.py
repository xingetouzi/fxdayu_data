from setuptools import setup


REQUIRES = open("requirements.txt").readlines()


setup(
    name="fxdayu_data",
    version="0.2.6.3",
    packages=["fxdayu_data",
              "fxdayu_data.handler",
              "fxdayu_data.data_api",
              "fxdayu_data.data_api.basic",
              "fxdayu_data.data_api.mongo",
              "fxdayu_data.data_api.bundle",
              "fxdayu_data.costume",
              "fxdayu_data.costume.file",
              "fxdayu_data.costume.mongo",
              "fxdayu_data.tools"],
    install_requires=REQUIRES,
    entry_points={"console_scripts": ["DataAPI = fxdayu_data.entry_point:api"]},
    url="https://github.com/cheatm/fxdayu_data.git",
    license="Apache License v2",
    author="Cam",
    author_email="cam@fxdayu.com"
)