from pandas_datareader.data import FredReader
from datetime import datetime
from fxdayu_data import MongoHandler


# step1: Create a function which returns the data(in timeseries) you want
def read_fred(symbols, start, end, **kwargs):
    return FredReader(symbols, start, end).read()


# step2: Create a function which reads and saves the data in db:Fred
def save_fred(handler, collection, symbols, start, end, **kwargs):
    # read data
    data = read_fred(symbols, start, end, **kwargs)
    # save data
    return handler.inplace(data, collection, "Fred")


# step3: Create a function that update one collection
def update_fred(handler, collection):
    # get last record
    last = handler.read(collection, 'Fred', length=1)
    start = last.index[0]
    symbols = last.columns
    end = datetime.now()
    return save_fred(handler, collection, symbols, start, end)


# Use function:save_fred to save symbols in different collection
def save_freds(handler, start, end, **symbols):
    for collection, symbol in symbols.items():
        print(save_fred(handler, collection, symbol, start, end))


# Use function:update_fred to update many collections in db:Fred
def update_freds(handler, *collections):
    for collection in collections:
        print(update_fred(handler, collection))


# Update all collections in db:Fred
def update_all(handler):
    update_freds(handler, *handler.table_names('Fred'))
