import pandas as pd

from fxdayu_data.handler.mongo_handler import MongoHandler
from connect_sql import read_json, get_db


COLUMNS = ['cal_date', 'yz']


def function(fqyz_config, target_config, source_config):
    fqyz = get_db(**read_json(fqyz_config))
    target = MongoHandler(**read_json(target_config))
    source = MongoHandler(**read_json(source_config))
    for code in source.table_names():
        fq(code, target, source, fqyz.cursor())


def fq(name, target, source, cursor):
    try:
        last = target.read(name, length=1)
        time = last.index[0]
        command = "SELECT cal_date,yz " \
                  "FROM fqyz " \
                  "WHERE cal_date > {} AND gp_code = {}".format(time.strftime("%Y-%m-%d"), name)
    except KeyError:
        command = "SELECT cal_date,yz FROM fqyz WHERE gp_code = '{}'".format(name)

    cursor.execute(command)
    yz = pd.DataFrame(
        list(cursor.fetchall()), columns=COLUMNS
    )
    yz['cal_date'] = pd.to_datetime(yz['cal_date'])
    for doc in get_range(yz, 20):
        print write_fq(name, target, source, **doc)


candle = ['datetime', 'open', 'close', 'high', 'low', 'volume']
price = candle[1:-1]


def write_fq(name, target, source, yz, **kwargs):
    try:
        frame = source.read(name, projection=candle, **kwargs)
    except KeyError:
        return

    for column in price:
        frame[column] = frame[column]*yz

    return target.inplace(frame, name)


def get_range(frame, gap=5):
    time = frame.iloc[0, 0]
    yz = frame.iloc[0, 1]

    count = 0
    for index, value in frame.iloc[1:].iterrows():
        if count < gap:
            if value.iloc[1] == yz:
                count += 1
                continue
            else:
                yield {"start": time, "end": value.iloc[0], "yz": yz}
                count = 0
                time = value.iloc[0]
                yz = value.iloc[1]
        else:
            yield {"start": time, "end": value.iloc[0], "yz": yz}
            count = 0
            time = value.iloc[0]
            yz = value.iloc[1]

    yield {"start": time, "yz": yz}


if __name__ == '__main__':
    function("mysql_config.json", "target.json", "source.json")