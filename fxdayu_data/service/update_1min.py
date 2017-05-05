from datetime import datetime
import json
from fxdayu_data.data.collector.stock_data import StockData


start = datetime(2017, 1, 1)


def read_json(json_path):
    try:
        return json.load(open(json_path))
    except IOError:
        import sys
        path = sys.argv[0]
        path = path.split('/')
        path[-1] = json_path
        path = '/'.join(path)
        return json.load(open(path))


if __name__ == '__main__':
    db_config = read_json("db_stock1min.json")
    target = read_json("sina_stock.json")['HS300']
    sd = StockData(**db_config)
    table = sd.client.table_names()
    for code in target:
        if code not in table:
            sd.save_1min(code, start)

    sd.update_1mins()