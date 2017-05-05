from datetime import datetime
import json
from fxdayu_data.data.collector.stock_data import StockData


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
    config = read_json("db_stock1min.json")
    sd = StockData(**config)
    sd.update_1mins()