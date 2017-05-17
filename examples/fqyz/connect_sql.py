import MySQLdb


def read_json(path='mysql_config.json'):
    import json
    try:
        return json.load(open(path))
    except IOError:
        import sys
        source = sys.argv[0].split('/')
        source[-1] = path
        return json.load(open("/".join(source)))


def get_db(**kwargs):
    return MySQLdb.connect(**kwargs)
