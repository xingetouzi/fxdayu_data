from datetime import datetime
from functools import partial
import json
import traceback
import re


TRACE_FORMAT = """File "(.*?)", line (.*?),"""


def log_error(error, f_path='error.log'):
    with open(f_path, 'a+') as log:
        dct = {'datetime': datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f'),
               'type': str(type(error)),
               'error': str(error),
               'traceback': re.findall(TRACE_FORMAT, traceback.format_exc(error), re.S)}
        log.write(json.dumps(dct)+'\n')
