import re
from datetime import datetime
from pandas import (to_datetime, Timestamp)
from time import time


class Stopwatch:

    def __init__(self):
        self.t0 = 0
        self.t1 = 0

    def start(self):
        self.t0 = time()

    def stop(self, verbose=True):
        self.t1 = time()
        if verbose:
            print('Elapsed time: {0}'.format(self.t1 - self.t0))


def _convert_letter_to_num(str_num):
    powers = {'B': 10 ** 9, 'M': 10 ** 6, 'K': 10 ** 3, '': 1}
    m = re.search("([0-9\.]+)(M|B|K|)", str_num)
    if m:
        val = m.group(1)
        mag = m.group(2)
        return float(val) * powers[mag]
    return 0.0


def _validate_dates(start, end):
    start = to_datetime(start)
    end = to_datetime(end)

    if start is None:
        start = datetime(1970, 1, 1)
    if end is None:
        end = datetime.today()
    return start, end


