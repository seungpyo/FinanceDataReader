import pickle as pkl
import os
from datetime import datetime
import pandas


cwd = os.path.split(__file__)[0]


def date_to_cacheline(d):
    if isinstance(d, str):
        return datetime.strptime(d, '%Y-%m-%d')
    elif isinstance(d, pandas.Timestamp):
        return d.to_pydatetime()
    elif isinstance(d, datetime):
        return d
    else:
        raise TypeError('date_to_cacheline expected str(\'%Y-%m-%d\'), pandas.Timestamp, datetime.datetime, but got{0}'.format(type(d)))

def ticker_cache_init():
    if not os.path.isfile(os.path.join(cwd, 'cache', 'ticker_cache.pkl')):
        ticker_cache = [('000000', date_to_cacheline('2020-08-19'), date_to_cacheline('2020-08-20'))]  # dummy cache line
        with open(os.path.join(cwd, 'cache', 'ticker_cache.pkl'), 'wb') as f:
            pkl.dump(ticker_cache, f)


def ticker_cache_readall():
    with open(os.path.join(cwd, 'cache', 'ticker_cache.pkl'), 'rb') as f:
        ticker_cache = pkl.load(f)
    return ticker_cache


def ticker_cache_write(ticker_cache):
    with open(os.path.join(cwd, 'cache', 'ticker_cache.pkl'), 'wb') as f:
        pkl.dump(ticker_cache, f)


def ticker_cache_delete(idx):
    ticker_cache = ticker_cache_readall()
    del ticker_cache[idx]
    ticker_cache_write(ticker_cache)
