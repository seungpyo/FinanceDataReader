import pickle as pkl
import os


cwd = os.path.split(__file__)[0]


def ticker_cache_init():
    if not os.path.isfile(os.path.join(cwd, 'cache', 'ticker_cache.pkl')):
        ticker_cache = [('000000', '2020-08-19', '2020-08-20')]  # dummy cache line
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
