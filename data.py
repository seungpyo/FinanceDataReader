from FinanceDataReader.investing.data import (InvestingDailyReader)
from FinanceDataReader.fred.data import (FredReader)
from FinanceDataReader.krx.data import (KrxDelistingReader)
from FinanceDataReader.naver.data import (NaverDailyReader)
from FinanceDataReader.nasdaq.listing import (NasdaqStockListing)
from FinanceDataReader.krx.listing import (KrxStockListing, KrxDelisting, KrxAdministrative)
from FinanceDataReader.wikipedia.listing import (WikipediaStockListing)
from FinanceDataReader.investing.listing import (InvestingEtfListing)
from FinanceDataReader.naver.listing import (NaverStockListing, NaverEtfListing)
from FinanceDataReader._utils import (_convert_letter_to_num, _validate_dates)
import FinanceDataReader.cache as cache
from FinanceDataReader.cache import *

import os
import re
import pandas as pd
from datetime import datetime, timedelta
from FinanceDataReader._utils import Stopwatch


def DataReader(symbol, start=None, end=None, exchange=None, data_source=None):
    start, end = _validate_dates(start, end)

    use_cache = \
        datetime(end.year, end.month, end.day) != datetime(datetime.today().year, datetime.today().month, datetime.today().day)
    if use_cache:
        cwd = os.path.split(__file__)[0]
        cache.ticker_cache_init()
        ticker_cache = cache.ticker_cache_readall()

        for i, (t, s, e) in enumerate(ticker_cache):
            if t == symbol:
                if s <= start and end <= e:
                    try:
                        df = pd.read_pickle((os.path.join(cwd, 'cache', '{0}.pkl'.format(symbol))))
                    except FileNotFoundError:
                        # Cache line corrupted; refresh!
                        cache.ticker_cache_delete(i)
                        df = DataReader(symbol, start, end, exchange, data_source)
                else:
                    _start = min(start, s)
                    _end = max(end, e)
                    cache.ticker_cache_delete(i)
                    df = DataReader(symbol, _start, _end, exchange, data_source)
                    break
                return df[start:end+timedelta(days=1)]

        ticker_cache.append((symbol, cache.date_to_cacheline(start),  cache.date_to_cacheline(end)))
        cache.ticker_cache_write(ticker_cache)

    # FRED Reader
    if data_source and data_source.upper() == 'FRED':
        ret = FredReader(symbol, start, end, exchange, data_source).read()
        if use_cache:
            ret.to_pickle(os.path.join(cwd, 'cache', '{0}.pkl'.format(symbol)))
        return ret

    # KRX and Naver Finance
    if (symbol[:5].isdigit() and exchange==None) or \
       (symbol[:5].isdigit() and exchange and exchange.upper() in ['KRX', '한국거래소']):
        ret = NaverDailyReader(symbol, start, end, exchange, data_source).read()
        if use_cache:
            ret.to_pickle(os.path.join(cwd, 'cache', '{0}.pkl'.format(symbol)))
        return ret

    # KRX-DELISTINGS
    if (symbol[:5].isdigit() and exchange and exchange.upper() in ['KRX-DELISTING']):
        ret = KrxDelistingReader(symbol, start, end, exchange, data_source).read()
        if use_cache:
            ret.to_pickle(os.path.join(cwd, 'cache', '{0}.pkl'.format(symbol)))
        return ret

    # Investing
    reader = InvestingDailyReader
    df = reader(symbol, start, end, exchange, data_source).read()
    end = min([pd.to_datetime(end), datetime.today()])
    while len(df) and df.index[-1] < end: # issues/30
        more = reader(symbol, df.index[-1] + timedelta(1), end, exchange, data_source).read()
        if len(more) == 0:
            break
        df = df.append(more)
    if use_cache:
        df.to_pickle(os.path.join(cwd, 'cache', '{0}.pkl'.format(symbol)))
    return df

def StockListing(market):
    market = market.upper()
    if market in [ 'NASDAQ', 'NYSE', 'AMEX', 'SSE', 'SZSE', 'HKEX', 'TSE', 'HOSE']:
        return NaverStockListing(market).read()
    if market in [ 'KRX', 'KOSPI', 'KOSDAQ', 'KONEX']:
        return KrxStockListing(market).read()
    if market in [ 'KRX-DELISTING' ]:
        return KrxDelisting(market).read()
    if market in [ 'KRX-ADMINISTRATIVE' ]:
        return KrxAdministrative(market).read()
    if market in [ 'S&P500', 'SP500']:
        return WikipediaStockListing(market).read()
    else:
        msg = "market='%s' is not implemented" % market
        raise NotImplementedError(msg)

def EtfListing(country='KR'):
    if country.upper() == 'KR':
        return NaverEtfListing().read()
    return InvestingEtfListing(country).read()
