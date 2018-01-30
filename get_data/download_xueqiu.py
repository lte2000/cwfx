
# coding: utf-8

import requests
from multiprocessing import Pool, Queue, Manager
import io
import time


lrb_base_url = 'http://api.xueqiu.com/stock/f10/incstatement.csv?page=1&size=10000&symbol='
llb_base_url = 'http://api.xueqiu.com/stock/f10/cfstatement.csv?page=1&size=10000&symbol='
fzb_base_url = 'http://api.xueqiu.com/stock/f10/balsheet.csv?page=1&size=10000&symbol='

headers = {
    'User-Agent': 'Mozilla/5.0',
    'Cookie': 'xq_a_token=93ef7d84fd99d7b5f81ea4e1442c7252dff29d20'
}


def download_as(url, filename):
    retry = 0
    for retry in range(4):
        try:
            r = requests.get(url, headers=headers)
            r.raise_for_status()
        except Exception as e:
            print e.message
            if retry < 3 and (isinstance(e, requests.ConnectionError) or isinstance(e, requests.Timeout)):
                retry += 1
                sleep_time = retry * 3
                print "sleep {} and retry {}".format(sleep_time, retry)
                time.sleep(sleep_time)
                continue
            else:
                return False
        else:
            with open(filename, 'wb') as f:
                f.write(r.content)
            return True


def download_lrb(symbol):
    url = lrb_base_url + symbol
    filename = symbol + '_lrb.csv'
    if download_as(url, filename):
        return None
    return symbol


def download_fzb(symbol):
    url = fzb_base_url + symbol
    filename = symbol + '_fzb.csv'
    if download_as(url, filename):
        return None
    return symbol


def download_llb(symbol):
    url = llb_base_url + symbol
    filename = symbol + '_llb.csv'
    if download_as(url, filename):
        return None
    return symbol

def test(symbol, q):
    #time.sleep(10)
    #return symbol
    q.put(symbol)
    #q.append(symbol)


if __name__ == '__main__':
    with io.open('symbol.txt', 'r', encoding='utf-8') as f:
        symbol = [s.strip() for s in f.readlines()]

    pool = Pool(5)

    lrb_error = pool.map(download_lrb, symbol)
    lrb_error = [i for i in lrb_error if i is not None]
    fzb_error = pool.map(download_fzb, symbol)
    fzb_error = [i for i in fzb_error if i is not None]
    llb_error = pool.map(download_llb, symbol)
    llb_error = [i for i in llb_error if i is not None]

    lrb_error = pool.map(download_lrb, lrb_error)
    lrb_error = [i for i in lrb_error if i is not None]
    fzb_error = pool.map(download_fzb, fzb_error)
    fzb_error = [i for i in fzb_error if i is not None]
    llb_error = pool.map(download_llb, llb_error)
    llb_error = [i for i in llb_error if i is not None]

    pool.close()
    pool.join()

    if lrb_error:
        print "final lrb download error: ", ",".join(lrb_error)
    if fzb_error:
        print "final fzb download error: ", ",".join(fzb_error)
    if llb_error:
        print "final llb download error: ", ",".join(llb_error)






