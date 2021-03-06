
# coding: utf-8

import requests
from multiprocessing import Pool, Queue, Manager
import io
import time
import os
import random
import logging

logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

sleep_list = [0.975472863353, 0.512082061907, 0.963594279676, 0.404375406937, 0.89212466871, 0.408083302649, 0.782775657789, 0.792513790796, 0.984610670022, 0.870269809242, 0.756699973299, 0.357260174607, 0.385469977995, 0.819587823578, 0.784420669891, 0.993681828474, 0.788971213721]
sleep_list_len = len(sleep_list)
current_sleep = 0

random.seed()

lrb_base_url = 'http://money.finance.sina.com.cn/corp/go.php/vDOWN_ProfitStatement/displaytype/4/stockid/{0}/ctrl/all.phtml'
llb_base_url = 'http://money.finance.sina.com.cn/corp/go.php/vDOWN_CashFlow/displaytype/4/stockid/{0}/ctrl/all.phtml'
fzb_base_url = 'http://money.finance.sina.com.cn/corp/go.php/vDOWN_BalanceSheet/displaytype/4/stockid/{0}/ctrl/all.phtml'

headers = {
    'User-Agent': 'Mozilla/5.0',
#    'Cookie': 'xq_a_token=93ef7d84fd99d7b5f81ea4e1442c7252dff29d20'
}


def download_as(url, filename, max_retry=3, retry_count=0):
    if os.path.isfile(filename):
        return True
    result = False
    try:
        #r = None
        r = requests.get(url, headers=headers)
        r.raise_for_status()
    except requests.HTTPError as e:
        # variable r was assigned in this exception
        if 400 <= r.status_code < 500:
            logger.critical(e.message)
            if retry_count < max_retry:
                retry_count += 1
                sleep_time = retry_count * 12 * 60
                logger.info("sleep {0} and retry {1}".format(sleep_time, retry_count))
                time.sleep(sleep_time)
                return download_as(url, filename, max_retry, retry_count)
            else:
                raise
        else:
            logger.error(e.message)
    except (requests.ConnectionError, requests.Timeout) as e:
        logger.error(e.message)
        if retry_count < max_retry:
            retry_count += 1
            sleep_time = retry_count * 3
            logger.info("sleep {0} and retry {1}".format(sleep_time, retry_count))
            time.sleep(sleep_time)
            return download_as(url, filename, max_retry, retry_count)
    except Exception as e:
        logger.error(e.message)
    else:
        with open(filename, 'wb') as f:
            f.write(r.content)
        result = True

    # wait 0.2~2.0 seconds to avoid be banned
    sleep_time = (0.4 + random.random()*1.8)
    #global current_sleep
    #sleep_time = sleep_list[current_sleep % sleep_list_len]
    #current_sleep += 1
    logger.debug("sleep {0}".format(sleep_time))
    time.sleep(sleep_time)
    return result


def download_lrb(symbol):
    symbol = symbol[2:]
    url = lrb_base_url.format(symbol)
    filename = symbol + '_lrb.xls'
    if download_as(url, filename):
        return None
    return symbol


def download_fzb(symbol):
    symbol = symbol[2:]
    url = fzb_base_url.format(symbol)
    filename = symbol + '_fzb.xls'
    if download_as(url, filename):
        return None
    return symbol


def download_llb(symbol):
    symbol = symbol[2:]
    url = llb_base_url.format(symbol)
    filename = symbol + '_llb.xls'
    if download_as(url, filename):
        return None
    return symbol


if __name__ == '__main__':
    with io.open('..\\all_other_data\\symbol.txt', 'r', encoding='utf-8') as f:
        symbol = [s.strip() for s in f.readlines()]

    pool = Pool(1)

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






