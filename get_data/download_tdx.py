# coding: utf-8

import pandas as pd
import numpy as np
import re
import csv
import io
import time
import traceback
from pytdx.crawler.base_crawler import demo_reporthook
from pytdx.crawler.history_financial_crawler import HistoryFinancialCrawler, HistoryFinancialListCrawler
import logging

logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=logging.DEBUG)

crawler = HistoryFinancialListCrawler()
list_data = crawler.fetch_and_parse()
list_df = pd.DataFrame(list_data)
try:
    old_list_df = pd.read_csv(r"..\all_cwsj\gpcw.txt")
    temp_list_df = list_df.merge(old_list_df, how='outer', on='filename')
    temp_list_df = temp_list_df[temp_list_df['hash_x'] != temp_list_df['hash_y']]
except:
    temp_list_df = list_df

if temp_list_df.empty:
    logging.info('No update.')
    exit()
else:
    list_df.to_csv(r"..\all_cwsj\gpcw.txt", encoding="gbk", quoting=csv.QUOTE_NONE,index=False)
    filename_list = temp_list_df['filename']

crawler = HistoryFinancialCrawler()
for filename in filename_list:
    logging.info("download {}".format(filename))
    result = crawler.fetch_and_parse(filename=filename, path_to_download=r"..\all_cwsj\{}".format(filename))

logging.info("end")

#pd.set_option('display.max_columns', None)
#result = datacrawler.fetch_and_parse(reporthook=demo_reporthook, filename='gpcw20000930.zip', path_to_download=".\\gpcw20000930.zip")
#print(datacrawler.to_df(data=result))