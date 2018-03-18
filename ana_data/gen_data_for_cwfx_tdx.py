# coding: utf-8
"""
把TDX财务数据合并成一个文件
"""

import pandas as pd
import numpy as np
import re
import csv
import io
import time
import traceback
import logging
from pytdx.crawler.history_financial_crawler import HistoryFinancialCrawler, HistoryFinancialListCrawler

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.DEBUG)

if __name__ == '__main__':
    gpcw_df = pd.read_csv(r"..\all_cwsj\gpcw.txt")  # type: pd.DataFrame
    datacrawler = HistoryFinancialCrawler()
    all_gpcw_df = None # type: pd.DataFrame

    # http://www.tdx.com.cn/products/helpfile/tdxw/chm/%E7%AC%AC%E4%B8%89%E7%AB%A0%20%20%20%E5%9F%BA%E7%A1%80%E5%8A%9F%E8%83%BD/3-3/3-3-2/3-3-2-15/3-3-2-15.html
    used_zhibiao_df = pd.DataFrame.from_records(
        [['report_date', 'report_date'],
         ['col40',   '总资产'],
         ['col238',  '总股本'],
         ['col271',  '净资产'],
         ['col96',   '净利润'],
         ['col74',   '营业总收入'],
         ['col1',    '每股收益'],
         ['col2',    '每股扣非收益'],
         ['col5',    '每股公积金'],
         ['col3',    '每股未分配利润'],
         ['col4',    '每股净资产'],
         ['col225',  '每股现金流'],
         ['col183',  '营业收入增长率'],
         ['col189',  '营业利润增长率'],
         ['col184',  '净利润增长率'],
         ['col185',  '净资产增长率'],
         ['col202',  '销售毛利率'],
         ['col199',  '销售净利率'],
         ['col175',  '总资产周转率'],
         ['col167',  '权益乘数'],
         ['col6',    'ROE']], columns=['col', 'name'])

    used_zhibiao_col_list = used_zhibiao_df['col'].tolist()
    used_zhibiao_df = used_zhibiao_df.set_index('col')
    #used_zhibiao_name_list = used_zhibiao_df['name'].tolist()

    for filename in gpcw_df['filename']:
        logging.debug("process file: {}".format(filename))
        with open(r"..\all_cwsj\{}".format(filename), "rb") as fp:
            result = datacrawler.parse(download_file=fp)
            temp_df = datacrawler.to_df(data=result)
            temp_df = temp_df.reindex(columns=used_zhibiao_col_list)
            if all_gpcw_df is None:
                all_gpcw_df = temp_df
            else:
                all_gpcw_df = pd.concat([all_gpcw_df, temp_df], ignore_index = False)


    logging.debug("process data")
    all_gpcw_df = all_gpcw_df.rename(columns=lambda x: used_zhibiao_df.loc[x].iat[0])
    all_gpcw_df = all_gpcw_df.set_index('report_date', append = True)
    # remove duplicated
    all_gpcw_df = all_gpcw_df[~all_gpcw_df.index.duplicated(keep='first')]
    # transpose
    all_gpcw_df = all_gpcw_df.stack(0).unstack(1)

    all_gpcw_df = all_gpcw_df.replace(np.float64.fromhex('-0x1.f1f1f00000000p+114'), np.NaN)
    all_gpcw_df.columns = all_gpcw_df.columns.astype(str)
    date_list = pd.date_range("19700301", "20201231", freq="Q").strftime("%Y%m%d")[::-1]
    all_gpcw_df = all_gpcw_df.reindex(columns=date_list).dropna(axis='columns', how='all')
    all_gpcw_df = all_gpcw_df.rename_axis([u'代码', u'指标'])
    with io.open(r'..\all_other_data\symbol.txt', 'r', encoding='utf-8') as f:
        symbol = [s.strip()[2:] for s in f.readlines()]

    logging.debug("write to file")
    all_gpcw_df.to_csv(r"..\all_other_data\all_zhibiao_for_cwfx_tdx.csv", encoding="gbk", quoting=csv.QUOTE_NONE, index = True)
    logging.debug("end")