# coding: utf-8
"""
生成净利润
"""
import pandas as pd
import numpy as np
import re
import csv
import io
import time
import traceback


if __name__ == '__main__':
    with io.open(r'..\all_other_data\symbol.txt', 'r', encoding='utf-8') as f:
        symbol = [s.strip()[2:] for s in f.readlines()]

    #symbol = symbol[0:10]
    first_df = True
    #date_list = ['20170930']
    #for d in range(2016, 1970, -1):
    #    date_list.append(str(d * 10000 + 1231))
    #date_list = pd.date_range("19700301", "20171001", freq="Q").strftime("%Y%m%d")[::-1]
    date_list = pd.date_range("19700301", "20201231", freq="Q").strftime("%Y%m%d")[::-1]

    for code in symbol:
        try:
            cwb = pd.read_csv(r"..\all_cwsj\{}_lrb.xls".format(code), encoding='gbk', delim_whitespace=True, index_col=0)
        except Exception as e:
            print "read csv error for {}, skipped".format(code)
            traceback.print_exc()
            continue

        try:
            # if cwb.index.contains(u"净利润"):
            #     row_jlr = u"净利润"
            # elif cwb.index.contains(u"五、净利润"):
            #     row_jlr = u"五、净利润"
            row_jlr = None
            for key in [u"归属于母公司所有者的净利润", u"归属于母公司的净利润", u"归属于母公司股东的净利润"]:
                if cwb.index.contains(key):
                    row_jlr = key
                    break
            if row_jlr is None:
                raise Exception("Can't find column 净利润 for {}".format(code))
            cwb = cwb.reindex([row_jlr], columns=date_list).reset_index(drop=True)
            #cwb = cwb.reindex([row_jlr]).reset_index(drop=True)
            cwb.insert(0, u'代码', [code])
            if first_df:
                all_lrb = cwb
                first_df = False
            else:
                all_lrb = all_lrb.append(cwb)
        except Exception as e:
            print "process data error for {}, skipped".format(code)
            traceback.print_exc()
            continue

    all_lrb = all_lrb.dropna(axis='columns', how='all')
    #print all_lrb
    # string_columns = ['代码']
    # all_lrb[string_columns] = all_lrb[string_columns].applymap(
    #     lambda x: '=""' if type(x) is float else '="' + str(x) + '"')
    all_lrb.to_csv(r"..\all_other_data\all_jlr.csv", encoding="gbk", quoting=csv.QUOTE_NONE, index=False)

