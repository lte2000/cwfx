
# coding: utf-8

import pandas as pd
import numpy as np
import re
import csv
import io
import time
import traceback


if __name__ == '__main__':
    with io.open('symbol.txt', 'r', encoding='utf-8') as f:
        symbol = [s.strip() for s in f.readlines()]

    #symbol = symbol[0:10]
    first_df = True
    date_list = [20170930]
    for d in range(2016, 1980, -1):
        date_list.append(d * 10000 + 1231)

    for code in symbol:
        try:
            cwb = pd.read_csv(r"..\..\all_cwsj\\" + code + "_lrb.csv", encoding='utf-8', index_col=1)
        except Exception as e:
            print "read csv error for {}, skipped".format(code)
            traceback.print_exc()
            continue

        try:
            if cwb.columns.contains(u"净利润"):
                column_jlr = u"净利润"
            elif cwb.columns.contains(u"五、净利润"):
                column_jlr = u"五、净利润"
            else:
                raise Exception("Can't find column 净利润 for {}".format(code))
            cwb = cwb.reindex(date_list, columns=[column_jlr]).rename_axis("净利润").rename(columns={column_jlr:code})
            if first_df:
                all_lrb = cwb.T
                first_df = False
            else:
                all_lrb = all_lrb.append(cwb.T)
        except Exception as e:
            print "process data error for {}, skipped".format(code)
            traceback.print_exc()
            continue

    all_lrb.to_csv("all_lrb.csv", encoding="gbk", quoting=csv.QUOTE_NONE)

