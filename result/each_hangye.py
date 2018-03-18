# coding: utf-8
"""

"""
import pandas as pd
import numpy as np
import re
import csv
import io
import time
import traceback
import logging


if __name__ == '__main__':
    filtered_df = pd.read_csv(r"filtered.csv", encoding='gbk', sep='\t', index_col=None, dtype={u'代码': str})
    each_hangye = {}
    for row in filtered_df.itertuples(index=False):
        hangye = row[2]
        code = row[0]
        name = row[1]
        if each_hangye.has_key(hangye):
            if len(each_hangye[hangye]) < 2:
                each_hangye[hangye].append((code, name))
        else:
            each_hangye[hangye] = [(code, name)]

    with io.open("hangye_stock.csv", mode="w", encoding="gbk") as f:
        f.write(u"代码,名称,行业\n")
        for k,v in each_hangye.items():
            for c in v:
                f.write(u"{},{},{}\n".format(k, c[0], c[1]))