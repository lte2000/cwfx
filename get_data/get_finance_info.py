# coding: utf-8


import os

#os.environ['TDX_DEBUG'] = "1"

from pytdx.hq import TdxHq_API
from pytdx.params import TDXParams
import pandas as pd
import numpy as np
import re
import csv
import io
import time
import traceback
import logging


ALL_STOCK_LIST = {}


def get_all_stock_list():
    for market in (TDXParams.MARKET_SZ, TDXParams.MARKET_SH):
        stock_count = TDXHQ.get_security_count(market)
        remain = stock_count
        stock_list = {}
        while remain > 0:
            data = TDXHQ.get_security_list(market, stock_count - remain)
            remain -= len(data)
            for d in data:
                stock_list[d['code']]= d
        if len(stock_list) != stock_count:
            print "market: {}, doesn't get all stock list".format(market)
        ALL_STOCK_LIST[market] = stock_list


if __name__ == '__main__':
    with io.open(r'..\all_other_data\symbol.txt', 'r', encoding='utf-8') as f:
        symbol = [s.strip() for s in f.readlines()]

    TDXHQ = TdxHq_API(raise_exception=True, auto_retry=True)
    if not TDXHQ.connect('121.14.110.200', 443):
        raise Exception("Can't connect.")


    get_all_stock_list()

    #symbol = symbol[0:11]
    first_df = True

    for code in symbol:
        if code[0:2] == 'SH':
            market = 1
        else:
            market = 0
        code = code [2:]
        finance_info = TDXHQ.get_finance_info(market, code)
        if first_df:
            columns = finance_info.keys()
            columns.insert(2, 'name')
            finance_df = pd.DataFrame(columns=columns)
            first_df = False
        values = finance_info.values()
        values.insert(2, ALL_STOCK_LIST[market][code]['name'])
        finance_df.loc[finance_df.shape[0]] = values

    finance_df['province'] = finance_df['province'].map({
        1: '黑龙江',
        2: '新疆',
        3: '吉林',
        4: '甘肃',
        5: '辽宁',
        6: '青海',
        7: '北京',
        8: '陕西',
        9: '天津',
        10: '广西',
        11: '河北',
        12: '广东',
        13: '河南',
        14: '宁夏',
        15: '山东',
        16: '上海',
        17: '山西',
        18: '深圳',
        19: '湖北',
        20: '福建',
        21: '湖南',
        22: '江西',
        23: '四川',
        24: '安徽',
        25: '重庆',
        26: '江苏',
        27: '云南',
        28: '浙江',
        29: '贵州',
        30: '海南',
        31: '西藏',
        32: '内蒙',
    })

    # pytdx bug
    bug_columns = [
    "zhigonggu",
    "zongzichan",
    "liudongzichan",
    "gudingzichan",
    "wuxingzichan",
    "liudongfuzhai",
    "changqifuzhai",
    "zibengongjijin",
    "jingzichan",
    "zhuyingshouru",
    "zhuyinglirun",
    "yingshouzhangkuan",
    "yingyelirun",
    "touzishouyu",
    "jingyingxianjinliu",
    "zongxianjinliu",
    "cunhuo",
    "lirunzonghe",
    "shuihoulirun",
    "jinglirun",
    "weifenpeilirun"
    ]

    for c in bug_columns:
        finance_df[c] = finance_df[c] / 10

    finance_df = finance_df.rename(columns={
        'market':'市场',
        'code':'代码',
        'name':'名称',
        'liutongguben':'流通股本',
        'province':'地区',
        'industry':'行业',
        'updated_date':'更新日期',
        'ipo_date':'上市日期',
        'zongguben':'总股本',
        'guojiagu':'国家股',
        'faqirenfarengu':'发起人法人股',
        'farengu':'法人股',
        'bgu':'B股',
        'hgu':'H股',
        'zhigonggu':'职工股',
        'zongzichan':'总资产',
        'liudongzichan':'流动资产',
        'gudingzichan':'固定资产',
        'wuxingzichan':'无形资产',
        'gudongrenshu':'股东人数',
        'liudongfuzhai':'流动负债',
        'changqifuzhai':'长期负债',
        'zibengongjijin':'资本公积金',
        'jingzichan':'净资产',
        'zhuyingshouru':'主营收入',
        'zhuyinglirun':'主营利润',
        'yingshouzhangkuan':'应收账款',
        'yingyelirun':'营业利润',
        'touzishouyu':'投资收益',
        'jingyingxianjinliu':'经营现金流',
        'zongxianjinliu':'总现金流',
        'cunhuo':'存货',
        'lirunzonghe':'利润总和',
        'shuihoulirun':'税后利润',
        'jinglirun':'净利润',
        'weifenpeilirun':'未分配利润',
        'meigujingzichan':'每股净资产',
        'baoliu2':'baoliu2'
    })


    #print finance_df
    #finance_df.to_csv(r"..\all_other_data\test_finance_info.csv", encoding="gbk", quoting=csv.QUOTE_NONE, index=False)
    # string_columns = ['代码']
    # finance_df[string_columns] = finance_df[string_columns].applymap(
    #     lambda x: '=""' if type(x) is float else '="' + str(x) + '"')
    finance_df.to_csv(r"..\all_other_data\all_finance_info.csv", encoding="gbk", quoting=csv.QUOTE_NONE, index=False)

    TDXHQ.disconnect()


