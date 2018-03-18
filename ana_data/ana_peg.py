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

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.DEBUG)


def cal_jlr_zengzhanglv(jlr_list, col1, col2, col3):
    kuisun_count = zengzhang_count = zengzhang_total = 0
    for i in range(1, jlr_list.size - 1):
        benqi_jlr = jlr_list.iat[i]
        shangqi_jlr = jlr_list.iat[i+1]
        if np.isnan(benqi_jlr) or np.isnan(shangqi_jlr) or shangqi_jlr <= 0:
            continue
        if benqi_jlr < 0:
            kuisun_count += 1
        zengzhang_total += benqi_jlr / shangqi_jlr - 1
        zengzhang_count += 1
    if zengzhang_count == 0:
        zengzhang_lv = np.nan
    else:
        zengzhang_lv = zengzhang_total / zengzhang_count * 100
    pe = jlr_list.iat[0]
    if pe > 0 and zengzhang_lv > 0:
        peg = pe / zengzhang_lv
    else:
        peg = np.nan
    return pd.Series({col1: zengzhang_lv, col2: kuisun_count, col3: peg})


if __name__ == '__main__':
    logging.debug("read all input start.")
    with io.open(r'..\all_other_data\symbol.txt', 'r', encoding='utf-8') as f:
        symbol = [s.strip()[2:] for s in f.readlines()]

    #symbol = symbol[0:1]

    jlr_df = pd.read_csv(r"..\all_other_data\all_jlr.csv", encoding='gbk', dtype={u'代码': str})
    jlr_df = jlr_df.set_index(keys=u'代码')
    finance_df = pd.read_csv(r"..\all_other_data\all_finance_info.csv", encoding='gbk', dtype={u'代码': str})
    finance_df = finance_df.set_index(keys=u'代码')
    price_df = pd.read_csv(r"..\all_other_data\all_last_price.csv", encoding='gbk', dtype={u'代码': str})
    price_df = price_df.set_index(keys=u'代码')
    hyname_df = pd.read_csv(r"..\all_other_data\tdxzs.cfg", encoding='gbk', sep='|', header=None,
                            names=[u'行业', u'zhishu', u'c1', u'c2', u'c3', u'code'], index_col=u'code',
                            usecols=[u'行业', u'code'])
    hangye_df = pd.read_csv(r"..\all_other_data\tdxhy.cfg", encoding='gbk', sep='|', header=None,
                            names=['code', 'hycode1', 'hycode2'], index_col=False,
                            dtype=str, usecols=[1, 2, 3])
    hangye_df = hangye_df.set_index('code')
    logging.debug("read all input end.")

    logging.debug("data process step 1 start.")
    hangye_df = hangye_df.join(hyname_df, on=u'hycode1').loc[:, [u'行业']]

    col_list = [u'名称', u'行业',
                u'动态PE', u'静态PE', u'TTM PE', u'ROE',
                u'三年PEG', u'五年PEG', u'十年PEG', u'所有PEG',
                u'三年平均增长率', u'五年平均增长率', u'十年平均增长率', u'所有平均增长率',
                u'三年亏损数', u'五年亏损数', u'十年亏损数', u'所有亏损数',
                u'R1', u'R2', u'R3',
                u'地区', u'价格', u'总股本', u'总市值', u'流通股本', u'流通市值', u'未流通占比',
                u'净资产', u'净利润', u'未分配利润', u'资本公积金',
                u'更新日期', u'上市日期', u'股东人数', u'经营现金流', u'总现金流',
                u'RA', u'RB', u'RC']
    # if use update(), need take care of the dtypes
    # ana_df.update(finance_df)
    ana_df = pd.DataFrame(index=jlr_df.index)
    ana_df = ana_df.join(finance_df.reindex(columns=[u'名称', u'地区', u'总股本', u'流通股本',
                                                     u'净资产', u'净利润', u'未分配利润', u'资本公积金',
                                                     u'更新日期', u'上市日期', u'股东人数', u'经营现金流', u'总现金流']))
    ana_df = ana_df[ana_df[u'总股本'] > 0]
    ana_df[u'价格'] = price_df[u'价格']
    ana_df[u'行业'] = hangye_df[u'行业']
    ana_df[u'总市值'] = ana_df[u'总股本'] * ana_df[u'价格']
    ana_df[u'流通市值'] = ana_df[u'流通股本'] * ana_df[u'价格']
    ana_df[u'未流通占比'] = 100 - ana_df[u'流通股本'] / ana_df[u'总股本'] * 100
    ana_df = ana_df.join(jlr_df)

    dongtai_PE_label = '20170930'
    dongtai_PE_bili = pd.Timestamp(dongtai_PE_label).quarter / 4.0
    if pd.Timestamp(dongtai_PE_label).is_year_end:
        jingtai_PE_label = dongtai_PE_label
    else:
        jingtai_PE_label = (pd.Timestamp(dongtai_PE_label)- pd.tseries.offsets.YearEnd(1)).strftime("%Y%m%d")
    # q4 ttm = q4 + last q4 - last q4
    # q3 ttm = q3 + last q4 - last q3
    # q2 ttm = q2 + last q4 - last q2
    # q1 ttm = q1 + last q4 - last q1
    ttm_PE_label1 = dongtai_PE_label
    ttm_PE_label2 = (pd.Timestamp(dongtai_PE_label) -
                     pd.tseries.offsets.YearEnd(1)).strftime("%Y%m%d")
    ttm_PE_label3 = (pd.Timestamp(dongtai_PE_label) -
                     pd.tseries.offsets.DateOffset(years=1)).strftime("%Y%m%d")
    y3_label = pd.date_range(end=jingtai_PE_label, freq='Y', periods=4).strftime("%Y%m%d")[::-1].tolist()
    y5_label = pd.date_range(end=jingtai_PE_label, freq='Y', periods=6).strftime("%Y%m%d")[::-1].tolist()
    y10_label = pd.date_range(end=jingtai_PE_label, freq='Y', periods=11).strftime("%Y%m%d")[::-1].tolist()
    yall_label = pd.date_range(end=jingtai_PE_label, freq='Y', periods=30).strftime("%Y%m%d")[::-1].tolist()

    ana_df = ana_df[ana_df[dongtai_PE_label] != 0][ana_df[jingtai_PE_label] != 0]
    ana_df[u'动态PE'] = ana_df[u'总市值'] / (ana_df[dongtai_PE_label] / dongtai_PE_bili)
    ana_df[u'静态PE'] = ana_df[u'总市值'] / ana_df[jingtai_PE_label]
    ana_df[u'TTM PE'] = ana_df[u'总市值'] / (ana_df[ttm_PE_label1] + ana_df[ttm_PE_label2] - ana_df[ttm_PE_label3])
    ana_df[u'ROE'] = ana_df[dongtai_PE_label] / ana_df[u'净资产'] * 100
    logging.debug("data process step 1 end.")

    logging.debug("data process step 2 start.")

    ana_df = ana_df.merge(
        ana_df.reindex(columns=[u'动态PE']+y3_label).apply(
            cal_jlr_zengzhanglv, axis=1, args=(u'三年平均增长率', u'三年亏损数', u'三年PEG')),
        left_index=True, right_index=True)
    ana_df = ana_df.merge(
        ana_df.reindex(columns=[u'动态PE']+y5_label).apply(
            cal_jlr_zengzhanglv, axis=1, args=(u'五年平均增长率', u'五年亏损数', u'五年PEG')),
        left_index=True, right_index=True)
    ana_df = ana_df.merge(
        ana_df.reindex(columns=[u'动态PE'] + y10_label).apply(
            cal_jlr_zengzhanglv, axis=1, args=(u'十年平均增长率', u'十年亏损数', u'十年PEG')),
        left_index=True, right_index=True)
    ana_df = ana_df.merge(
        ana_df.reindex(columns=[u'动态PE'] + yall_label).apply(
            cal_jlr_zengzhanglv, axis=1, args=(u'所有平均增长率', u'所有亏损数', u'所有PEG')),
        left_index=True, right_index=True)

    logging.debug("data process step 2 end.")

    logging.debug("write file start.")
    columns = col_list
    # columns = ana_df.columns.tolist()
    # for c in col_list:
    #     if c in columns:
    #         columns.remove(c)
    # columns = col_list + sorted(columns,reverse=True)
    ana_df = ana_df.reindex(columns=columns)
    ana_df.to_csv(r"..\result\peg.csv", encoding="gbk", quoting=csv.QUOTE_NONE)
    logging.debug("write file end.")

