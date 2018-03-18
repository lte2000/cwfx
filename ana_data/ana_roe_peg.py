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

    zhibiao_df = pd.read_csv(r"..\all_other_data\all_zhibiao_for_roe_peg.csv", encoding='gbk', dtype={u'代码': str}) # type: pd.DataFrame
    zhibiao_df = zhibiao_df.set_index(keys=[u'代码', u'指标'])
    #zhibiao_df = zhibiao_df.reindex(index=[u'000001', u'000002', u'000003', u'000004'], level=0)

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

    ana_df = pd.DataFrame(index=zhibiao_df.index.levels[0])


    # generate data label
    dongtai_label = '20170930'
    if pd.Timestamp(dongtai_label).is_year_end:
        jingtai_label = dongtai_label
    else:
        jingtai_label = (pd.Timestamp(dongtai_label)- pd.tseries.offsets.YearEnd(1)).strftime("%Y%m%d")
    # q4 ttm = q4 + last q4 - last q4
    # q3 ttm = q3 + last q4 - last q3
    # q2 ttm = q2 + last q4 - last q2
    # q1 ttm = q1 + last q4 - last q1
    ttm_label1 = dongtai_label
    ttm_label2 = (pd.Timestamp(dongtai_label) -
                     pd.tseries.offsets.YearEnd(1)).strftime("%Y%m%d")
    ttm_label3 = (pd.Timestamp(dongtai_label) -
                     pd.tseries.offsets.DateOffset(years=1)).strftime("%Y%m%d")
    y4_label = pd.date_range(end=jingtai_label, freq='Y', periods=4).strftime("%Y%m%d")[::-1].tolist()
    y3_label = y4_label[:-1]
    y6_label = pd.date_range(end=jingtai_label, freq='Y', periods=6).strftime("%Y%m%d")[::-1].tolist()
    y5_label = y6_label[:-1]
    y11_label = pd.date_range(end=jingtai_label, freq='Y', periods=11).strftime("%Y%m%d")[::-1].tolist()
    y10_label = y11_label[:-1]
    yall_label = pd.date_range(end=jingtai_label, freq='Y', periods=30).strftime("%Y%m%d")[::-1].tolist()


    jlr_df = zhibiao_df.reindex(index=[u'净利润'], level=1).reset_index(level=1, drop=True)
    jlr_df[u'动态比例'] = jlr_df[ttm_label3] / jlr_df[ttm_label2]

    temp_df = zhibiao_df.reindex(index=[u'每股净利润'], level=1).reset_index(level=1, drop=True)
    temp_df[u'价格'] = price_df[u'价格']
    temp_df = temp_df[(temp_df[dongtai_label] != 0) & (temp_df[jingtai_label] != 0)]
    ana_df[u'PE(动)'] = temp_df[u'价格'] / (temp_df[dongtai_label] / jlr_df[u'动态比例'])
    ana_df[u'PE(静)'] = temp_df[u'价格'] / temp_df[jingtai_label]
    ana_df[u'PE(TTM)'] = temp_df[u'价格'] / (temp_df[ttm_label1] + temp_df[ttm_label2] - temp_df[ttm_label3])

    temp_df = zhibiao_df.reindex(index=[u'ROE'], level=1).reset_index(level=1, drop=True)
    ana_df[u'ROE(动)'] = temp_df[dongtai_label] * temp_df[ttm_label2] / temp_df[ttm_label3] * 100
    ana_df[u'ROE(静)'] = temp_df[jingtai_label] * 100
    ana_df[u'ROE(三)'] = temp_df[y3_label].sum(axis=1, skipna=False) / 3 * 100
    ana_df[u'ROE(五)'] = temp_df[y5_label].sum(axis=1, skipna=False) / 5 * 100
    ana_df[u'ROE(十)'] = temp_df[y10_label].sum(axis=1, skipna=False) / 10 * 100

    temp_df = zhibiao_df.reindex(index=[u'销售净利率'], level=1).reset_index(level=1, drop=True)
    ana_df[u'销售净利率'] = temp_df[dongtai_label] * 100

    temp_df = zhibiao_df.reindex(index=[u'总资产周转率'], level=1).reset_index(level=1, drop=True)
    ana_df[u'总资产周转率'] = temp_df[dongtai_label] * temp_df[ttm_label2] / temp_df[ttm_label3] * 100

    temp_df = zhibiao_df.reindex(index=[u'权益乘数'], level=1).reset_index(level=1, drop=True)
    ana_df[u'权益乘数'] = temp_df[dongtai_label]

    logging.debug("data process step 1 end.")

    logging.debug("data process step 2 start.")
    temp_df = zhibiao_df.reindex(index=[u'每股净利润'], level=1).reset_index(level=1, drop=True)
    #temp_df = zhibiao_df.reindex(index=[u'净利润'], level=1).reset_index(level=1, drop=True)
    temp_df[u'PE(动)'] = ana_df[u'PE(动)']
    ana_df = ana_df.join(
        temp_df.reindex(columns=[u'PE(动)']+y4_label).apply(
            cal_jlr_zengzhanglv, axis=1, args=(u'净利润增长率(三)', u'亏损数(三)', u'PEG(三)')))
    ana_df = ana_df.join(
        temp_df.reindex(columns=[u'PE(动)']+y6_label).apply(
            cal_jlr_zengzhanglv, axis=1, args=(u'净利润增长率(五)', u'亏损数(五)', u'PEG(五)')))
    ana_df = ana_df.join(
        temp_df.reindex(columns=[u'PE(动)'] + y11_label).apply(
            cal_jlr_zengzhanglv, axis=1, args=(u'净利润增长率(十)', u'亏损数(十)', u'PEG(十)')))
    ana_df = ana_df.join(
        temp_df.reindex(columns=[u'PE(动)'] + yall_label).apply(
            cal_jlr_zengzhanglv, axis=1, args=(u'净利润增长率(全)', u'亏损数(全)', u'PEG(全)')))
    logging.debug("data process step 2 end.")

    ana_df = ana_df.join(finance_df.reindex(columns=[u'名称', u'地区', u'上市日期']))
    ana_df[u'行业'] = hangye_df[u'行业']


    logging.debug("write file start.")
    col_list = [u'名称', u'行业', u'地区', u'上市日期', u'PE(动)', u'PE(静)', u'PE(TTM)',
                u'ROE(动)', u'ROE(静)', u'ROE(三)', u'ROE(五)', u'ROE(十)', u'销售净利率', u'总资产周转率', u'权益乘数',
                u'PEG(三)', u'PEG(五)', u'PEG(十)', u'PEG(全)',
                u'净利润增长率(三)', u'净利润增长率(五)', u'净利润增长率(十)', u'净利润增长率(全)',
                u'亏损数(三)', u'亏损数(五)', u'亏损数(十)', u'亏损数(全)']
    ana_df = ana_df.reindex(columns=col_list)
    ana_df.to_csv(r"..\result\roe_peg.csv", encoding="gbk", quoting=csv.QUOTE_NONE)
    logging.debug("write file end.")
