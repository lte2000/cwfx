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

logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=logging.DEBUG)


def cal_jlr_zengzhanglv(data, col1, col2):
    kuisun_count = 0
    if not (data.iat[0] > 0 and data.iat[-1] > 0):
        fhzzl = np.nan
    else:
        # 复合增长率
        fhzzl = ((data.iat[0] / data.iat[-1]) ** (1.0 / (len(data) - 1)) - 1) * 100
    for d in data[:-1]:
        if d < 0:
            kuisun_count += 1
    return pd.Series({col1: fhzzl, col2: kuisun_count})


def cal_PEG(data, col1, col2):
    # data.iat[0] is PE
    if not (data.iat[0] > 0 and data.iat[1] > 0 and data.iat[-1] > 0):
        peg = np.nan
        fhzzl = np.nan
    else:
        # 复合增长率
        fhzzl = ((data.iat[1] / data.iat[-1]) ** (1.0 / (len(data) - 2)) - 1) * 100
        if fhzzl == 0:
            peg = np.nan
        else:
            peg = data.iat[0] / fhzzl
    return pd.Series({col1: fhzzl, col2: peg})


def generate_date_label(start_label):
    dongtai_label = start_label
    if pd.Timestamp(dongtai_label).is_year_end:
        jingtai_label = dongtai_label
    else:
        jingtai_label = (pd.Timestamp(dongtai_label)- pd.offsets.YearEnd(1)).strftime("%Y%m%d")
    # q4 ttm = q4 + last q4 - last q4
    # q3 ttm = q3 + last q4 - last q3
    # q2 ttm = q2 + last q4 - last q2
    # q1 ttm = q1 + last q4 - last q1
    ttm_label1 = dongtai_label
    ttm_label2 = (pd.Timestamp(dongtai_label) -
                     pd.offsets.YearEnd(1)).strftime("%Y%m%d")
    ttm_label3 = (pd.Timestamp(dongtai_label) -
                     pd.offsets.DateOffset(years=1)).strftime("%Y%m%d")
    y4_label = pd.date_range(end=jingtai_label, freq='Y', periods=4).strftime("%Y%m%d")[::-1].tolist()
    y3_label = y4_label[:-1]
    y6_label = pd.date_range(end=jingtai_label, freq='Y', periods=6).strftime("%Y%m%d")[::-1].tolist()
    y5_label = y6_label[:-1]
    y11_label = pd.date_range(end=jingtai_label, freq='Y', periods=11).strftime("%Y%m%d")[::-1].tolist()
    y10_label = y11_label[:-1]
    yall_label = pd.date_range(end=jingtai_label, freq='Y', periods=30).strftime("%Y%m%d")[::-1].tolist()
    return dongtai_label, jingtai_label, ttm_label1, ttm_label2, ttm_label3, \
           y3_label, y4_label, y5_label, y6_label, y10_label, y11_label, yall_label


if __name__ == '__main__':
    logging.debug("read all input.")
    with io.open(r'..\all_other_data\symbol.txt', 'r', encoding='utf-8') as f:
        symbol = [s.strip()[2:] for s in f.readlines()]
    # symbol = symbol[0:1]

    zhibiao_df = pd.read_csv(r"..\all_other_data\all_zhibiao_for_cwfx_tdx.csv", encoding='gbk', dtype={u'代码': str})
    zhibiao_df = zhibiao_df.set_index(keys=[u'代码', u'指标'])
    # zhibiao_df = zhibiao_df.reindex(index=[u'000001', u'000002', u'000003', u'000004'], level=0)

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

    logging.debug("data process.")
    hangye_df = hangye_df.join(hyname_df, on=u'hycode1').loc[:, [u'行业']]
    # zhibiao_df.index.levels[0] may contain 已退市股票
    # ana_df = pd.DataFrame(index=zhibiao_df.index.levels[0])
    all_ana_df = None

    # 处理新季报有的已出来, 有的未出来的情况
    for report in range(2):
        # generate data label
        dongtai_label, jingtai_label, ttm_label1, ttm_label2, ttm_label3,\
            y3_label, y4_label, y5_label, y6_label, y10_label, y11_label, yall_label\
            = generate_date_label(zhibiao_df.columns[report])

        if report == 0:
            temp_df = zhibiao_df.reindex(index=[u'总资产'], level=1).reset_index(level=1, drop=True)
            temp_df = temp_df[pd.notna(temp_df[dongtai_label])]
            report_symbol_list = [[x for x in symbol if x in temp_df.index], [x for x in symbol if x not in temp_df.index]]

        ana_df = pd.DataFrame(index=report_symbol_list[report])
        zhibiao_df_p = zhibiao_df.reindex(index=report_symbol_list[report], level=0)

        ana_df[u'报告日期'] = dongtai_label

        # 计算PE
        # 去年Qx净利润占去年全年的比例, 用来推算今年全年的净利润
        jlr_df = zhibiao_df_p.reindex(index=[u'净利润'], level=1).reset_index(level=1, drop=True)
        jlr_df[u'动态比例'] = jlr_df[ttm_label3] / jlr_df[ttm_label2]
        temp_df = zhibiao_df_p.reindex(index=[u'每股收益'], level=1).reset_index(level=1, drop=True)
        ana_df[u'每股收益'] = temp_df[dongtai_label]
        temp_df[u'价格'] = price_df[u'价格']
        # temp_df = temp_df[(temp_df[dongtai_label] != 0) & (temp_df[jingtai_label] != 0)]
        ana_df[u'PE'] = temp_df[u'价格'] / temp_df[dongtai_label]
        ana_df[u'PE(动)'] = temp_df[u'价格'] / (temp_df[dongtai_label] / jlr_df[u'动态比例'])
        ana_df[u'PE(静)'] = temp_df[u'价格'] / temp_df[jingtai_label]
        ana_df[u'PE(TTM)'] = temp_df[u'价格'] / (temp_df[ttm_label1] + temp_df[ttm_label2] - temp_df[ttm_label3])

        # 扣非收益占比
        temp_df = zhibiao_df_p.reindex(index=[u'每股扣非收益'], level=1).reset_index(level=1, drop=True)
        ana_df[u'扣非收益占比'] = temp_df[dongtai_label] / ana_df[u'每股收益'] * 100

        # 计算PB
        temp_df = zhibiao_df_p.reindex(index=[u'每股净资产'], level=1).reset_index(level=1, drop=True)
        ana_df[u'PB'] = price_df[u'价格'] / temp_df[dongtai_label]

        # 计算ROE
        temp_df = zhibiao_df_p.reindex(index=[u'ROE'], level=1).reset_index(level=1, drop=True)
        ana_df[u'ROE'] = temp_df[dongtai_label]
        ana_df[u'ROE(TTM)'] = temp_df[dongtai_label] + temp_df[ttm_label2] - temp_df[ttm_label3]
        ana_df[u'ROE(静)'] = temp_df[jingtai_label]
        ana_df[u'ROE(三)'] = temp_df[y3_label].sum(axis=1, skipna=False) / 3
        ana_df[u'ROE(五)'] = temp_df[y5_label].sum(axis=1, skipna=False) / 5
        ana_df[u'ROE(十)'] = temp_df[y10_label].sum(axis=1, skipna=False) / 10

        temp_df = zhibiao_df_p.reindex(index=[u'销售毛利率'], level=1).reset_index(level=1, drop=True)
        ana_df[u'销售毛利率'] = temp_df[dongtai_label]
        temp_df = zhibiao_df_p.reindex(index=[u'销售净利率'], level=1).reset_index(level=1, drop=True)
        ana_df[u'销售净利率'] = temp_df[dongtai_label]
        temp_df = zhibiao_df_p.reindex(index=[u'总资产周转率'], level=1).reset_index(level=1, drop=True)
        ana_df[u'总资产周转率'] = temp_df[dongtai_label] * temp_df[ttm_label2] / temp_df[ttm_label3] * 100
        temp_df = zhibiao_df_p.reindex(index=[u'权益乘数'], level=1).reset_index(level=1, drop=True)
        ana_df[u'权益乘数'] = temp_df[dongtai_label]
        temp_df = zhibiao_df_p.reindex(index=[u'每股公积金'], level=1).reset_index(level=1, drop=True)
        ana_df[u'每股公积金'] = temp_df[dongtai_label]
        temp_df = zhibiao_df_p.reindex(index=[u'每股未分配利润'], level=1).reset_index(level=1, drop=True)
        ana_df[u'每股未分配利润'] = temp_df[dongtai_label]
        temp_df = zhibiao_df_p.reindex(index=[u'每股现金流'], level=1).reset_index(level=1, drop=True)
        ana_df[u'每股现金流'] = temp_df[dongtai_label]
        temp_df = zhibiao_df_p.reindex(index=[u'营业收入增长率'], level=1).reset_index(level=1, drop=True)
        ana_df[u'营业收入增长率'] = temp_df[dongtai_label]
        temp_df = zhibiao_df_p.reindex(index=[u'营业利润增长率'], level=1).reset_index(level=1, drop=True)
        ana_df[u'营业利润增长率'] = temp_df[dongtai_label]
        temp_df = zhibiao_df_p.reindex(index=[u'净资产增长率'], level=1).reset_index(level=1, drop=True)
        ana_df[u'净资产增长率'] = temp_df[dongtai_label]
        temp_df = zhibiao_df_p.reindex(index=[u'净利润增长率'], level=1).reset_index(level=1, drop=True)
        ana_df[u'净利润增长率'] = temp_df[dongtai_label]

        # 市销率
        zgb_df = zhibiao_df_p.reindex(index=[u'总股本'], level=1).reset_index(level=1, drop=True)
        temp_df = zhibiao_df_p.reindex(index=[u'营业总收入'], level=1).reset_index(level=1, drop=True)
        ana_df[u'市销率'] = zgb_df[dongtai_label] * price_df[u'价格'] / temp_df[dongtai_label]

        # 5年净利润增长率
        temp_df = zhibiao_df_p.reindex(index=[u'净利润增长率'], level=1).reset_index(level=1, drop=True)
        for i in range(5):
            ana_df[u'净利润增长率{}'.format(i)] = temp_df[y5_label[i]]

        # 净利润复合增长率
        temp_df = zhibiao_df_p.reindex(index=[u'净利润'], level=1).reset_index(level=1, drop=True)
        ana_df = ana_df.join(
            temp_df.reindex(columns=y4_label).apply(
                cal_jlr_zengzhanglv, axis=1, args=(u'净利润复合增长率(三)', u'亏损数(三)')))
        ana_df = ana_df.join(
            temp_df.reindex(columns=y6_label).apply(
                cal_jlr_zengzhanglv, axis=1, args=(u'净利润复合增长率(五)', u'亏损数(五)')))
        ana_df = ana_df.join(
            temp_df.reindex(columns=y11_label).apply(
                cal_jlr_zengzhanglv, axis=1, args=(u'净利润复合增长率(十)', u'亏损数(十)')))

        # 计算PEG
        ## temp_df = zhibiao_df_p.reindex(index=[u'每股收益'], level=1).reset_index(level=1, drop=True)
        ## temp_df[u'PE(动)'] = ana_df[u'PE(动)']
        ## ana_df = ana_df.join(
        ##     temp_df.reindex(columns=[u'PE(动)']+y4_label).apply(
        ##         cal_PEG, axis=1, args=(u'每股收益复合增长率(三)', u'PEG(三)')))
        ## ana_df = ana_df.join(
        ##     temp_df.reindex(columns=[u'PE(动)']+y6_label).apply(
        ##         cal_PEG, axis=1, args=(u'每股收益复合增长率(五)',u'PEG(五)')))
        # 换种算法
        temp_df = zhibiao_df_p.reindex(index=[u'每股收益'], level=1).reset_index(level=1, drop=True)
        temp_df[u'PE(动)'] = ana_df[u'PE(动)']
        ana_df = ana_df.join(
            temp_df.reindex(columns=[u'PE(动)']+y4_label).apply(
                cal_PEG, axis=1, args=(u'每股收益复合增长率(三)', u'PEG(三)'))[u'每股收益复合增长率(三)'])
        ana_df = ana_df.join(
            temp_df.reindex(columns=[u'PE(动)']+y6_label).apply(
                cal_PEG, axis=1, args=(u'每股收益复合增长率(五)',u'PEG(五)'))[u'每股收益复合增长率(五)'])
        ana_df[u'PEG'] = ana_df[u'PE'] / ana_df[u'净利润增长率']
        ana_df[u'PEG(三)'] = ana_df[u'PE(TTM)'] / ana_df[u'净利润复合增长率(三)']
        ana_df[u'PEG(五)'] = ana_df[u'PE(TTM)'] / ana_df[u'净利润复合增长率(五)']


        ana_df = ana_df.join(finance_df.reindex(columns=[u'名称', u'地区', u'上市日期']))
        ana_df[u'行业'] = hangye_df[u'行业']

        if all_ana_df is None:
            all_ana_df = ana_df
        else:
            all_ana_df = pd.concat([all_ana_df, ana_df])

    all_ana_df = all_ana_df.rename_axis(u"代码")
    logging.debug("write file.")
    col_list = [u'名称', u'地区', u'行业', u'上市日期', u'报告日期',
                u'PB', u'PE', u'PE(TTM)', u'PE(动)', u'PE(静)', u'PEG', u'PEG(三)', u'PEG(五)',
                u'ROE', u'ROE(TTM)', u'ROE(静)', u'ROE(三)', u'ROE(五)', u'ROE(十)',
                u'市销率', u'销售净利率', u'销售毛利率',
                u'净利润复合增长率(三)', u'净利润复合增长率(五)', u'净利润复合增长率(十)',
                u'亏损数(三)', u'亏损数(五)', u'亏损数(十)',
                u'净利润增长率0', u'净利润增长率1', u'净利润增长率2', u'净利润增长率3', u'净利润增长率4',
                u'每股收益', u'扣非收益占比', u'每股收益复合增长率(三)', u'每股收益复合增长率(五)',
                u'净利润增长率', u'营业利润增长率', u'营业收入增长率', u'净资产增长率',
                u'每股公积金', u'每股未分配利润', u'每股现金流', u'权益乘数', u'总资产周转率']
    all_ana_df = all_ana_df.reindex(columns=col_list)
    all_ana_df.to_csv(r"..\result\cwfx_tdx.csv", encoding="gbk", quoting=csv.QUOTE_NONE)
    logging.debug("end.")
