# coding: utf-8
"""
生成ROE有关数据
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

    all_lrb_df = None # type: pd.DataFrame
    all_fzb_df = None # type: pd.DataFrame
    all_zhibiao_df = None # type: pd.DataFrame
    #symbol = symbol[0:3]
    first_lrb_df = True
    #date_list = ["20170930"]
    #date_list = pd.date_range("20170301", "20170930", freq="Q").strftime("%Y%m%d")[::-1]
    date_list = pd.date_range("19700301", "20201231", freq="Q").strftime("%Y%m%d")[::-1]

    for code in symbol:
        try:
            lrb = pd.read_csv(r"..\all_cwsj\{}_lrb.xls".format(code), encoding='gbk', delim_whitespace=True, index_col=0)  # type: pd.DataFrame
        except Exception as e:
            print "read csv error for {}, skipped".format(code)
            traceback.print_exc()
            continue

        try:
            row_jlr = None
            for key in [u"归属于母公司所有者的净利润", u"归属于母公司的净利润", u"归属于母公司股东的净利润"]:
                if lrb.index.contains(key):
                    row_jlr = key
                    break
            if row_jlr is None:
                raise Exception("Can't find column 净利润 for {}".format(code))

            row_yyzsl = None
            for key in [u"一、营业总收入", u"一、营业收入"]:
                if lrb.index.contains(key):
                    row_yyzsl = key
                    break
            if row_yyzsl is None:
                raise Exception("Can't find column 营业总收入 for {}".format(code))

            row_mgsy = None
            for key in [u"基本每股收益(元/股)"]:
                if lrb.index.contains(key):
                    row_mgsy = key
                    break
            if row_mgsy is None:
                raise Exception("Can't find column 每股收益 for {}".format(code))

            lrb = lrb.rename_axis(u"指标")
            lrb = lrb.reindex([row_jlr, row_yyzsl, row_mgsy], columns=date_list).reset_index(drop=False)
            lrb[u"指标"] = [u'净利润', u'营业总收入', u'每股收益']
            #lrb[u'code'] = code
            #lrb = lrb.set_index([u'code', u"指标"])
            #lrb = lrb.transpose()
            #lrb.index = [code]
            #if all_lrb_df is None:
            #    all_lrb_df = lrb
            #else:
            #    all_lrb_df = all_lrb_df.append(lrb)
        except Exception as e:
            print "process data error for {}, skipped".format(code)
            traceback.print_exc()
            continue


        try:
            fzb = pd.read_csv(r"..\all_cwsj\{}_fzb.xls".format(code), encoding='gbk', delim_whitespace=True, index_col=0)  # type: pd.DataFrame
        except Exception as e:
            print "read csv error for {}, skipped".format(code)
            traceback.print_exc()
            continue

        try:
            row_zzc = None
            for key in [u"资产总计"]:
                if fzb.index.contains(key):
                    row_zzc = key
                    break
            if row_zzc is None:
                raise Exception("Can't find column 总资产 for {}".format(code))

            row_jzc = None
            for key in [u"归属于母公司股东权益合计", u"归属于母公司股东的权益", u"归属于母公司所有者权益合计", u"归属于母公司的股东权益合计"]:
                if fzb.index.contains(key):
                    row_jzc = key
                    break
            if row_jzc is None:
                raise Exception("Can't find column 净资产 for {}".format(code))

            row_zgb = None
            for key in [u"实收资本(或股本)", u"股本"]:
                if fzb.index.contains(key):
                    row_zgb = key
                    break
            if row_zgb is None:
                raise Exception("Can't find column 总股本 for {}".format(code))

            row_zbgjj = None
            for key in [u"资本公积", u"资本公积金"]:
                if fzb.index.contains(key):
                    row_zbgjj = key
                    break
            if row_zbgjj is None:
                raise Exception("Can't find column 资本公积金 for {}".format(code))

            row_wfplr = None
            for key in [u"未分配利润"]:
                if fzb.index.contains(key):
                    row_wfplr = key
                    break
            if row_wfplr is None:
                raise Exception("Can't find column 未分配利润 for {}".format(code))

            fzb = fzb.rename_axis(u"指标")
            fzb = fzb.reindex([row_zzc, row_jzc, row_zgb, row_zbgjj, row_wfplr], columns=date_list).reset_index(drop=False)
            fzb[u"指标"] = [u'总资产', u'净资产', u'总股本', u'资本公积金', u'未分配利润']
            #fzb[u'code'] = code
            #fzb = fzb.set_index([u'code', u"指标"])
            #fzb = fzb.transpose()
            #fzb.index = [code]
            #if all_fzb_df is None:
            #    all_fzb_df = fzb
            #else:
            #    all_fzb_df = all_fzb_df.append(fzb)
        except Exception as e:
            print "process data error for {}, skipped".format(code)
            traceback.print_exc()
            continue


        temp_df = lrb.append(fzb).set_index(u"指标").astype(np.float64) # type: pd.DataFrame
        # the 总股本 from fzb is not as expected
        temp_df.loc[u"总股本"] = temp_df.loc[u"净利润"] / temp_df.loc[u"每股收益"]
        temp_df.loc[u"每股公积金"] = temp_df.loc[u"资本公积金"] / temp_df.loc[u"总股本"]
        temp_df.loc[u"每股未分配利润"] = temp_df.loc[u"未分配利润"] / temp_df.loc[u"总股本"]
        temp_df.loc[u"每股净利润"] = temp_df.loc[u"净利润"] / temp_df.loc[u"总股本"]
        temp_df.loc[u"每股营业收入"] = temp_df.loc[u"营业总收入"] / temp_df.loc[u"总股本"]
        temp_df.loc[u"每股总资产"] = temp_df.loc[u"总资产"] / temp_df.loc[u"总股本"]
        temp_df.loc[u"每股净资产"] = temp_df.loc[u"净资产"] / temp_df.loc[u"总股本"]
        temp_df.loc[u"销售净利率"] = temp_df.loc[u"每股净利润"] / temp_df.loc[u"每股营业收入"]
        temp_df.loc[u"总资产周转率"] = temp_df.loc[u"每股营业收入"] / temp_df.loc[u"每股总资产"]
        temp_df.loc[u"权益乘数"] = temp_df.loc[u"每股总资产"] / temp_df.loc[u"每股净资产"]
        temp_df.loc[u"ROE"] = temp_df.loc[u"净利润"] / temp_df.loc[u"净资产"]
        #temp_df = temp_df.reindex(index=[u"每股公积金", u"每股未分配利润", u"每股净利润", u"每股营业收入", u"每股总资产", u"每股净资产", u"销售净利率", u"总资产周转率", u"权益乘数", u"ROE"])
        temp_df = temp_df.replace([np.inf, -np.inf], np.nan)
        temp_df[u'代码'] = code

        if all_zhibiao_df is None:
           all_zhibiao_df = temp_df
        else:
           all_zhibiao_df = all_zhibiao_df.append(temp_df)

all_zhibiao_df = all_zhibiao_df.dropna(axis='columns', how='all')
all_zhibiao_df = all_zhibiao_df.reset_index().set_index([u'代码', u'指标'])
all_zhibiao_df.to_csv(r"..\all_other_data\all_zhibiao_for_roe_peg.csv", encoding="gbk", quoting=csv.QUOTE_NONE)
