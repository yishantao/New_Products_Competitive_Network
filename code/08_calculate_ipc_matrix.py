# -*- coding:utf-8 -*-
"""第七步、第十步：针对汽车面板中的企业，构建IPCMG、IPCSG矩阵"""

import re
import os
import datetime
import pandas as pd

from itertools import combinations


def get_institutions():
    """获取《汽车产业面板数据》中的所有企业名称"""
    panel = pd.read_excel(io='../data/原始数据/02汽车面板数据以及说明/AmIndustryWTOPanel20181128.xlsx',
                          usecols=[2],
                          converters={'OGNM': lambda x: x.strip()})
    institutions = panel.drop_duplicates(subset=['OGNM'], keep='last').reset_index(drop=True)
    return institutions


def extract_patent():
    """提取专利数据
       运行时间：10分钟"""
    patent = pd.read_excel(io='../data/原始数据/03专利数据/汽车产业专利数据(修订)_境内境外完整版.xlsx',
                           converters={'申请（专利权）人': lambda x: x.strip().strip(';')})

    institutions = get_institutions()
    company_mapping = {institutions['OGNM'].iloc[i]: institutions['OGNM'].iloc[i] for i in range(len(institutions))}

    copy = patent['申请（专利权）人'].copy().str.split(';', expand=True)
    for i in copy.columns:
        copy[i].update(copy[i][copy[i].notnull()].apply(lambda s: s.strip()))
        copy[i] = copy[i].map(company_mapping)
        copy[i].update(copy[copy[i].notnull()][i].apply(lambda s: s + ';'))

    copy = copy.fillna('')
    company = copy[0]
    for i in range(1, len(copy.columns)):
        company = company + copy[i]

    patent['申请（专利权）人'] = company.apply(lambda s: s[:-1])
    patent = patent[patent['申请（专利权）人'].str.len() > 0]
    patent.to_excel(excel_writer='../data/生成数据/06专利数据/汽车产业专利数据(仅包含在汽车产业面板中).xlsx',
                    index=False)


def extract_ipcmg_list(x):
    """提取分类号中ipcmg编码"""
    ipc_list = re.compile('[a-zA-Z]\d+[a-zA-Z]\d+[/:：]\d+|\d+[/:：]\d+').findall(str(x))
    if ipc_list:
        for i in range(len(ipc_list)):
            classification_number = re.compile('[a-zA-Z]\d+[a-zA-Z]').search(ipc_list[i])
            if classification_number:
                pass
            else:
                if i > 0:
                    common_string = re.compile('[a-zA-Z]\d+[a-zA-Z]').search(ipc_list[i - 1])
                    if common_string:
                        ipc_list[i] = common_string.group() + ipc_list[i]

        for i in range(len(ipc_list)):
            ipc_list[i] = re.sub('[:：]', '/', ipc_list[i]).split('/')[0]

    ipcmg_list = list(ipc_list)
    return ipcmg_list


def extract_ipcsg_list(x):
    """提取分类号中ipcsg编码"""
    ipcsg_list = re.compile('[a-zA-Z]\d+[a-zA-Z]\d+[/:：]\d+|\d+[/:：]\d+').findall(str(x))
    if ipcsg_list:
        for i in range(len(ipcsg_list)):
            classification_number = re.compile('[a-zA-Z]\d+[a-zA-Z]').search(ipcsg_list[i])
            if classification_number:
                pass
            else:
                if i > 0:
                    common_string = re.compile('[a-zA-Z]\d+[a-zA-Z]').search(ipcsg_list[i - 1])
                    if common_string:
                        ipcsg_list[i] = common_string.group() + ipcsg_list[i]

        for i in range(len(ipcsg_list)):
            ipcsg_list[i] = re.sub('[:：]', '/', ipcsg_list[i])
    ipcsg_list = list(ipcsg_list)
    return ipcsg_list


def calculate_duplicates(x):
    """计算重复项"""
    duplicates_list = list(set([item for item in x if x.count(item) > 1]))
    return duplicates_list


def cooperative_relationship_matrix_ipcmg(df):
    """构建ipcmg合作关系矩阵"""
    df['分类号'] = df['分类号'].apply(lambda x: extract_ipcmg_list(x))
    df['自身共现'] = df['分类号'].copy().apply(lambda x: calculate_duplicates(x))
    df['分类号'] = df['分类号'].apply(lambda x: list(set(x)))
    ipcmg_index = list(set([df['分类号'].iloc[i][j] for i in range(0, len(df)) for j in range(0, len(df['分类号'].iloc[i]))]))
    patent_matrix = pd.DataFrame(columns=ipcmg_index, index=ipcmg_index).fillna(0)

    df_one = df.copy()
    df_one['分类号'] = df_one['分类号'].apply(lambda x: list(combinations(x, 2)))
    df_one = df_one[df_one['分类号'].apply(lambda x: True if len(x) else False)].reset_index(drop=True)
    for j in range(len(df_one)):
        for relationship in df_one['分类号'][j]:
            patent_matrix.loc[relationship[0], relationship[1]] += 1
            patent_matrix.loc[relationship[1], relationship[0]] += 1

    df_two = df.copy()
    df_two = df_two[df_two['自身共现'].apply(lambda x: True if len(x) else False)].reset_index(drop=True)
    for j in range(len(df_two)):
        for element in df_two['自身共现'][j]:
            patent_matrix.loc[element, element] += 1
    return patent_matrix


def cooperative_relationship_matrix_ipcsg(df):
    """构建ipcsg合作关系矩阵"""
    df['分类号'] = df['分类号'].apply(lambda x: extract_ipcsg_list(x))
    df['自身共现'] = df['分类号'].copy().apply(lambda x: calculate_duplicates(x))
    df['分类号'] = df['分类号'].apply(lambda x: list(set(x)))
    ipcsg_index = list(set([df['分类号'].iloc[i][j] for i in range(0, len(df)) for j in range(0, len(df['分类号'].iloc[i]))]))
    patent_matrix = pd.DataFrame(columns=ipcsg_index, index=ipcsg_index).fillna(0)

    df_one = df.copy()
    df_one['分类号'] = df_one['分类号'].apply(lambda x: list(combinations(x, 2)))
    df_one = df_one[df_one['分类号'].apply(lambda x: True if len(x) else False)].reset_index(drop=True)
    for j in range(len(df_one)):
        for relationship in df_one['分类号'][j]:
            patent_matrix.loc[relationship[0], relationship[1]] += 1
            patent_matrix.loc[relationship[1], relationship[0]] += 1

    df_two = df.copy()
    df_two = df_two[df_two['自身共现'].apply(lambda x: True if len(x) else False)].reset_index(drop=True)
    for j in range(len(df_two)):
        for element in df_two['自身共现'][j]:
            patent_matrix.loc[element, element] += 1
    return patent_matrix


def ipcmg_matrix():
    """构建汽车产业面板数据中的企业的IPCMG矩阵；包含三年期/五年期"""
    # 备注：成都汽车配件总厂郫县湓斐?
    patent = pd.read_excel(io='../data/生成数据/06专利数据/汽车产业专利数据(仅包含在汽车产业面板中).xlsx',
                           usecols=[3, 4, 14],
                           converters={'分类号': lambda x: x.strip().strip(';'),
                                       '申请（专利权）人': lambda s: s.strip().strip(';'),
                                       '年份': int})
    company = patent['申请（专利权）人'].str.split(';', expand=True).stack().reset_index(level=1, drop=True)
    company = company.rename('企业名称').str.strip()
    patent = patent.drop(labels=['申请（专利权）人'], axis=1).join(company).reset_index(drop=True)
    institutions = patent[['企业名称']].copy().drop_duplicates(subset=['企业名称'], keep='last').reset_index(drop=True)
    institutions.to_excel(excel_writer='../data/生成数据/06专利数据/汽车产业(企业名称).xlsx',
                          index=False)

    for i in range(len(institutions)):
        each_institutions = institutions['企业名称'].iloc[i]
        each_patent = patent[patent['企业名称'] == each_institutions]

        for year in range(1985, 2013):
            df = each_patent[(year <= each_patent['年份']) & (each_patent['年份'] < year + 3)].reset_index(drop=True)
            if not df.empty:
                patent_matrix = cooperative_relationship_matrix_ipcmg(df)
                if not patent_matrix.empty:
                    file_path = '../data/生成数据/07IPCMGSG网络/三年期IPCMG/' + each_institutions
                    folder = os.path.exists(file_path)
                    if not folder:
                        os.makedirs(file_path)
                    patent_matrix.to_csv(path_or_buf=file_path + '/' + str(year) + '-' + str(year + 2) + '年IPCMG矩阵.csv')

        for year in range(1985, 2011):
            df = each_patent[(year <= each_patent['年份']) & (each_patent['年份'] < year + 5)].reset_index(drop=True)
            if not df.empty:
                patent_matrix = cooperative_relationship_matrix_ipcmg(df)
                if not patent_matrix.empty:
                    file_path = '../data/生成数据/07IPCMGSG网络/五年期IPCMG/' + each_institutions
                    folder = os.path.exists(file_path)
                    if not folder:
                        os.makedirs(file_path)
                    patent_matrix.to_csv(path_or_buf=file_path + '/' + str(year) + '-' + str(year + 4) + '年IPCMG矩阵.csv')


def ipcsg_matrix():
    """构建汽车产业面板数据中的企业的IPCSG矩阵；包含三年期/五年期"""
    patent = pd.read_excel(io='../data/生成数据/06专利数据/汽车产业专利数据(仅包含在汽车产业面板中).xlsx',
                           usecols=[3, 4, 14],
                           converters={'分类号': lambda x: x.strip().strip(';'),
                                       '申请（专利权）人': lambda s: s.strip().strip(';'),
                                       '年份': int})
    company = patent['申请（专利权）人'].str.split(';', expand=True).stack().reset_index(level=1, drop=True)
    company = company.rename('企业名称').str.strip()
    patent = patent.drop(labels=['申请（专利权）人'], axis=1).join(company).reset_index(drop=True)
    institutions = patent[['企业名称']].copy().drop_duplicates(subset=['企业名称'], keep='last').reset_index(drop=True)

    for i in range(len(institutions)):
        each_institutions = institutions['企业名称'].iloc[i]
        each_patent = patent[patent['企业名称'] == each_institutions]

        for year in range(1985, 2013):
            df = each_patent[(year <= each_patent['年份']) & (each_patent['年份'] < year + 3)].reset_index(drop=True)
            if not df.empty:
                patent_matrix = cooperative_relationship_matrix_ipcsg(df)
                if not patent_matrix.empty:
                    file_path = '../data/生成数据/07IPCMGSG网络/三年期IPCSG/' + each_institutions
                    folder = os.path.exists(file_path)
                    if not folder:
                        os.makedirs(file_path)
                    patent_matrix.to_csv(path_or_buf=file_path + '/' + str(year) + '-' + str(year + 2) + '年IPCSG矩阵.csv')

        for year in range(1985, 2011):
            df = each_patent[(year <= each_patent['年份']) & (each_patent['年份'] < year + 5)].reset_index(drop=True)
            if not df.empty:
                patent_matrix = cooperative_relationship_matrix_ipcsg(df)
                if not patent_matrix.empty:
                    file_path = '../data/生成数据/07IPCMGSG网络/五年期IPCSG/' + each_institutions
                    folder = os.path.exists(file_path)
                    if not folder:
                        os.makedirs(file_path)
                    patent_matrix.to_csv(path_or_buf=file_path + '/' + str(year) + '-' + str(year + 4) + '年IPCSG矩阵.csv')


if __name__ == "__main__":
    # 运行时间25分钟
    start_time = datetime.datetime.now()
    extract_patent()
    ipcmg_matrix()
    ipcsg_matrix()
    end_time = datetime.datetime.now()
    run_time = end_time - start_time
    print('程序开始时间：', start_time, '\n程序结束时间：', end_time, '\n程序运行时间：', run_time)
