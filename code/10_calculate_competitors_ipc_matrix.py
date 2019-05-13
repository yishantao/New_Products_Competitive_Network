# -*- coding:utf-8 -*-
"""为第十一步之后做准备：竞争对手IPCSG/MG矩阵构建"""

import re
import os
import datetime
import pandas as pd

from itertools import combinations
from multiprocessing import Pool


def three_competitors(begin_year, end_year, excel_number):
    """三年期竞争对手企业名称(在面板数据中出现并且在整车数据中出现的企业和其竞争对手(不一定在面板中但一定在整车数据中))"""
    competitors_institutions = pd.DataFrame(columns=['ZZCMC编号'])
    index = 0

    vehicle_data = pd.read_excel(io='../data/生成数据/02整车数据/04整车数据(企业名称修改为ZZCMC编号).xlsx',
                                 converters={'ZZCMC': str,
                                             'CLLX': str},
                                 parse_dates=['CXSXRQ', 'GGSXRQ'])
    vehicle_data['CXSXRQ'] = vehicle_data['CXSXRQ'].fillna(pd.to_datetime('2017-12-31'))

    panel = pd.read_excel(io='../data/原始数据/02汽车面板数据以及说明/AmIndustryWTOPanel20181128.xlsx',
                          usecols=[2],
                          converters={'OGNM': lambda x: x.strip()})
    institutions = panel.drop_duplicates(subset=['OGNM'], keep='last').reset_index(drop=True)
    institutions = institutions.rename(columns={'OGNM': '企业名称'})
    address = pd.read_excel(io='../data/生成数据/01企业名称修改表/13整车数据ZZCMCZ编码表.xlsx',
                            usecols=[0, 1],
                            converters={'ZZCMC编号': str,
                                        'ZZCMC企业名称': lambda x: x.strip()})
    address = address.rename(columns={'ZZCMC企业名称': '企业名称'})
    institutions = pd.merge(left=institutions, right=address, how='left', on=['企业名称'])
    institutions = institutions[institutions['ZZCMC编号'].notnull()].reset_index(drop=True)

    for year in range(begin_year, end_year):
        filter_data = vehicle_data[((pd.to_datetime(str(year)) <= vehicle_data['GGSXRQ']) & (
                vehicle_data['GGSXRQ'] <= pd.to_datetime(str(year + 2) + '-12-31'))) | (
                                           (pd.to_datetime(str(year)) <= vehicle_data['CXSXRQ']) & (
                                           vehicle_data['CXSXRQ'] <= pd.to_datetime(str(year + 2) + '-12-31')))]

        if not filter_data.empty:
            filter_data = filter_data.reset_index(drop=True)
            company_index = filter_data['ZZCMC'].drop_duplicates(keep='last').astype(int).sort_values(
                ascending=True).astype(
                str).reset_index(drop=True)
            company_index = list(company_index)
            company_car_type_counts = {
                company: filter_data[filter_data['ZZCMC'] == company]['CLLX'].value_counts().to_dict()
                for company in company_index}

            for institution_index in range(len(institutions)):
                institution = institutions['ZZCMC编号'].iloc[institution_index]
                if institution in company_car_type_counts:
                    competitors_institutions.loc[index] = [institution]
                    index += 1
                    original_car_type = company_car_type_counts[institution]
                    for i in range(0, len(company_index)):
                        if company_index[i] != institution:
                            compare_car_type = company_car_type_counts[company_index[i]]
                            company_key = original_car_type.keys() & compare_car_type.keys()
                            if company_key:
                                competitors_institutions.loc[index] = [company_index[i]]
                                index += 1

    competitors_institutions = pd.merge(left=competitors_institutions, right=address, on=['ZZCMC编号'], how='left')
    competitors_institutions = competitors_institutions[['企业名称']].drop_duplicates(subset=['企业名称'],
                                                                                  keep='last').reset_index(drop=True)
    return (competitors_institutions, excel_number, '三年期')


def write_file(parameter):
    """将数据写入文件"""
    competitors_institutions, excel_number, tag = parameter[0], parameter[1], parameter[2]
    competitors_institutions.to_excel(
        excel_writer='../data/生成数据/08竞争对手/竞争对手企业名称/' + excel_number + tag + '竞争对手企业名称.xlsx',
        index=False)


def calculate_three_competitors():
    """使用多进程计算三年期竞争对手企业名称"""
    p = Pool(4)
    p.apply_async(func=three_competitors, args=(1985, 2004, '01'), callback=write_file)
    p.apply_async(func=three_competitors, args=(2004, 2008, '02'), callback=write_file)
    p.apply_async(func=three_competitors, args=(2008, 2011, '03'), callback=write_file)
    p.apply_async(func=three_competitors, args=(2011, 2013, '04'), callback=write_file)
    p.close()
    p.join()


def five_competitors(begin_year, end_year, excel_number):
    """五年期竞争对手企业名称(在面板数据中出现并且在整车数据中出现的企业和其竞争对手(不一定在面板中但一定在整车数据中))"""
    competitors_institutions = pd.DataFrame(columns=['ZZCMC编号'])
    index = 0

    vehicle_data = pd.read_excel(io='../data/生成数据/02整车数据/04整车数据(企业名称修改为ZZCMC编号).xlsx',
                                 converters={'ZZCMC': str,
                                             'CLLX': str},
                                 parse_dates=['CXSXRQ', 'GGSXRQ'])
    vehicle_data['CXSXRQ'] = vehicle_data['CXSXRQ'].fillna(pd.to_datetime('2017-12-31'))

    panel = pd.read_excel(io='../data/原始数据/02汽车面板数据以及说明/AmIndustryWTOPanel20181128.xlsx',
                          usecols=[2],
                          converters={'OGNM': lambda x: x.strip()})
    institutions = panel.drop_duplicates(subset=['OGNM'], keep='last').reset_index(drop=True)
    institutions = institutions.rename(columns={'OGNM': '企业名称'})
    address = pd.read_excel(io='../data/生成数据/01企业名称修改表/13整车数据ZZCMCZ编码表.xlsx',
                            usecols=[0, 1],
                            converters={'ZZCMC编号': str,
                                        'ZZCMC企业名称': lambda x: x.strip()})
    address = address.rename(columns={'ZZCMC企业名称': '企业名称'})
    institutions = pd.merge(left=institutions, right=address, how='left', on=['企业名称'])
    institutions = institutions[institutions['ZZCMC编号'].notnull()].reset_index(drop=True)

    for year in range(begin_year, end_year):
        filter_data = vehicle_data[((pd.to_datetime(str(year)) <= vehicle_data['GGSXRQ']) & (
                vehicle_data['GGSXRQ'] <= pd.to_datetime(str(year + 4) + '-12-31'))) | (
                                           (pd.to_datetime(str(year)) <= vehicle_data['CXSXRQ']) & (
                                           vehicle_data['CXSXRQ'] <= pd.to_datetime(str(year + 4) + '-12-31')))]

        if not filter_data.empty:
            filter_data = filter_data.reset_index(drop=True)
            company_index = filter_data['ZZCMC'].drop_duplicates(keep='last').astype(int).sort_values(
                ascending=True).astype(
                str).reset_index(drop=True)
            company_index = list(company_index)
            company_car_type_counts = {
                company: filter_data[filter_data['ZZCMC'] == company]['CLLX'].value_counts().to_dict()
                for company in company_index}

            for institution_index in range(len(institutions)):
                institution = institutions['ZZCMC编号'].iloc[institution_index]
                if institution in company_car_type_counts:
                    competitors_institutions.loc[index] = [institution]
                    index += 1
                    original_car_type = company_car_type_counts[institution]
                    for i in range(0, len(company_index)):
                        if company_index[i] != institution:
                            compare_car_type = company_car_type_counts[company_index[i]]
                            company_key = original_car_type.keys() & compare_car_type.keys()
                            if company_key:
                                competitors_institutions.loc[index] = [company_index[i]]
                                index += 1

    competitors_institutions = pd.merge(left=competitors_institutions, right=address, on=['ZZCMC编号'], how='left')
    competitors_institutions = competitors_institutions[['企业名称']].drop_duplicates(subset=['企业名称'],
                                                                                  keep='last').reset_index(drop=True)
    return (competitors_institutions, excel_number, '五年期')


def calculate_five_competitors():
    """使用多进程计算五年期竞争对手企业名称"""
    p = Pool()
    p.apply_async(func=five_competitors, args=(1985, 2004, '05'), callback=write_file)
    p.apply_async(func=five_competitors, args=(2004, 2007, '06'), callback=write_file)
    p.apply_async(func=five_competitors, args=(2007, 2008, '07',), callback=write_file)
    p.apply_async(func=five_competitors, args=(2008, 2009, '08',), callback=write_file)
    p.apply_async(func=five_competitors, args=(2009, 2010, '09',), callback=write_file)
    p.apply_async(func=five_competitors, args=(2010, 2011, '10',), callback=write_file)
    p.close()
    p.join()


def total_competitors():
    """企业自身以及竞争对手企业名称"""
    three_competitors = [pd.read_excel(io='../data/生成数据/08竞争对手/竞争对手企业名称/0' + str(i) + '三年期竞争对手企业名称.xlsx')
                         for i in range(1, 5)]

    five_competitors = [pd.read_excel(io='../data/生成数据/08竞争对手/竞争对手企业名称/0' + str(i) + '五年期竞争对手企业名称.xlsx')
                        for i in range(5, 10)]
    five_competitors.append(pd.read_excel(io='../data/生成数据/08竞争对手/竞争对手企业名称/10五年期竞争对手企业名称.xlsx'))

    three_competitors.extend(five_competitors)
    competitors = pd.concat(objs=three_competitors, ignore_index=True)
    competitors = competitors.drop_duplicates(subset=['企业名称'], keep='last').reset_index(drop=True)
    competitors.to_excel(excel_writer='../data/生成数据/08竞争对手/竞争对手企业名称/11企业自身及其竞争对手企业名称.xlsx',
                         index=False)


def extract_patent():
    """提取专利数据
       运行时间：10分钟"""
    patent = pd.read_excel(io='../data/原始数据/03专利数据/汽车产业专利数据(修订)_境内境外完整版.xlsx',
                           converters={'申请（专利权）人': lambda x: x.strip().strip(';')})

    institutions = pd.read_excel(io='../data/生成数据/08竞争对手/竞争对手企业名称/11企业自身及其竞争对手企业名称.xlsx')
    company_mapping = {institutions['企业名称'].iloc[i]: institutions['企业名称'].iloc[i] for i in range(len(institutions))}

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
    patent.to_excel(excel_writer='../data/生成数据/08竞争对手/专利数据/汽车产业专利数据(包含企业自身及竞争对手).xlsx',
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
    """构建企业的IPCMG矩阵；包含三年期/五年期"""
    patent = pd.read_excel(io='../data/生成数据/08竞争对手/专利数据/汽车产业专利数据(包含企业自身及竞争对手).xlsx',
                           usecols=[3, 4, 14],
                           converters={'分类号': lambda x: x.strip().strip(';'),
                                       '申请（专利权）人': lambda s: s.strip().strip(';'),
                                       '年份': int})
    company = patent['申请（专利权）人'].str.split(';', expand=True).stack().reset_index(level=1, drop=True)
    company = company.rename('企业名称').str.strip()
    patent = patent.drop(labels=['申请（专利权）人'], axis=1).join(company).reset_index(drop=True)
    institutions = patent[['企业名称']].copy().drop_duplicates(subset=['企业名称'], keep='last').reset_index(drop=True)
    institutions.to_excel(excel_writer='../data/生成数据/08竞争对手/专利数据/汽车产业(企业名称).xlsx',
                          index=False)

    for i in range(len(institutions)):
        each_institutions = institutions['企业名称'].iloc[i]
        each_patent = patent[patent['企业名称'] == each_institutions]

        for year in range(1985, 2013):
            df = each_patent[(year <= each_patent['年份']) & (each_patent['年份'] < year + 3)].reset_index(drop=True)
            if not df.empty:
                patent_matrix = cooperative_relationship_matrix_ipcmg(df)
                if not patent_matrix.empty:
                    file_path = '../data/生成数据/08竞争对手/IPCMGSG/三年期IPCMG/' + each_institutions
                    folder = os.path.exists(file_path)
                    if not folder:
                        os.makedirs(file_path)
                    patent_matrix.to_csv(path_or_buf=file_path + '/' + str(year) + '-' + str(year + 2) + '年IPCMG矩阵.csv')

        for year in range(1985, 2011):
            df = each_patent[(year <= each_patent['年份']) & (each_patent['年份'] < year + 5)].reset_index(drop=True)
            if not df.empty:
                patent_matrix = cooperative_relationship_matrix_ipcmg(df)
                if not patent_matrix.empty:
                    file_path = '../data/生成数据/08竞争对手/IPCMGSG/五年期IPCMG/' + each_institutions
                    folder = os.path.exists(file_path)
                    if not folder:
                        os.makedirs(file_path)
                    patent_matrix.to_csv(path_or_buf=file_path + '/' + str(year) + '-' + str(year + 4) + '年IPCMG矩阵.csv')


def ipcsg_matrix():
    """构建汽车产业面板数据中的企业的IPCSG矩阵；包含三年期/五年期"""
    patent = pd.read_excel(io='../data/生成数据/08竞争对手/专利数据/汽车产业专利数据(包含企业自身及竞争对手).xlsx',
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
                    file_path = '../data/生成数据/08竞争对手/IPCMGSG/三年期IPCSG/' + each_institutions
                    folder = os.path.exists(file_path)
                    if not folder:
                        os.makedirs(file_path)
                    patent_matrix.to_csv(path_or_buf=file_path + '/' + str(year) + '-' + str(year + 2) + '年IPCSG矩阵.csv')

        for year in range(1985, 2011):
            df = each_patent[(year <= each_patent['年份']) & (each_patent['年份'] < year + 5)].reset_index(drop=True)
            if not df.empty:
                patent_matrix = cooperative_relationship_matrix_ipcsg(df)
                if not patent_matrix.empty:
                    file_path = '../data/生成数据/08竞争对手/IPCMGSG/五年期IPCSG/' + each_institutions
                    folder = os.path.exists(file_path)
                    if not folder:
                        os.makedirs(file_path)
                    patent_matrix.to_csv(path_or_buf=file_path + '/' + str(year) + '-' + str(year + 4) + '年IPCSG矩阵.csv')


if __name__ == "__main__":
    start_time = datetime.datetime.now()
    # calculate_three_competitors() # 运行时间大约12小时
    # calculate_five_competitors()  # 运行时间大约12小时
    # total_competitors()
    # extract_patent()
    # 下一步之前改写不标准的企业名称
    ipcmg_matrix()
    ipcsg_matrix()
    end_time = datetime.datetime.now()
    run_time = end_time - start_time
    print('程序开始时间：', start_time, '\n程序结束时间：', end_time, '\n程序运行时间：', run_time)
