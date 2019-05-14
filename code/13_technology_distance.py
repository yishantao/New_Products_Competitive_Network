# -*- coding:utf-8 -*-
"""第十五——十八步：计算技术距离"""

import re
import datetime
import pandas as pd

from multiprocessing import Pool


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


def ipcsg_three_distance(mark):
    """在三年窗口期中某企业的三年期IPCSG矩阵与它的竞争对手的三年期IPCSG矩阵之间cosine技术距离的均值"""
    statistical_information = pd.DataFrame(columns=['企业名称', '年份', 'cptor3ywipcsgcsd'])
    index = 0

    technology_distance = pd.DataFrame(columns=['企业名称', '年份', '三年期IPCSG技术向量'])
    technology_index = 0

    all_patent = pd.read_excel(io='../data/生成数据/08竞争对手/专利数据/汽车产业专利数据(包含企业自身及竞争对手).xlsx',
                               usecols=[3, 4, 14],
                               converters={'分类号': lambda x: x.strip().strip(';'),
                                           '申请（专利权）人': lambda x: x.strip().strip(';'),
                                           '年份': int})
    company = all_patent['申请（专利权）人'].str.split(';', expand=True).stack().reset_index(level=1, drop=True)
    company = company.rename('企业名称').str.strip()
    all_patent = all_patent.drop(labels=['申请（专利权）人'], axis=1).join(company).reset_index(drop=True)

    institutions = pd.read_excel(io='../data/生成数据/06专利数据/汽车产业(企业名称).xlsx')
    address = pd.read_excel(io='../data/生成数据/01企业名称修改表/13整车数据ZZCMCZ编码表.xlsx',
                            usecols=[0, 1],
                            converters={'ZZCMC编号': str,
                                        'ZZCMC企业名称': lambda x: x.strip()})
    address = address.rename(columns={'ZZCMC企业名称': '企业名称'})
    institutions = pd.merge(left=institutions, right=address, how='left', on=['企业名称'])
    institutions = institutions[institutions['ZZCMC编号'].notnull()].reset_index(drop=True)

    vehicle_data = pd.read_excel(io='../data/生成数据/02整车数据/03整车数据(第三步修改).xlsx',
                                 usecols=[8, 40, 43, 49],
                                 converters={'ZZCMC': lambda x: x.strip(),
                                             'CLLX': lambda x: x.strip()},
                                 parse_dates=['CXSXRQ', 'GGSXRQ'])
    vehicle_data['CXSXRQ'] = vehicle_data['CXSXRQ'].fillna(pd.to_datetime('2017-12-31'))

    for year in range(1985, 2013):
        filter_data = vehicle_data[((pd.to_datetime(str(year)) <= vehicle_data['GGSXRQ']) & (
                vehicle_data['GGSXRQ'] <= pd.to_datetime(str(year + 2) + '-12-31'))) | (
                                           (pd.to_datetime(str(year)) <= vehicle_data['CXSXRQ']) & (
                                           vehicle_data['CXSXRQ'] <= pd.to_datetime(str(year + 2) + '-12-31')))]

        if not filter_data.empty:
            filter_data = filter_data.reset_index(drop=True)
            company_index = filter_data['ZZCMC'].drop_duplicates(keep='last').sort_values(ascending=True).reset_index(
                drop=True)
            company_index = list(company_index)
            company_car_type_counts = {
                company: filter_data[filter_data['ZZCMC'] == company]['CLLX'].value_counts().to_dict()
                for company in company_index}

            for institution_index in range(len(institutions)):
                institution = institutions['企业名称'].iloc[institution_index]
                if institution in company_car_type_counts:
                    original_car_type = company_car_type_counts[institution]
                    competitors = []
                    for i in range(0, len(company_index)):
                        if company_index[i] != institution:
                            compare_car_type = company_car_type_counts[company_index[i]]
                            company_key = original_car_type.keys() & compare_car_type.keys()
                            if company_key:
                                competitors.append(company_index[i])
                    if len(competitors) > 0:
                        total_distance = 0
                        original_patent = all_patent[
                            (all_patent['企业名称'] == institution) & (year <= all_patent['年份']) & (
                                    all_patent['年份'] <= year + 2)]
                        if not original_patent.empty:
                            original_patent_length = len(original_patent)
                            original_patent['分类号'] = original_patent['分类号'].apply(lambda x: list(extract_ipcsg_list(x)))
                            ipcsg_index = list(set(
                                [original_patent['分类号'].iloc[i][j] for i in range(0, len(original_patent)) for j in
                                 range(0, len(original_patent['分类号'].iloc[i]))]))
                            original_ipcsg_vector = {each_ipcsg: 0 for each_ipcsg in ipcsg_index}
                            for each_ipcsg in ipcsg_index:
                                for i in range(original_patent_length):
                                    if each_ipcsg in original_patent['分类号'].iloc[i]:
                                        original_ipcsg_vector[each_ipcsg] += 1
                            original_ipcsg_vector = {
                                each_ipcsg: original_ipcsg_vector[each_ipcsg] / original_patent_length
                                for each_ipcsg
                                in original_ipcsg_vector.keys()}

                            technology_distance.loc[technology_index] = [institution, year + 2,
                                                                         original_ipcsg_vector]
                            technology_index += 1

                            ipcsg_sum = sum([ipcsg_value ** 2 for ipcsg_value in original_ipcsg_vector.values()]) ** 0.5
                            original_ipcsg_vector = {each_ipcsg: original_ipcsg_vector[each_ipcsg] / ipcsg_sum for
                                                     each_ipcsg in original_ipcsg_vector.keys()}

                            for industry in competitors:
                                industry_patent = all_patent[
                                    (all_patent['企业名称'] == industry) & (year <= all_patent['年份']) & (
                                            all_patent['年份'] <= year + 2)]
                                if not industry_patent.empty:
                                    industry_patent_length = len(industry_patent)
                                    industry_patent['分类号'] = industry_patent['分类号'].apply(
                                        lambda x: list(extract_ipcsg_list(x)))
                                    ipcsg_index = list(set(
                                        [industry_patent['分类号'].iloc[i][j] for i in range(0, len(industry_patent)) for j
                                         in
                                         range(0, len(industry_patent['分类号'].iloc[i]))]))
                                    compare_ipcsg_vector = {each_ipcsg: 0 for each_ipcsg in ipcsg_index}
                                    for each_ipcsg in ipcsg_index:
                                        for i in range(industry_patent_length):
                                            if each_ipcsg in industry_patent['分类号'].iloc[i]:
                                                compare_ipcsg_vector[each_ipcsg] += 1
                                    compare_ipcsg_vector = {
                                        each_ipcsg: compare_ipcsg_vector[each_ipcsg] / industry_patent_length for
                                        each_ipcsg
                                        in compare_ipcsg_vector.keys()}

                                    technology_distance.loc[technology_index] = [industry, year + 2,
                                                                                 compare_ipcsg_vector]
                                    technology_index += 1

                                    ipcsg_sum = sum(
                                        [ipcsg_value ** 2 for ipcsg_value in compare_ipcsg_vector.values()]) ** 0.5
                                    compare_ipcsg_vector = {each_ipcsg: compare_ipcsg_vector[each_ipcsg] / ipcsg_sum
                                                            for each_ipcsg in compare_ipcsg_vector.keys()}

                                    common_key = original_ipcsg_vector.keys() & compare_ipcsg_vector.keys()
                                    distance = 1 - sum(
                                        [original_ipcsg_vector[i] * compare_ipcsg_vector[i] for i in common_key])
                                    total_distance += distance
                        average_distance = total_distance / len(competitors)
                        statistical_information.loc[index] = [institution, year + 2, average_distance]
                        index += 1

    statistical_information.to_excel(excel_writer='../data/生成数据/09技术距离/01三年期IPCSG技术距离.xlsx',
                                     index=False)
    technology_distance.to_excel(excel_writer='../data/生成数据/09技术距离/01三年期IPCSG技术向量.xlsx',
                                 index=False)
    print(mark + '执行完毕！')


def ipcsg_five_distance(mark):
    """在五年窗口期中某企业的五年期IPCSG矩阵与它的竞争对手的五年期IPCSG矩阵之间cosine技术距离的均值"""
    statistical_information = pd.DataFrame(columns=['企业名称', '年份', 'cptor5ywipcsgcsd'])
    index = 0

    technology_distance = pd.DataFrame(columns=['企业名称', '年份', '五年期IPCSG技术向量'])
    technology_index = 0

    all_patent = pd.read_excel(io='../data/生成数据/08竞争对手/专利数据/汽车产业专利数据(包含企业自身及竞争对手).xlsx',
                               usecols=[3, 4, 14],
                               converters={'分类号': lambda x: x.strip().strip(';'),
                                           '申请（专利权）人': lambda x: x.strip().strip(';'),
                                           '年份': int})
    company = all_patent['申请（专利权）人'].str.split(';', expand=True).stack().reset_index(level=1, drop=True)
    company = company.rename('企业名称').str.strip()
    all_patent = all_patent.drop(labels=['申请（专利权）人'], axis=1).join(company).reset_index(drop=True)

    institutions = pd.read_excel(io='../data/生成数据/06专利数据/汽车产业(企业名称).xlsx')
    address = pd.read_excel(io='../data/生成数据/01企业名称修改表/13整车数据ZZCMCZ编码表.xlsx',
                            usecols=[0, 1],
                            converters={'ZZCMC编号': str,
                                        'ZZCMC企业名称': lambda x: x.strip()})
    address = address.rename(columns={'ZZCMC企业名称': '企业名称'})
    institutions = pd.merge(left=institutions, right=address, how='left', on=['企业名称'])
    institutions = institutions[institutions['ZZCMC编号'].notnull()].reset_index(drop=True)

    vehicle_data = pd.read_excel(io='../data/生成数据/02整车数据/03整车数据(第三步修改).xlsx',
                                 usecols=[8, 40, 43, 49],
                                 converters={'ZZCMC': lambda x: x.strip(),
                                             'CLLX': lambda x: x.strip()},
                                 parse_dates=['CXSXRQ', 'GGSXRQ'])
    vehicle_data['CXSXRQ'] = vehicle_data['CXSXRQ'].fillna(pd.to_datetime('2017-12-31'))

    for year in range(1985, 2011):
        filter_data = vehicle_data[((pd.to_datetime(str(year)) <= vehicle_data['GGSXRQ']) & (
                vehicle_data['GGSXRQ'] <= pd.to_datetime(str(year + 4) + '-12-31'))) | (
                                           (pd.to_datetime(str(year)) <= vehicle_data['CXSXRQ']) & (
                                           vehicle_data['CXSXRQ'] <= pd.to_datetime(str(year + 4) + '-12-31')))]

        if not filter_data.empty:
            filter_data = filter_data.reset_index(drop=True)
            company_index = filter_data['ZZCMC'].drop_duplicates(keep='last').sort_values(ascending=True).reset_index(
                drop=True)
            company_index = list(company_index)
            company_car_type_counts = {
                company: filter_data[filter_data['ZZCMC'] == company]['CLLX'].value_counts().to_dict()
                for company in company_index}

            for institution_index in range(len(institutions)):
                institution = institutions['企业名称'].iloc[institution_index]
                if institution in company_car_type_counts:
                    original_car_type = company_car_type_counts[institution]
                    competitors = []
                    for i in range(0, len(company_index)):
                        if company_index[i] != institution:
                            compare_car_type = company_car_type_counts[company_index[i]]
                            company_key = original_car_type.keys() & compare_car_type.keys()
                            if company_key:
                                competitors.append(company_index[i])
                    if len(competitors) > 0:
                        total_distance = 0
                        original_patent = all_patent[
                            (all_patent['企业名称'] == institution) & (year <= all_patent['年份']) & (
                                    all_patent['年份'] <= year + 4)]
                        if not original_patent.empty:
                            original_patent_length = len(original_patent)
                            original_patent['分类号'] = original_patent['分类号'].apply(lambda x: list(extract_ipcsg_list(x)))
                            ipcsg_index = list(set(
                                [original_patent['分类号'].iloc[i][j] for i in range(0, len(original_patent)) for j in
                                 range(0, len(original_patent['分类号'].iloc[i]))]))
                            original_ipcsg_vector = {each_ipcsg: 0 for each_ipcsg in ipcsg_index}
                            for each_ipcsg in ipcsg_index:
                                for i in range(original_patent_length):
                                    if each_ipcsg in original_patent['分类号'].iloc[i]:
                                        original_ipcsg_vector[each_ipcsg] += 1
                            original_ipcsg_vector = {
                                each_ipcsg: original_ipcsg_vector[each_ipcsg] / original_patent_length
                                for each_ipcsg
                                in original_ipcsg_vector.keys()}

                            technology_distance.loc[technology_index] = [institution, year + 4,
                                                                         original_ipcsg_vector]
                            technology_index += 1

                            ipcsg_sum = sum([ipcsg_value ** 2 for ipcsg_value in original_ipcsg_vector.values()]) ** 0.5
                            original_ipcsg_vector = {each_ipcsg: original_ipcsg_vector[each_ipcsg] / ipcsg_sum for
                                                     each_ipcsg in original_ipcsg_vector.keys()}

                            for industry in competitors:
                                industry_patent = all_patent[
                                    (all_patent['企业名称'] == industry) & (year <= all_patent['年份']) & (
                                            all_patent['年份'] <= year + 4)]
                                if not industry_patent.empty:
                                    industry_patent_length = len(industry_patent)
                                    industry_patent['分类号'] = industry_patent['分类号'].apply(
                                        lambda x: list(extract_ipcsg_list(x)))
                                    ipcsg_index = list(set(
                                        [industry_patent['分类号'].iloc[i][j] for i in range(0, len(industry_patent)) for j
                                         in
                                         range(0, len(industry_patent['分类号'].iloc[i]))]))
                                    compare_ipcsg_vector = {each_ipcsg: 0 for each_ipcsg in ipcsg_index}
                                    for each_ipcsg in ipcsg_index:
                                        for i in range(industry_patent_length):
                                            if each_ipcsg in industry_patent['分类号'].iloc[i]:
                                                compare_ipcsg_vector[each_ipcsg] += 1
                                    compare_ipcsg_vector = {
                                        each_ipcsg: compare_ipcsg_vector[each_ipcsg] / industry_patent_length for
                                        each_ipcsg
                                        in compare_ipcsg_vector.keys()}

                                    technology_distance.loc[technology_index] = [industry, year + 4,
                                                                                 compare_ipcsg_vector]
                                    technology_index += 1

                                    ipcsg_sum = sum(
                                        [ipcsg_value ** 2 for ipcsg_value in compare_ipcsg_vector.values()]) ** 0.5
                                    compare_ipcsg_vector = {each_ipcsg: compare_ipcsg_vector[each_ipcsg] / ipcsg_sum
                                                            for each_ipcsg in compare_ipcsg_vector.keys()}

                                    common_key = original_ipcsg_vector.keys() & compare_ipcsg_vector.keys()
                                    distance = 1 - sum(
                                        [original_ipcsg_vector[i] * compare_ipcsg_vector[i] for i in common_key])
                                    total_distance += distance
                        average_distance = total_distance / len(competitors)
                        statistical_information.loc[index] = [institution, year + 4, average_distance]
                        index += 1

    statistical_information.to_excel(excel_writer='../data/生成数据/09技术距离/02五年期IPCSG技术距离.xlsx',
                                     index=False)
    technology_distance.to_excel(excel_writer='../data/生成数据/09技术距离/02五年期IPCSG技术向量.xlsx',
                                 index=False)
    print(mark + '执行完毕！')


def ipcmg_three_distance(mark):
    """在三年窗口期中某企业的三年期IPCMG矩阵与它的竞争对手的三年期IPCMG矩阵之间cosine技术距离的均值"""
    statistical_information = pd.DataFrame(columns=['企业名称', '年份', 'cptor3ywipcmgcsd'])
    index = 0

    technology_distance = pd.DataFrame(columns=['企业名称', '年份', '三年期IPCMG技术向量'])
    technology_index = 0

    all_patent = pd.read_excel(io='../data/生成数据/08竞争对手/专利数据/汽车产业专利数据(包含企业自身及竞争对手).xlsx',
                               usecols=[3, 4, 14],
                               converters={'分类号': lambda x: x.strip().strip(';'),
                                           '申请（专利权）人': lambda x: x.strip().strip(';'),
                                           '年份': int})
    company = all_patent['申请（专利权）人'].str.split(';', expand=True).stack().reset_index(level=1, drop=True)
    company = company.rename('企业名称').str.strip()
    all_patent = all_patent.drop(labels=['申请（专利权）人'], axis=1).join(company).reset_index(drop=True)

    institutions = pd.read_excel(io='../data/生成数据/06专利数据/汽车产业(企业名称).xlsx')
    address = pd.read_excel(io='../data/生成数据/01企业名称修改表/13整车数据ZZCMCZ编码表.xlsx',
                            usecols=[0, 1],
                            converters={'ZZCMC编号': str,
                                        'ZZCMC企业名称': lambda x: x.strip()})
    address = address.rename(columns={'ZZCMC企业名称': '企业名称'})
    institutions = pd.merge(left=institutions, right=address, how='left', on=['企业名称'])
    institutions = institutions[institutions['ZZCMC编号'].notnull()].reset_index(drop=True)

    vehicle_data = pd.read_excel(io='../data/生成数据/02整车数据/03整车数据(第三步修改).xlsx',
                                 usecols=[8, 40, 43, 49],
                                 converters={'ZZCMC': lambda x: x.strip(),
                                             'CLLX': lambda x: x.strip()},
                                 parse_dates=['CXSXRQ', 'GGSXRQ'])
    vehicle_data['CXSXRQ'] = vehicle_data['CXSXRQ'].fillna(pd.to_datetime('2017-12-31'))

    for year in range(1985, 2013):
        filter_data = vehicle_data[((pd.to_datetime(str(year)) <= vehicle_data['GGSXRQ']) & (
                vehicle_data['GGSXRQ'] <= pd.to_datetime(str(year + 2) + '-12-31'))) | (
                                           (pd.to_datetime(str(year)) <= vehicle_data['CXSXRQ']) & (
                                           vehicle_data['CXSXRQ'] <= pd.to_datetime(str(year + 2) + '-12-31')))]

        if not filter_data.empty:
            filter_data = filter_data.reset_index(drop=True)
            company_index = filter_data['ZZCMC'].drop_duplicates(keep='last').sort_values(ascending=True).reset_index(
                drop=True)
            company_index = list(company_index)
            company_car_type_counts = {
                company: filter_data[filter_data['ZZCMC'] == company]['CLLX'].value_counts().to_dict()
                for company in company_index}

            for institution_index in range(len(institutions)):
                institution = institutions['企业名称'].iloc[institution_index]
                if institution in company_car_type_counts:
                    original_car_type = company_car_type_counts[institution]
                    competitors = []
                    for i in range(0, len(company_index)):
                        if company_index[i] != institution:
                            compare_car_type = company_car_type_counts[company_index[i]]
                            company_key = original_car_type.keys() & compare_car_type.keys()
                            if company_key:
                                competitors.append(company_index[i])
                    if len(competitors) > 0:
                        total_distance = 0
                        original_patent = all_patent[
                            (all_patent['企业名称'] == institution) & (year <= all_patent['年份']) & (
                                    all_patent['年份'] <= year + 2)]
                        if not original_patent.empty:
                            original_patent_length = len(original_patent)
                            original_patent['分类号'] = original_patent['分类号'].apply(lambda x: list(extract_ipcmg_list(x)))
                            ipcsg_index = list(set(
                                [original_patent['分类号'].iloc[i][j] for i in range(0, len(original_patent)) for j in
                                 range(0, len(original_patent['分类号'].iloc[i]))]))
                            original_ipcsg_vector = {each_ipcsg: 0 for each_ipcsg in ipcsg_index}
                            for each_ipcsg in ipcsg_index:
                                for i in range(original_patent_length):
                                    if each_ipcsg in original_patent['分类号'].iloc[i]:
                                        original_ipcsg_vector[each_ipcsg] += 1
                            original_ipcsg_vector = {
                                each_ipcsg: original_ipcsg_vector[each_ipcsg] / original_patent_length
                                for each_ipcsg
                                in original_ipcsg_vector.keys()}

                            technology_distance.loc[technology_index] = [institution, year + 2,
                                                                         original_ipcsg_vector]
                            technology_index += 1

                            ipcsg_sum = sum([ipcsg_value ** 2 for ipcsg_value in original_ipcsg_vector.values()]) ** 0.5
                            original_ipcsg_vector = {each_ipcsg: original_ipcsg_vector[each_ipcsg] / ipcsg_sum for
                                                     each_ipcsg in original_ipcsg_vector.keys()}

                            for industry in competitors:
                                industry_patent = all_patent[
                                    (all_patent['企业名称'] == industry) & (year <= all_patent['年份']) & (
                                            all_patent['年份'] <= year + 2)]
                                if not industry_patent.empty:
                                    industry_patent_length = len(industry_patent)
                                    industry_patent['分类号'] = industry_patent['分类号'].apply(
                                        lambda x: list(extract_ipcmg_list(x)))
                                    ipcsg_index = list(set(
                                        [industry_patent['分类号'].iloc[i][j] for i in range(0, len(industry_patent)) for j
                                         in
                                         range(0, len(industry_patent['分类号'].iloc[i]))]))
                                    compare_ipcsg_vector = {each_ipcsg: 0 for each_ipcsg in ipcsg_index}
                                    for each_ipcsg in ipcsg_index:
                                        for i in range(industry_patent_length):
                                            if each_ipcsg in industry_patent['分类号'].iloc[i]:
                                                compare_ipcsg_vector[each_ipcsg] += 1
                                    compare_ipcsg_vector = {
                                        each_ipcsg: compare_ipcsg_vector[each_ipcsg] / industry_patent_length for
                                        each_ipcsg
                                        in compare_ipcsg_vector.keys()}

                                    technology_distance.loc[technology_index] = [industry, year + 2,
                                                                                 compare_ipcsg_vector]
                                    technology_index += 1

                                    ipcsg_sum = sum(
                                        [ipcsg_value ** 2 for ipcsg_value in compare_ipcsg_vector.values()]) ** 0.5
                                    compare_ipcsg_vector = {each_ipcsg: compare_ipcsg_vector[each_ipcsg] / ipcsg_sum
                                                            for each_ipcsg in compare_ipcsg_vector.keys()}

                                    common_key = original_ipcsg_vector.keys() & compare_ipcsg_vector.keys()
                                    distance = 1 - sum(
                                        [original_ipcsg_vector[i] * compare_ipcsg_vector[i] for i in common_key])
                                    total_distance += distance
                        average_distance = total_distance / len(competitors)
                        statistical_information.loc[index] = [institution, year + 2, average_distance]
                        index += 1

    statistical_information.to_excel(excel_writer='../data/生成数据/09技术距离/03三年期IPCMG技术距离.xlsx',
                                     index=False)
    technology_distance.to_excel(excel_writer='../data/生成数据/09技术距离/03三年期IPCMG技术向量.xlsx',
                                 index=False)
    print(mark + '执行完毕！')


def ipcmg_five_distance(mark):
    """在五年窗口期中某企业的五年期IPCMG矩阵与它的竞争对手的五年期IPCMG矩阵之间cosine技术距离的均值"""
    statistical_information = pd.DataFrame(columns=['企业名称', '年份', 'cptor5ywipcmgcsd'])
    index = 0

    technology_distance = pd.DataFrame(columns=['企业名称', '年份', '五年期IPCMG技术向量'])
    technology_index = 0

    all_patent = pd.read_excel(io='../data/生成数据/08竞争对手/专利数据/汽车产业专利数据(包含企业自身及竞争对手).xlsx',
                               usecols=[3, 4, 14],
                               converters={'分类号': lambda x: x.strip().strip(';'),
                                           '申请（专利权）人': lambda x: x.strip().strip(';'),
                                           '年份': int})
    company = all_patent['申请（专利权）人'].str.split(';', expand=True).stack().reset_index(level=1, drop=True)
    company = company.rename('企业名称').str.strip()
    all_patent = all_patent.drop(labels=['申请（专利权）人'], axis=1).join(company).reset_index(drop=True)

    institutions = pd.read_excel(io='../data/生成数据/06专利数据/汽车产业(企业名称).xlsx')
    address = pd.read_excel(io='../data/生成数据/01企业名称修改表/13整车数据ZZCMCZ编码表.xlsx',
                            usecols=[0, 1],
                            converters={'ZZCMC编号': str,
                                        'ZZCMC企业名称': lambda x: x.strip()})
    address = address.rename(columns={'ZZCMC企业名称': '企业名称'})
    institutions = pd.merge(left=institutions, right=address, how='left', on=['企业名称'])
    institutions = institutions[institutions['ZZCMC编号'].notnull()].reset_index(drop=True)

    vehicle_data = pd.read_excel(io='../data/生成数据/02整车数据/03整车数据(第三步修改).xlsx',
                                 usecols=[8, 40, 43, 49],
                                 converters={'ZZCMC': lambda x: x.strip(),
                                             'CLLX': lambda x: x.strip()},
                                 parse_dates=['CXSXRQ', 'GGSXRQ'])
    vehicle_data['CXSXRQ'] = vehicle_data['CXSXRQ'].fillna(pd.to_datetime('2017-12-31'))

    for year in range(1985, 2011):
        filter_data = vehicle_data[((pd.to_datetime(str(year)) <= vehicle_data['GGSXRQ']) & (
                vehicle_data['GGSXRQ'] <= pd.to_datetime(str(year + 4) + '-12-31'))) | (
                                           (pd.to_datetime(str(year)) <= vehicle_data['CXSXRQ']) & (
                                           vehicle_data['CXSXRQ'] <= pd.to_datetime(str(year + 4) + '-12-31')))]

        if not filter_data.empty:
            filter_data = filter_data.reset_index(drop=True)
            company_index = filter_data['ZZCMC'].drop_duplicates(keep='last').sort_values(ascending=True).reset_index(
                drop=True)
            company_index = list(company_index)
            company_car_type_counts = {
                company: filter_data[filter_data['ZZCMC'] == company]['CLLX'].value_counts().to_dict()
                for company in company_index}

            for institution_index in range(len(institutions)):
                institution = institutions['企业名称'].iloc[institution_index]
                if institution in company_car_type_counts:
                    original_car_type = company_car_type_counts[institution]
                    competitors = []
                    for i in range(0, len(company_index)):
                        if company_index[i] != institution:
                            compare_car_type = company_car_type_counts[company_index[i]]
                            company_key = original_car_type.keys() & compare_car_type.keys()
                            if company_key:
                                competitors.append(company_index[i])
                    if len(competitors) > 0:
                        total_distance = 0
                        original_patent = all_patent[
                            (all_patent['企业名称'] == institution) & (year <= all_patent['年份']) & (
                                    all_patent['年份'] <= year + 4)]
                        if not original_patent.empty:
                            original_patent_length = len(original_patent)
                            original_patent['分类号'] = original_patent['分类号'].apply(lambda x: list(extract_ipcmg_list(x)))
                            ipcsg_index = list(set(
                                [original_patent['分类号'].iloc[i][j] for i in range(0, len(original_patent)) for j in
                                 range(0, len(original_patent['分类号'].iloc[i]))]))
                            original_ipcsg_vector = {each_ipcsg: 0 for each_ipcsg in ipcsg_index}
                            for each_ipcsg in ipcsg_index:
                                for i in range(original_patent_length):
                                    if each_ipcsg in original_patent['分类号'].iloc[i]:
                                        original_ipcsg_vector[each_ipcsg] += 1
                            original_ipcsg_vector = {
                                each_ipcsg: original_ipcsg_vector[each_ipcsg] / original_patent_length
                                for each_ipcsg
                                in original_ipcsg_vector.keys()}

                            technology_distance.loc[technology_index] = [institution, year + 4,
                                                                         original_ipcsg_vector]
                            technology_index += 1

                            ipcsg_sum = sum([ipcsg_value ** 2 for ipcsg_value in original_ipcsg_vector.values()]) ** 0.5
                            original_ipcsg_vector = {each_ipcsg: original_ipcsg_vector[each_ipcsg] / ipcsg_sum for
                                                     each_ipcsg in original_ipcsg_vector.keys()}

                            for industry in competitors:
                                industry_patent = all_patent[
                                    (all_patent['企业名称'] == industry) & (year <= all_patent['年份']) & (
                                            all_patent['年份'] <= year + 4)]
                                if not industry_patent.empty:
                                    industry_patent_length = len(industry_patent)
                                    industry_patent['分类号'] = industry_patent['分类号'].apply(
                                        lambda x: list(extract_ipcmg_list(x)))
                                    ipcsg_index = list(set(
                                        [industry_patent['分类号'].iloc[i][j] for i in range(0, len(industry_patent)) for j
                                         in
                                         range(0, len(industry_patent['分类号'].iloc[i]))]))
                                    compare_ipcsg_vector = {each_ipcsg: 0 for each_ipcsg in ipcsg_index}
                                    for each_ipcsg in ipcsg_index:
                                        for i in range(industry_patent_length):
                                            if each_ipcsg in industry_patent['分类号'].iloc[i]:
                                                compare_ipcsg_vector[each_ipcsg] += 1
                                    compare_ipcsg_vector = {
                                        each_ipcsg: compare_ipcsg_vector[each_ipcsg] / industry_patent_length for
                                        each_ipcsg
                                        in compare_ipcsg_vector.keys()}

                                    technology_distance.loc[technology_index] = [industry, year + 4,
                                                                                 compare_ipcsg_vector]
                                    technology_index += 1

                                    ipcsg_sum = sum(
                                        [ipcsg_value ** 2 for ipcsg_value in compare_ipcsg_vector.values()]) ** 0.5
                                    compare_ipcsg_vector = {each_ipcsg: compare_ipcsg_vector[each_ipcsg] / ipcsg_sum
                                                            for each_ipcsg in compare_ipcsg_vector.keys()}

                                    common_key = original_ipcsg_vector.keys() & compare_ipcsg_vector.keys()
                                    distance = 1 - sum(
                                        [original_ipcsg_vector[i] * compare_ipcsg_vector[i] for i in common_key])
                                    total_distance += distance
                        average_distance = total_distance / len(competitors)
                        statistical_information.loc[index] = [institution, year + 4, average_distance]
                        index += 1

    statistical_information.to_excel(excel_writer='../data/生成数据/09技术距离/04五年期IPCMG技术距离.xlsx',
                                     index=False)
    technology_distance.to_excel(excel_writer='../data/生成数据/09技术距离/04五年期IPCMG技术向量.xlsx',
                                 index=False)
    print(mark + '执行完毕！')


def construct_panel_data():
    """向汽车产业面板数据添加指标"""
    auto_panel = pd.read_excel(io='../data/生成数据/05面板数据/汽车产业面板数据(第3版).xlsx',
                               converters={'YEAR': int})
    ipcsg_three = pd.read_excel(io='../data/生成数据/09技术距离/01三年期IPCSG技术距离.xlsx',
                                converters={'企业名称': lambda x: x.strip(),
                                            '年份': int}).rename(
        columns={'企业名称': 'OGNM', '年份': 'YEAR'})
    auto_panel = pd.merge(left=auto_panel, right=ipcsg_three, how='left', on=['OGNM', 'YEAR'])

    ipcsg_five = pd.read_excel(io='../data/生成数据/09技术距离/02五年期IPCSG技术距离.xlsx',
                               converters={'企业名称': lambda x: x.strip(),
                                           '年份': int}).rename(
        columns={'企业名称': 'OGNM', '年份': 'YEAR'})
    auto_panel = pd.merge(left=auto_panel, right=ipcsg_five, how='left', on=['OGNM', 'YEAR'])

    ipcmg_three = pd.read_excel(io='../data/生成数据/09技术距离/03三年期IPCMG技术距离.xlsx',
                                converters={'企业名称': lambda x: x.strip(),
                                            '年份': int}).rename(
        columns={'企业名称': 'OGNM', '年份': 'YEAR'})
    auto_panel = pd.merge(left=auto_panel, right=ipcmg_three, how='left', on=['OGNM', 'YEAR'])

    ipcmg_five = pd.read_excel(io='../data/生成数据/09技术距离/04五年期IPCMG技术距离.xlsx',
                               converters={'企业名称': lambda x: x.strip(),
                                           '年份': int}).rename(
        columns={'企业名称': 'OGNM', '年份': 'YEAR'})
    auto_panel = pd.merge(left=auto_panel, right=ipcmg_five, how='left', on=['OGNM', 'YEAR'])
    auto_panel.to_excel(excel_writer='../data/生成数据/05面板数据/汽车产业面板数据(第4版).xlsx',
                        index=False)


if __name__ == "__main__":
    start_time = datetime.datetime.now()
    p = Pool(4)
    p.apply_async(ipcsg_three_distance, ('三年期IPCSG技术距离',))
    p.apply_async(ipcsg_five_distance, ('五年期IPCSG技术距离',))
    p.apply_async(ipcmg_three_distance, ('三年期IPCMG技术距离',))
    p.apply_async(ipcmg_five_distance, ('五年期IPCMG技术距离',))
    p.close()
    p.join()
    construct_panel_data()
    end_time = datetime.datetime.now()
    run_time = end_time - start_time
    print('程序开始时间：', start_time, '\n程序结束时间：', end_time, '\n程序运行时间：', run_time)
