# -*- coding:utf-8 -*-
"""第三步：构建3年窗口期的汽车新产品竞争网络
   第四步：构建5年窗口期的汽车新产品竞争网络"""

import datetime
import pandas as pd


def enterprise_symbol_mapping():
    """构建企业名称与编码对应映射"""
    df = pd.read_excel(io='../data/生成数据/01企业名称修改表/09整车数据ZZCMCZ编码表.xlsx',
                       usecols=[0, 1],
                       converters={'ZZCMC企业名称': lambda s: s.strip(),
                                   'ZZCMC编号': str})

    mapping = {}
    for i in range(len(df)):
        mapping[df['ZZCMC企业名称'][i]] = df['ZZCMC编号'][i]
    return mapping


def patent_to_symbol():
    """将《整车数据》中的企业名称改成在《整车数据ZZCMCZ编码表》中的对应编码"""
    automobile = pd.read_excel(io='../data/生成数据/02整车数据/02整车数据(第二步修改).xlsx',
                               usecols=[8, 40, 43, 49],
                               converters={'CLLX': lambda x: x.strip(),
                                           'ZZCMC': lambda x: x.strip()},
                               parse_dates=['CXSXRQ', 'GGSXRQ'])

    automobile['ZZCMC'] = automobile['ZZCMC'].map(enterprise_symbol_mapping())
    automobile.to_excel(excel_writer='../data/生成数据/02整车数据/03整车数据(企业名称修改为ZZCMC编号).xlsx',
                        index=None)


def relationship_matrix(df):
    """构建竞争关系矩阵"""
    company_index = df['ZZCMC'].drop_duplicates(keep='last').astype(int).sort_values(ascending=True).astype(
        str).reset_index(drop=True)
    company_index = list(company_index)
    company_length = len(company_index)
    company_car_type_counts = {company: df[df['ZZCMC'] == company]['CLLX'].value_counts().to_dict()
                               for company in company_index}

    patent_matrix = pd.DataFrame(columns=company_index, index=company_index).fillna(0)
    patent_matrix_values = patent_matrix.values
    for i in range(company_length):
        i_car_type = company_car_type_counts[company_index[i]]
        for j in range(i + 1, company_length):
            j_car_type = company_car_type_counts[company_index[j]]
            company_key = i_car_type.keys() & j_car_type.keys()
            competition_intensity = sum([i_car_type[z] * j_car_type[z] for z in company_key])
            patent_matrix_values[i, j] = competition_intensity
            patent_matrix_values[j, i] = competition_intensity
    patent_matrix_values = pd.DataFrame(patent_matrix_values)
    patent_matrix_values.columns = patent_matrix.columns
    patent_matrix_values.index = patent_matrix.index
    return patent_matrix_values


def export_three_matrix_file():
    """导出三年期竞争关系矩阵"""
    automobile = pd.read_excel(io='../data/生成数据/02整车数据/03整车数据(企业名称修改为ZZCMC编号).xlsx',
                               converters={'ZZCMC': str,
                                           'CLLX': str},
                               parse_dates=['CXSXRQ', 'GGSXRQ'])
    automobile['CXSXRQ'] = automobile['CXSXRQ'].fillna(pd.to_datetime('2017-12-31'))

    for year in range(1985, 2015):
        filter_data = automobile[((pd.to_datetime(str(year)) <= automobile['GGSXRQ']) & (
                    automobile['GGSXRQ'] <= pd.to_datetime(str(year + 2) + '-12-31'))) | (
                                             (pd.to_datetime(str(year)) <= automobile['CXSXRQ']) & (
                                                 automobile['CXSXRQ'] <= pd.to_datetime(str(year + 2) + '-12-31')))]
        if not filter_data.empty:
            filter_data = filter_data.reset_index(drop=True)
            patent_matrix = relationship_matrix(filter_data)
            patent_matrix.to_csv(
                path_or_buf='../data/生成数据/03关系矩阵/三年期/' + str(year) + '-' + str(year + 2) + '年竞争关系矩阵.csv')


def export_five_matrix_file():
    """导出五年期竞争关系矩阵"""
    automobile = pd.read_excel(io='../data/生成数据/02整车数据/03整车数据(企业名称修改为ZZCMC编号).xlsx',
                               converters={'ZZCMC': str,
                                           'CLLX': str},
                               parse_dates=['CXSXRQ', 'GGSXRQ'])
    automobile['CXSXRQ'] = automobile['CXSXRQ'].fillna(pd.to_datetime('2017-12-31'))

    for year in range(1985, 2015):
        filter_data = automobile[((pd.to_datetime(str(year)) <= automobile['GGSXRQ']) & (
                    automobile['GGSXRQ'] <= pd.to_datetime(str(year + 4) + '-12-31'))) | (
                                             (pd.to_datetime(str(year)) <= automobile['CXSXRQ']) & (
                                                 automobile['CXSXRQ'] <= pd.to_datetime(str(year + 4) + '-12-31')))]
        if not filter_data.empty:
            filter_data = filter_data.reset_index(drop=True)
            patent_matrix = relationship_matrix(filter_data)
            patent_matrix.to_csv(
                path_or_buf='../data/生成数据/03关系矩阵/五年期/' + str(year) + '-' + str(year + 4) + '年竞争关系矩阵.csv')


if __name__ == "__main__":
    # 运行时间20分钟
    start_time = datetime.datetime.now()
    export_three_matrix_file()
    export_five_matrix_file()
    end_time = datetime.datetime.now()
    run_time = end_time - start_time
    print('程序开始时间：', start_time, '\n程序结束时间：', end_time, '\n程序运行时间：', run_time)
