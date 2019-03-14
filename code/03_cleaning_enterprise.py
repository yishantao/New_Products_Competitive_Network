# -*- coding:utf-8 -*-
"""第二步：清洗汽车新产品企业名称"""

import datetime
import Levenshtein
import pandas as pd


def address_analysis():
    """地址相似度分析"""
    modify_table = pd.read_csv(filepath_or_buffer='../data/生成数据/01企业名称修改表/05ZZCMC对OGNM企业名称清洗表(第一版).csv',
                               encoding='utf-8-sig',
                               engine='python')

    address = modify_table[(modify_table['生产地址A'].notnull()) & (modify_table['生产地址B'].notnull())]
    address['地址相似度'] = [Levenshtein.ratio(address['生产地址A'].iloc[i], address['生产地址B'].iloc[i])
                        for i in range(len(address))]
    modify_table['地址相似度'] = None
    modify_table['地址相似度'].update(address['地址相似度'])

    modify_table.to_excel(excel_writer='../data/生成数据/01企业名称修改表/06ZZCMC对OGNM企业名称清洗表(第二版).xlsx',
                          index=False)


def finalized_version():
    """修改表定稿版"""
    modify_table = pd.read_excel(io='../data/生成数据/01企业名称修改表/07ZZCMC对OGNM企业名称清洗表(第三版).xlsx')
    modify_table = modify_table[modify_table['审核'] == '是'].reset_index(drop=True)
    modify_table = modify_table.drop_duplicates(subset=['ZZCMC名称A'], keep='last').reset_index(drop=True)
    del modify_table['审核']
    del modify_table['标记']
    modify_table['统一后ZZCMC企业名称'] = modify_table['OGNM名称B'].copy()
    modify_table.to_excel(excel_writer='../data/生成数据/01企业名称修改表/08ZZCMC对OGNM企业名称清洗表(定稿版).xlsx',
                          index=False)


def cleaning_vehicle():
    """清洗整车数据"""
    vehicle = pd.read_excel(io='../data/生成数据/02整车数据/01整车数据(第一步修改).xlsx',
                            converters={'ZZCMC': lambda x: x.strip()})

    modify_table = pd.read_excel(io='../data/生成数据/01企业名称修改表/08ZZCMC对OGNM企业名称清洗表(定稿版).xlsx',
                                 usecols=[0, 2],
                                 converters={'ZZCMC名称A': lambda x: x.strip(),
                                             'OGNM名称B': lambda x: x.strip()})

    company_mapping = {modify_table['ZZCMC名称A'].iloc[i]: modify_table['OGNM名称B'].iloc[i]
                       for i in range(len(modify_table))}

    vehicle['ZZCMC'].update(vehicle['ZZCMC'].map(company_mapping))
    vehicle.to_excel(excel_writer='../data/生成数据/02整车数据/02整车数据(第二步修改).xlsx',
                     index=False)


def code_table():
    """构建整车数据编码表"""
    automobile = pd.read_excel(io='../data/生成数据/02整车数据/02整车数据(第二步修改).xlsx',
                               usecols=[40],
                               converters={'ZZCMC': lambda x: x.strip()})
    automobile = automobile[automobile['ZZCMC'].notnull()].reset_index(drop=True)
    automobile = automobile.drop_duplicates(subset=['ZZCMC'], keep='last').reset_index(drop=True)
    automobile['ZZCMC编号'] = [i + 1 for i in range(len(automobile))]
    automobile = automobile.rename(columns={'ZZCMC': 'ZZCMC企业名称'})

    enterprise = pd.read_excel(io='../data/原始数据/02汽车面板数据以及说明/AmIndustryWTOPanel20181128.xlsx',
                               usecols=[0, 2],
                               converters={'IDNO': int,
                                           'OGNM': lambda x: x.strip()})
    enterprise = enterprise.drop_duplicates(subset=['OGNM'], keep='last').reset_index(drop=True)
    enterprise = enterprise.rename(columns={'IDNO': 'AmIndustryWTOPanel中IDNO',
                                            'OGNM': 'ZZCMC企业名称'})

    automobile = pd.merge(left=automobile, right=enterprise, on=['ZZCMC企业名称'], how='left').reset_index(drop=True)
    automobile = automobile[['ZZCMC企业名称', 'ZZCMC编号', 'AmIndustryWTOPanel中IDNO']]
    automobile.to_excel(excel_writer='../data/生成数据/01企业名称修改表/09整车数据ZZCMCZ编码表.xlsx',
                        index=False)


if __name__ == "__main__":
    # 运行时间1分钟
    start_time = datetime.datetime.now()
    code_table()
    end_time = datetime.datetime.now()
    run_time = end_time - start_time
    print('程序开始时间：', start_time, '\n程序结束时间：', end_time, '\n程序运行时间：', run_time)
