# -*- coding:utf-8 -*-
"""将汽车新产品竞争网络中的企业名称与汽车产业专利数据中的企业名称进行匹配清洗，
使得同一家企业在汽车新产品竞争网络中的名称与专利数据中的名称保持一致"""

import csv
import jieba
import warnings
import datetime
import pandas as pd

from multiprocessing import Pool

warnings.filterwarnings(action='ignore', category=UserWarning, module='gensim')
from gensim import corpora, models, similarities

# 设置停用词
stop_list = set('- ( )'.split())
jieba.suggest_freq(('有限', '公司'), True)
jieba.suggest_freq(('有限', '责任'), True)
jieba.suggest_freq(('汽车', '集团'), True)
jieba.suggest_freq(('集团', '公司'), True)


def transfer_program(parameter):
    """中转函数"""
    return parameter


def judgement_patent_years(s):
    """根据申请号判断专利发生年份"""
    if int(s[2:4]) >= 85:
        s = '19' + s[2:4]
    elif int(s[2:4]) <= 3:
        s = '20' + s[2:4]
    else:
        s = s[2:6]
    return s


def delete_unqualified_enterprises(df):
    """删除不符合条件的企业"""
    flag_list = ['公司', '企业', '厂', '会社', '局', '处', '所', '中心', '会', '局', '室',
                 '学校', '学院', '大学', '部', '局', '系', '医院', '株', '机构', '科技']
    for i in range(len(df)):
        company_name = df['组织名称'][i]
        if '·' in company_name or len(company_name) < 7 or '.' in company_name:
            symbol = 0
            for flag in flag_list:
                if flag in company_name:
                    symbol = 1
                    break
            if symbol == 0:
                df['组织名称'][i] = 'NONE'
    df = df.drop(df[df['组织名称'] == 'NONE'].index.tolist()).reset_index(drop=True)
    return df


def panel_enterprise():
    """获取面板数据中的企业名称"""
    enterprise = pd.read_excel(io='../data/原始数据/02汽车面板数据以及说明/AmIndustryWTOPanel20181128.xlsx',
                               usecols=[2])
    enterprise = enterprise.drop_duplicates(subset=['OGNM'], keep='last').reset_index(drop=True)
    return enterprise


def patent_business():
    """提取汽车产业专利数据中的企业名称"""
    patent = pd.read_excel(io='../data/原始数据/03专利数据/汽车产业专利数据(修订)_境内境外完整版.xlsx',
                           usecols=[0, 4, 11, 13],
                           converters={'申请号': lambda s: judgement_patent_years(s),
                                       '申请（专利权）人': lambda s: s.strip().strip(';')})

    patent = patent[['申请（专利权）人', '地址', '国省代码', '申请号']][patent['申请（专利权）人'].notnull()]
    patent.columns = ['组织名称', '地址', '国省代码', '年份']

    no_address = patent[['组织名称', '年份']]
    no_address = no_address.drop('组织名称', axis=1).join(
        no_address['组织名称'].str.split(';', expand=True).stack().reset_index(level=1, drop=True).rename(
            '组织名称').str.strip()).reset_index(drop=True)
    no_address = no_address.sort_values(by='年份').drop_duplicates(subset=['组织名称'],
                                                                 keep='last').reset_index(drop=True)
    no_address = no_address[no_address['组织名称'].str.len() >= 4]

    have_address = patent[['组织名称', '地址', '国省代码', '年份']]
    have_address['组织名称'] = have_address['组织名称'].str.split(';').str[0].apply(lambda s: s.strip())
    have_address = have_address.sort_values(by='年份').drop_duplicates(subset=['组织名称'],
                                                                     keep='last').reset_index(drop=True)
    have_address = have_address[have_address['组织名称'].str.len() >= 4]

    company = pd.concat([no_address, have_address],
                        ignore_index=True).drop_duplicates(subset=['组织名称'], keep='last').reset_index(drop=True)
    company = company[['组织名称', '地址']]
    company = delete_unqualified_enterprises(company)

    modify_table = panel_enterprise()
    company_mapping = {modify_table['OGNM'].iloc[i]: 'None'
                       for i in range(len(modify_table))}
    company['组织名称'].update(company['组织名称'].map(company_mapping))
    company = company[company['组织名称'] != 'None'].reset_index(drop=True)

    return company


def automobile_business():
    """读取《整车数据》中企业名称(已经修改过的企业名称不需要再清洗)"""
    automobile = pd.read_excel(io='../data/生成数据/02整车数据/02整车数据(第二步修改).xlsx',
                               usecols=[5, 40, 49],
                               parse_dates=['GGSXRQ'],
                               converters={'ZZCMC': lambda x: x.strip(),
                                           'SCDZ': lambda x: x.strip()})
    automobile = automobile[['ZZCMC', 'SCDZ', 'GGSXRQ']]
    automobile = automobile[automobile['ZZCMC'].notnull()].reset_index(drop=True)
    automobile = automobile.sort_values(by=['GGSXRQ'], ascending=True).reset_index(drop=True)
    automobile = automobile.drop_duplicates(subset=['ZZCMC'], keep='last').reset_index(drop=True)
    del automobile['GGSXRQ']

    modify_table = pd.read_excel(io='../data/生成数据/01企业名称修改表/08ZZCMC对OGNM企业名称清洗表(定稿版).xlsx',
                                 usecols=[0, 2],
                                 converters={'ZZCMC名称A': lambda x: x.strip(),
                                             'OGNM名称B': lambda x: x.strip()})
    company_mapping = {modify_table['OGNM名称B'].iloc[i]: 'None'
                       for i in range(len(modify_table))}

    automobile['ZZCMC'].update(automobile['ZZCMC'].map(company_mapping))
    automobile = automobile[automobile['ZZCMC'] != 'None'].reset_index(drop=True)
    return automobile


def text_similarity_analysis(file_path):
    """修改表(具体写入过程，文本相似度分析)"""

    automobile_enterprise = automobile_business()
    patent_enterprises = file_path.reset_index(drop=True)

    texts1 = [[word.strip() for word in jieba.cut(automobile_enterprise['ZZCMC'][i])
               if word.strip() and word.strip() not in stop_list]
              for i in range(len(automobile_enterprise))]
    texts2 = [[word.strip() for word in jieba.cut(patent_enterprises['组织名称'][i])
               if word.strip() and word.strip() not in stop_list]
              for i in range(len(patent_enterprises))]
    texts = texts1 + texts2

    dictionary = corpora.Dictionary(texts)
    corpus = [dictionary.doc2bow(text) for text in texts]
    tfidf = models.TfidfModel(corpus)
    corpus_tfidf = tfidf[corpus]
    index = similarities.MatrixSimilarity(corpus_tfidf)

    total_length = len(corpus)
    product_length = len(texts1)
    for i in range(product_length):
        new_vec_tfidf = tfidf[corpus[i]]
        sims = index[new_vec_tfidf]
        for j in range(product_length, total_length):
            if sims[j] >= 0.35:
                writer.writerow([automobile_enterprise['ZZCMC'].iloc[i], automobile_enterprise['SCDZ'].iloc[i],
                                 patent_enterprises['组织名称'].iloc[j - product_length],
                                 patent_enterprises['地址'].iloc[j - product_length], sims[j]])


if __name__ == "__main__":
    # 运行时间2小时
    start_time = datetime.datetime.now()
    with open('../data/生成数据/01企业名称修改表/09ZZCMC对专利数据企业名称清洗表(第一版).csv', 'w', encoding='utf-8-sig', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['汽车新产品竞争网络企业名称A', '生产地址A', '汽车产业专利数据企业名称B', '生产地址B', '企业名称相似度'])
        enterprise_name = patent_business()
        group_number = int(len(enterprise_name) / 5000)
        file_list = [enterprise_name.iloc[(group * 5000):(group * 5000 + 5000)] for group in range(group_number)]
        file_list.append(enterprise_name.iloc[group_number * 5000:])

        p = Pool(4)
        for each_file in file_list:
            p.apply_async(transfer_program, (each_file,), callback=text_similarity_analysis)
        p.close()
        p.join()
    end_time = datetime.datetime.now()
    run_time = end_time - start_time
    print('程序开始时间：', start_time, '\n程序结束时间：', end_time, '\n程序运行时间：', run_time)
