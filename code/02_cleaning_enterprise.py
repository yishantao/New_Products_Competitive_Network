# -*- coding:utf-8 -*-
"""第二步：清洗汽车新产品企业名称"""

import csv
import jieba
import datetime
import warnings
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


def panel_enterprise():
    """获取面板数据中的企业名称"""
    enterprise = pd.read_excel(io='../data/原始数据/02汽车面板数据以及说明/AmIndustryWTOPanel20181128.xlsx',
                               usecols=[2])
    enterprise = enterprise.drop_duplicates(subset=['OGNM'], keep='last').reset_index(drop=True)

    feature_one = pd.read_excel(io='../data/原始数据/04特征数据/交通运输设备制造业1996-2010.xlsx',
                                usecols=[0, 2, 8],
                                converters={'年份': int,
                                            '企业名称': lambda x: x.strip(),
                                            '村（街、门牌号）': lambda x: x.strip()})
    feature_one = feature_one.sort_values(by=['年份'], ascending=True).reset_index(drop=True)
    feature_one = feature_one.rename(columns={'企业名称': 'OGNM', '村（街、门牌号）': '生产地址'})[['OGNM', '生产地址']]

    feature_two = pd.read_excel(io='../data/原始数据/04特征数据/交通运输设备制造业2011TB.xlsx',
                                usecols=[1, 4],
                                converters={'详细名称': lambda x: x.strip(),
                                            '详细地址': lambda x: x.strip()})
    feature_two = feature_two.rename(columns={'详细名称': 'OGNM', '详细地址': '生产地址'})

    feature_three = pd.read_excel(io='../data/原始数据/04特征数据/交通运输设备制造业2012TB.xlsx',
                                  usecols=[1, 4],
                                  converters={'_详细名称': lambda x: x.strip(),
                                              '_详细地址': lambda x: x.strip()})
    feature_three = feature_three.rename(columns={'_详细名称': 'OGNM', '_详细地址': '生产地址'})

    feature_four = pd.read_excel(io='../data/原始数据/04特征数据/交通运输设备制造业2013TB.xlsx',
                                 usecols=[1, 11],
                                 converters={'单位详细名称': lambda x: x.strip(),
                                             '地址': lambda x: x.strip()})
    feature_four = feature_four.rename(columns={'单位详细名称': 'OGNM', '地址': '生产地址'})

    feature = pd.concat(objs=[feature_one, feature_two, feature_three, feature_four], ignore_index=True)
    feature = feature.drop_duplicates(subset=['OGNM'], keep='last').reset_index(drop=True)

    enterprise = pd.merge(left=enterprise, right=feature, on=['OGNM'], how='left')
    return enterprise


def automobile_enterprise():
    """读取《整车数据》"""
    automobile = pd.read_excel(io='../data/生成数据/02整车数据/01整车数据(第一步修改).xlsx',
                               usecols=[5, 40, 49],
                               parse_dates=['GGSXRQ'],
                               converters={'ZZCMC': lambda x: x.strip(),
                                           'SCDZ': lambda x: x.strip()})
    automobile = automobile[['ZZCMC', 'SCDZ', 'GGSXRQ']]
    automobile = automobile[automobile['ZZCMC'].notnull()].reset_index(drop=True)
    automobile = automobile.sort_values(by=['GGSXRQ'], ascending=True).reset_index(drop=True)
    automobile = automobile.drop_duplicates(subset=['ZZCMC'], keep='last').reset_index(drop=True)
    del automobile['GGSXRQ']
    return automobile


def text_similarity_analysis(file_path):
    """修改表(具体写入过程，文本相似度分析)"""
    product_enterprises = file_path.reset_index(drop=True)
    panel_enterprises = panel_enterprise()

    texts1 = [[word.strip() for word in jieba.cut(product_enterprises['ZZCMC'][i])
               if word.strip() and word.strip() not in stop_list]
              for i in range(len(product_enterprises))]
    texts2 = [[word.strip() for word in jieba.cut(panel_enterprises['OGNM'][i])
               if word.strip() and word.strip() not in stop_list]
              for i in range(len(panel_enterprises))]
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
            if sims[j] >= 0.3:
                writer.writerow([product_enterprises['ZZCMC'].iloc[i], product_enterprises['SCDZ'].iloc[i],
                                 panel_enterprises['OGNM'].iloc[j - product_length],
                                 panel_enterprises['生产地址'].iloc[j - product_length], sims[j]])


if __name__ == "__main__":
    # 运行时间1小时
    start_time = datetime.datetime.now()
    with open('../data/生成数据/01企业名称修改表/05ZZCMC对OGNM企业名称清洗表(第一版).csv', 'w', encoding='utf-8-sig', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['ZZCMC名称A', '生产地址A', 'OGNM名称B', '生产地址B', '企业名称相似度'])
        enterprise_name = automobile_enterprise()
        group_number = int(len(enterprise_name) / 300)
        file_list = [enterprise_name.iloc[(group * 300):(group * 300 + 300)] for group in range(group_number)]
        file_list.append(enterprise_name.iloc[group_number * 300:])

        p = Pool(4)
        for each_file in file_list:
            p.apply_async(transfer_program, (each_file,), callback=text_similarity_analysis)
        p.close()
        p.join()
    end_time = datetime.datetime.now()
    run_time = end_time - start_time
    print('程序开始时间：', start_time, '\n程序结束时间：', end_time, '\n程序运行时间：', run_time)
