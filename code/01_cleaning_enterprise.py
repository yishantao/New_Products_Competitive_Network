# -*- coding:utf-8 -*-
"""第一步：清洗汽车新产品企业名称"""

import csv
import jieba
import datetime
import warnings
import Levenshtein
import pandas as pd

warnings.filterwarnings(action='ignore', category=UserWarning, module='gensim')
from gensim import corpora, models, similarities

# 设置停用词
stop_list = set('- ( )'.split())
jieba.suggest_freq(('有限', '公司'), True)
jieba.suggest_freq(('有限', '责任'), True)
jieba.suggest_freq(('汽车', '集团'), True)
jieba.suggest_freq(('集团', '公司'), True)


def read_data():
    """读取《整车数据》"""
    automobile = pd.read_csv(filepath_or_buffer='../data/原始数据/01汽车新产品/整车数据.csv',
                             engine='python',
                             usecols=[5, 40, 49],
                             parse_dates=['GGSXRQ'],
                             converters={'ZZCMC': lambda x: x.strip(),
                                         'SCDZ': lambda x: x.strip()})
    automobile = automobile[['ZZCMC', 'SCDZ', 'GGSXRQ']]
    automobile = automobile[automobile['ZZCMC'].notnull()].reset_index(drop=True)
    automobile = automobile.sort_values(by=['GGSXRQ'], ascending=True).reset_index(drop=True)
    automobile = automobile.drop_duplicates(subset=['ZZCMC'], keep='last').reset_index(drop=True)
    return automobile


def text_similarity_analysis():
    """组织名称文本相似度分析"""
    enterprise_name = read_data()

    texts = [[word.strip() for word in jieba.cut(enterprise_name['ZZCMC'][i])
              if word.strip() and word.strip() not in stop_list]
             for i in range(len(enterprise_name))]

    dictionary = corpora.Dictionary(texts)
    corpus = [dictionary.doc2bow(text) for text in texts]
    tfidf = models.TfidfModel(corpus)
    corpus_tfidf = tfidf[corpus]
    index = similarities.MatrixSimilarity(corpus_tfidf)

    return corpus, tfidf, index, enterprise_name


def write_data(corpus, tfidf, index, enterprise_name, writer):
    """数据写入文件具体函数"""
    for i in range(len(enterprise_name)):
        new_vec_tfidf = tfidf[corpus[i]]
        sims = index[new_vec_tfidf]
        for j in range(i + 1, len(enterprise_name)):
            if sims[j] > 0.3:
                writer.writerow([enterprise_name['ZZCMC'][i], enterprise_name['SCDZ'][i], enterprise_name['GGSXRQ'][i],
                                 enterprise_name['ZZCMC'][j], enterprise_name['SCDZ'][j], enterprise_name['GGSXRQ'][j],
                                 sims[j]])


def write_data_main():
    """主函数"""
    corpus, tfidf, index, enterprise_name = text_similarity_analysis()
    with open('../data/生成数据/01企业名称修改表/01《整车数据》中企业名称修改表(第一版).csv', 'w', encoding='utf-8-sig', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['ZZCMC名称A', '生产地址A', '公告生效日期A', 'ZZCMC名称B', '生产地址B', '公告生效日期B', '企业名称相似度'])
        write_data(corpus, tfidf, index, enterprise_name, writer)


def address_analysis():
    """地址相似度分析"""
    modify_table = pd.read_csv(filepath_or_buffer='../data/生成数据/01企业名称修改表/01《整车数据》中企业名称修改表(第一版).csv',
                               encoding='utf-8-sig',
                               engine='python')

    address = modify_table[(modify_table['生产地址A'].notnull()) & (modify_table['生产地址B'].notnull())]
    address['地址相似度分析'] = [Levenshtein.ratio(address['生产地址A'].iloc[i], address['生产地址B'].iloc[i])
                          for i in range(len(address))]
    modify_table['地址相似度分析'] = None
    modify_table['地址相似度分析'].update(address['地址相似度分析'])

    modify_table.to_excel(excel_writer='../data/生成数据/01企业名称修改表/02《整车数据》中企业名称修改表(第二版).xlsx',
                          index=False)


def finalized_version():
    """修改表定稿版"""
    modify_table = pd.read_excel(io='../data/生成数据/01企业名称修改表/03《整车数据》中企业名称修改表(第三版).xlsx',
                                 converters={'统一后ZZCMC企业名称': lambda x: x.strip()})
    modify_table = modify_table[modify_table['统一后ZZCMC企业名称'] != '否'].reset_index(drop=True)
    modify_table = modify_table.drop_duplicates(subset=['ZZCMC名称A'], keep='last').reset_index(drop=True)
    del modify_table['标记']
    modify_table.to_excel(excel_writer='../data/生成数据/01企业名称修改表/04《整车数据》中企业名称修改表(定稿版).xlsx',
                          index=False)


def cleaning_vehicle():
    """清洗整车数据"""
    vehicle = pd.read_csv(filepath_or_buffer='../data/原始数据/01汽车新产品/整车数据.csv',
                          engine='python',
                          converters={'ZZCMC': lambda x: x.strip()})

    modify_table = pd.read_excel(io='../data/生成数据/01企业名称修改表/04《整车数据》中企业名称修改表(定稿版).xlsx',
                                 usecols=[0, 3],
                                 converters={'ZZCMC名称A': lambda x: x.strip(),
                                             'ZZCMC名称B': lambda x: x.strip()})
    company_mapping = {modify_table['ZZCMC名称A'].iloc[i]: modify_table['ZZCMC名称B'].iloc[i]
                       for i in range(len(modify_table))}

    vehicle['ZZCMC'].update(vehicle['ZZCMC'].map(company_mapping))
    vehicle.to_excel(excel_writer='../data/生成数据/02整车数据/01整车数据(第一步修改).xlsx',
                     index=False)


if __name__ == "__main__":
    # 运行时间1分钟
    start_time = datetime.datetime.now()
    cleaning_vehicle()
    end_time = datetime.datetime.now()
    run_time = end_time - start_time
    print('程序开始时间：', start_time, '\n程序结束时间：', end_time, '\n程序运行时间：', run_time)
