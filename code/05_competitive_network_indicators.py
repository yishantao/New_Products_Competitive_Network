# -*- coding:utf-8 -*-
"""第五步：计算3/5年窗口期的汽车新产品竞争网络的指标
   第六步：将第五步计算的指标添加到汽车产业面板数据中"""

import os
import datetime
import pandas as pd
import networkx as nx

from multiprocessing import Pool


def create_diagrams(file_path):
    """创建关系图"""
    patent_matrix = pd.read_csv(filepath_or_buffer=file_path,
                                engine='python', index_col=0)
    columns = list(patent_matrix.columns)
    values = patent_matrix.values
    length = len(values)
    graph = nx.Graph()
    graph.add_nodes_from(columns)
    for i in range(length):
        for j in range(length):
            if values[i][j] != 0:
                graph.add_weighted_edges_from([(columns[i], columns[j], values[i][j])])
    return graph


def basic_information(graph, nodes, year):
    betweenness_centrality = nx.betweenness_centrality(graph, weight='weight')
    local_reaching_centrality = [nx.local_reaching_centrality(graph, node, weight='weight') for node in nodes]
    triangles = nx.triangles(graph)
    clustering = nx.clustering(graph, weight='weight')
    weight_dict = {item[0]: item[1] for item in nx.degree(graph, weight='weight')}
    degree_dict = {item[0]: item[1] for item in nx.degree(graph)}
    average_weight_dict = {
        weight_key: (weight_dict[weight_key] / degree_dict[weight_key] if degree_dict[weight_key] != 0 else 0)
        for weight_key in weight_dict.keys()}
    data = pd.DataFrame({'nodes': nodes,
                         'betweenness_centrality': [betweenness_centrality[node] for node in nodes],
                         'local_reaching_centrality': local_reaching_centrality,
                         'triangles': [triangles[node] for node in nodes],
                         'clustering': [clustering[node] for node in nodes],
                         'tie_strength': [average_weight_dict[node] for node in nodes]})
    print(str(year) + '年' + 'basic_information' + '计算完毕！')
    return data


def constraint(graph, nodes, year):
    constraint = nx.constraint(graph, weight='weight')
    data = pd.DataFrame({'nodes': nodes,
                         'constraint': [constraint[node] for node in nodes]})
    print(str(year) + '年' + 'constraint' + '计算完毕！')
    return data


def effective_size(graph, nodes, year):
    effective_size = nx.effective_size(graph, weight='weight')
    data = pd.DataFrame({'nodes': nodes,
                         'effective_size': [effective_size[node] for node in nodes]})
    print(str(year) + '年' + 'effective_size' + '计算完毕！')
    return data


def calculate_networks_indicators(graph, year):
    """计算基本网络指标"""
    degree_centrality = nx.degree_centrality(graph)
    nodes = list(degree_centrality.keys())
    network_indicators = pd.DataFrame({'nodes': nodes,
                                       'degree_centrality': [degree_centrality[node] for node in nodes]})
    p = Pool(4)
    results = []
    results.append(p.apply_async(basic_information, (graph, nodes, year)))
    results.append(p.apply_async(constraint, (graph, nodes, year)))
    results.append(p.apply_async(effective_size, (graph, nodes, year)))
    p.close()
    p.join()

    for i in results:
        network_indicators = pd.merge(left=network_indicators, right=i.get(), on=['nodes'], how='left')
    network_indicators = network_indicators[
        ['nodes', 'degree_centrality', 'betweenness_centrality', 'local_reaching_centrality', 'constraint',
         'effective_size', 'triangles', 'clustering', 'tie_strength']]
    return network_indicators


def network_three_function(year):
    """具体函数"""
    file_path = '../data/生成数据/03关系矩阵/三年期/' + str(year) + '-' + str(year + 2) + '年竞争关系矩阵.csv'
    if os.path.exists(file_path):
        graph = create_diagrams(file_path)
        if not nx.is_empty(graph):
            network_indicators = calculate_networks_indicators(graph, year)
            excel_path = '../data/生成数据/04关系矩阵_网络指标/三年期/' + file_path[-20:-4]
            folder = os.path.exists(excel_path)
            if not folder:
                os.makedirs(excel_path)
            network_indicators.to_excel(excel_writer=excel_path + '/相关指标.xlsx',
                                        index=False)

            address = pd.read_excel(io='../data/生成数据/01企业名称修改表/09整车数据ZZCMCZ编码表.xlsx',
                                    usecols=[0, 1],
                                    converters={'ZZCMC编号': str,
                                                'ZZCMC企业名称': lambda x: x.strip()})
            address = address.rename(columns={'ZZCMC编号': 'nodes', 'ZZCMC企业名称': 'institution_name'})
            network_indicators_address = pd.merge(left=network_indicators, right=address, how='left', on=['nodes'])
            network_indicators_address['year'] = year + 2
            network_indicators_address.to_excel(excel_writer=excel_path + '/相关指标(企业名称).xlsx',
                                                index=False)


def network_three_indicators():
    """计算3年窗口期的汽车新产品竞争网络的网络指标"""
    network_three_function(1985)


def network_five_function(year):
    """具体函数"""
    file_path = '../data/生成数据/03关系矩阵/五年期/' + str(year) + '-' + str(year + 4) + '年竞争关系矩阵.csv'
    if os.path.exists(file_path):
        graph = create_diagrams(file_path)
        if not nx.is_empty(graph):
            network_indicators = calculate_networks_indicators(graph, year)
            excel_path = '../data/生成数据/04关系矩阵_网络指标/五年期/' + file_path[-20:-4]
            folder = os.path.exists(excel_path)
            if not folder:
                os.makedirs(excel_path)
            network_indicators.to_excel(excel_writer=excel_path + '/相关指标.xlsx',
                                        index=False)

            address = pd.read_excel(io='../data/生成数据/01企业名称修改表/09整车数据ZZCMCZ编码表.xlsx',
                                    usecols=[0, 1],
                                    converters={'ZZCMC编号': str,
                                                'ZZCMC企业名称': lambda x: x.strip()})
            address = address.rename(columns={'ZZCMC编号': 'nodes', 'ZZCMC企业名称': 'institution_name'})
            network_indicators_address = pd.merge(left=network_indicators, right=address, how='left', on=['nodes'])
            network_indicators_address['year'] = year + 4
            network_indicators_address.to_excel(excel_writer=excel_path + '/相关指标(企业名称).xlsx',
                                                index=False)


def network_five_indicators():
    """计算5年窗口期的汽车新产品竞争网络的网络指标"""
    network_five_function(1985)


def construct_panel_data():
    """构建汽车产业面板数据"""
    auto_panel = pd.read_excel(io='../data/原始数据/02汽车面板数据以及说明/AmIndustryWTOPanel20181128.xlsx',
                               converters={'YEAR': int})

    # 三年期网络指标
    network_indicators_list = []
    for i in range(1985, 2014):
        file_path = '../data/生成数据/04关系矩阵_网络指标/三年期/' + str(i) + '-' + str(i + 2) + '年竞争关系矩阵/相关指标(企业名称).xlsx'
        network_indicators = pd.read_excel(io=file_path)
        network_indicators = network_indicators.rename(columns={'institution_name': 'OGNM', 'year': 'YEAR'})
        del network_indicators['nodes']
        network_indicators_list.append(network_indicators)

    total_network_indicators = network_indicators_list[0]
    for i in range(1, len(network_indicators_list)):
        total_network_indicators = pd.concat(objs=[total_network_indicators, network_indicators_list[i]],
                                             ignore_index=True)
    total_network_indicators = total_network_indicators.rename(columns={'degree_centrality': 'cpntdgr3yw',
                                                                        'betweenness_centrality': 'cpntbtw3yw',
                                                                        'local_reaching_centrality': 'cpntlcrch3yw',
                                                                        'constraint': 'cpntcstr3yw',
                                                                        'effective_size': 'cpnteffsz3yw',
                                                                        'triangles': 'cpnttrgl3yw',
                                                                        'clustering': 'cpntclust3yw',
                                                                        'tie_strength': 'cpnttiestr3yw'})
    total_network_indicators = total_network_indicators[
        ['OGNM', 'YEAR', 'cpntdgr3yw', 'cpntbtw3yw', 'cpntlcrch3yw', 'cpntcstr3yw', 'cpnteffsz3yw', 'cpnttrgl3yw',
         'cpntclust3yw', 'cpnttiestr3yw']]
    auto_panel = pd.merge(left=auto_panel, right=total_network_indicators, on=['OGNM', 'YEAR'], how='left')

    # 五年期网络指标
    network_indicators_list = []
    for i in range(1985, 2014):
        file_path = '../data/生成数据/04关系矩阵_网络指标/五年期/' + str(i) + '-' + str(i + 4) + '年竞争关系矩阵/相关指标(企业名称).xlsx'
        network_indicators = pd.read_excel(io=file_path)
        network_indicators = network_indicators.rename(columns={'institution_name': 'OGNM', 'year': 'YEAR'})
        del network_indicators['nodes']
        network_indicators_list.append(network_indicators)

    total_network_indicators = network_indicators_list[0]
    for i in range(1, len(network_indicators_list)):
        total_network_indicators = pd.concat(objs=[total_network_indicators, network_indicators_list[i]],
                                             ignore_index=True)
    total_network_indicators = total_network_indicators.rename(columns={'degree_centrality': 'cpntdgr5yw',
                                                                        'betweenness_centrality': 'cpntbtw5yw',
                                                                        'local_reaching_centrality': 'cpntlcrch5yw',
                                                                        'constraint': 'cpntcstr5yw',
                                                                        'effective_size': 'cpnteffsz5yw',
                                                                        'triangles': 'cpnttrgl5yw',
                                                                        'clustering': 'cpntclust5yw',
                                                                        'tie_strength': 'cpnttiestr5yw'})
    total_network_indicators = total_network_indicators[
        ['OGNM', 'YEAR', 'cpntdgr5yw', 'cpntbtw5yw', 'cpntlcrch5yw', 'cpntcstr5yw', 'cpnteffsz5yw', 'cpnttrgl5yw',
         'cpntclust5yw', 'cpnttiestr5yw']]
    auto_panel = pd.merge(left=auto_panel, right=total_network_indicators, on=['OGNM', 'YEAR'], how='left')
    auto_panel.to_excel(excel_writer='../data/生成数据/05面板数据/汽车产业面板数据(第1版).xlsx',
                        index=False)


if __name__ == "__main__":
    # 运行时间1分钟
    start_time = datetime.datetime.now()
    network_three_indicators()
    network_five_indicators()
    construct_panel_data()
    end_time = datetime.datetime.now()
    run_time = end_time - start_time
    print('程序开始时间：', start_time, '\n程序结束时间：', end_time, '\n程序运行时间：', run_time)
