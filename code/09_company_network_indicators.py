# -*- coding:utf-8 -*-
"""第八、九、十步：计算IPCMG、IPCSG矩阵的网络指标"""

import os
import datetime
import pandas as pd
import networkx as nx


def create_diagrams(file_path):
    """创建关系图"""
    patent_matrix = pd.read_csv(filepath_or_buffer=file_path,
                                engine='python', index_col=0)
    columns = list(patent_matrix.columns)
    values = patent_matrix.values
    length = len(values)
    graph = nx.Graph()
    graph.add_nodes_from(columns)
    if length > 1:
        for i in range(length):
            for j in range(length):
                if values[i][j] != 0:
                    graph.add_weighted_edges_from([(columns[i], columns[j], values[i][j])])
    return graph


def calculate_networks_indicators(graph):
    """计算基本网络指标"""
    degree_centrality = nx.degree_centrality(graph)
    nodes = list(degree_centrality.keys())
    betweenness_centrality = nx.betweenness_centrality(graph, weight='weight')
    network_indicators = pd.DataFrame({'nodes': nodes,
                                       'degree_centrality': [degree_centrality[node] for node in nodes],
                                       'betweenness_centrality': [betweenness_centrality[node] for node in nodes]})

    network_indicators['local_reaching_centrality'] = [nx.local_reaching_centrality(graph, node, weight='weight') for
                                                       node in nodes]
    constraint = nx.constraint(graph, weight='weight')
    network_indicators['constraint'] = [constraint[node] for node in nodes]
    effective_size = nx.effective_size(graph, weight='weight')
    network_indicators['effective_size'] = [effective_size[node] for node in nodes]
    triangles = nx.triangles(graph)
    network_indicators['triangles'] = [triangles[node] for node in nodes]
    clustering = nx.clustering(graph, weight='weight')
    network_indicators['clustering'] = [clustering[node] for node in nodes]

    weight_dict = {item[0]: item[1] for item in nx.degree(graph, weight='weight')}
    degree_dict = {item[0]: item[1] for item in nx.degree(graph)}
    average_weight_dict = {
        weight_key: (weight_dict[weight_key] / degree_dict[weight_key] if degree_dict[weight_key] != 0 else 0)
        for weight_key in weight_dict.keys()}
    network_indicators['tie_strength'] = [average_weight_dict[node] for node in nodes]
    network_indicators['number_of_node'] = nx.number_of_nodes(graph)
    network_indicators['density'] = nx.density(graph)
    cliques = nx.graph_clique_number(graph)
    if cliques >= 3:
        network_indicators['cliques'] = cliques
    else:
        network_indicators['cliques'] = 0
    network_indicators['efficiency'] = nx.global_efficiency(graph)
    network_indicators['isolates'] = nx.number_of_isolates(graph)

    network_indicators = network_indicators[
        ['nodes', 'degree_centrality', 'betweenness_centrality', 'local_reaching_centrality', 'constraint',
         'effective_size', 'triangles', 'clustering', 'tie_strength', 'number_of_node', 'density', 'cliques',
         'efficiency', 'isolates']]
    return network_indicators


def calculate_enterprise_indicators_three_year_ipcmg():
    """计算企业网络指标(三年期IPCMG)"""
    institutions = pd.read_excel(io='../data/生成数据/06专利数据/汽车产业(企业名称).xlsx')

    description_columns = [str(i) + '-' + str(i + 2) for i in range(1985, 2013)]
    description_information = pd.DataFrame(columns=description_columns, index=list(institutions['企业名称']))

    statistical_columns = ['企业名称', '年份', 'ipcmgdgr3yw', 'ipcmgbtw3yw', 'ipcmglcrch3yw', 'ipcmgcnstr3yw',
                           'ipcmgeffsz3yw', 'ipcmgtrgl3yw', 'ipcmgclust3yw', 'ipcmgtirstr3yw', 'ipcmgntsz3yw',
                           'ipcmgntdnst3yw', 'ipcmgcliq3yw', 'ipcmgeffc3yw', 'ipcmgisolt3yw']
    statistical_information = pd.DataFrame(columns=statistical_columns)
    index_symbol = 0

    for i in range(len(institutions)):
        each_institutions = institutions['企业名称'].iloc[i]
        for j in range(1985, 2013):
            years_interval = str(j) + '-' + str(j + 2)
            file_path = '../data/生成数据/07IPCMGSG网络/三年期IPCMG/' + each_institutions + '/' + years_interval + '年IPCMG矩阵.csv'
            if os.path.exists(file_path):
                graph = create_diagrams(file_path)
                if not nx.is_empty(graph):
                    description_information.loc[each_institutions, years_interval] = 1
                    network_indicators = calculate_networks_indicators(graph)
                    excel_path = '../data/生成数据/07IPCMGSG网络/三年期IPCMG(网络指标)/' + each_institutions + '/' + years_interval + '年IPCMG矩阵'
                    folder = os.path.exists(excel_path)
                    if not folder:
                        os.makedirs(excel_path)
                    network_indicators.to_excel(excel_writer=excel_path + '/相关指标.xlsx',
                                                index=False)
                    del network_indicators['nodes']
                    network_indicators_average = network_indicators.mean()
                    row_information = [each_institutions, j + 2,
                                       network_indicators_average['degree_centrality'],
                                       network_indicators_average['betweenness_centrality'],
                                       network_indicators_average['local_reaching_centrality'],
                                       network_indicators_average['constraint'],
                                       network_indicators_average['effective_size'],
                                       network_indicators_average['triangles'],
                                       network_indicators_average['clustering'],
                                       network_indicators_average['tie_strength'],
                                       network_indicators_average['number_of_node'],
                                       network_indicators_average['density'],
                                       network_indicators_average['cliques'],
                                       network_indicators_average['efficiency'],
                                       network_indicators_average['isolates']]
                    statistical_information.loc[index_symbol] = row_information
                    index_symbol += 1
                else:
                    description_information.loc[each_institutions, years_interval] = 0
            else:
                description_information.loc[each_institutions, years_interval] = None

    description_information = description_information.reset_index(drop=False)
    description_information.to_excel(excel_writer='../data/生成数据/07IPCMGSG网络/统计信息/01三年期IPCMG矩阵描述信息.xlsx',
                                     index=False)
    statistical_information.to_excel(excel_writer='../data/生成数据/07IPCMGSG网络/统计信息/02三年期IPCMG网络指标信息.xlsx',
                                     index=False)


def calculate_enterprise_indicators_five_year_ipcmg():
    """计算企业网络指标(五年期IPCMG)"""
    institutions = pd.read_excel(io='../data/生成数据/06专利数据/汽车产业(企业名称).xlsx')

    description_columns = [str(i) + '-' + str(i + 4) for i in range(1985, 2011)]
    description_information = pd.DataFrame(columns=description_columns, index=list(institutions['企业名称']))

    statistical_columns = ['企业名称', '年份', 'ipcmgdgr5yw', 'ipcmgbtw5yw', 'ipcmglcrch5yw', 'ipcmgcnstr5yw',
                           'ipcmgeffsz5yw', 'ipcmgtrgl5yw', 'ipcmgclust5yw', 'ipcmgtirstr5yw', 'ipcmgntsz5yw',
                           'ipcmgntdnst5yw', 'ipcmgcliq5yw', 'ipcmgeffc5yw', 'ipcmgisolt5yw']
    statistical_information = pd.DataFrame(columns=statistical_columns)
    index_symbol = 0

    for i in range(len(institutions)):
        each_institutions = institutions['企业名称'].iloc[i]
        for j in range(1985, 2011):
            years_interval = str(j) + '-' + str(j + 4)
            file_path = '../data/生成数据/07IPCMGSG网络/五年期IPCMG/' + each_institutions + '/' + years_interval + '年IPCMG矩阵.csv'
            if os.path.exists(file_path):
                graph = create_diagrams(file_path)
                if not nx.is_empty(graph):
                    description_information.loc[each_institutions, years_interval] = 1
                    network_indicators = calculate_networks_indicators(graph)
                    excel_path = '../data/生成数据/07IPCMGSG网络/五年期IPCMG(网络指标)/' + each_institutions + '/' + years_interval + '年IPCMG矩阵'
                    folder = os.path.exists(excel_path)
                    if not folder:
                        os.makedirs(excel_path)
                    network_indicators.to_excel(excel_writer=excel_path + '/相关指标.xlsx',
                                                index=False)
                    del network_indicators['nodes']
                    network_indicators_average = network_indicators.mean()
                    row_information = [each_institutions, j + 4,
                                       network_indicators_average['degree_centrality'],
                                       network_indicators_average['betweenness_centrality'],
                                       network_indicators_average['local_reaching_centrality'],
                                       network_indicators_average['constraint'],
                                       network_indicators_average['effective_size'],
                                       network_indicators_average['triangles'],
                                       network_indicators_average['clustering'],
                                       network_indicators_average['tie_strength'],
                                       network_indicators_average['number_of_node'],
                                       network_indicators_average['density'],
                                       network_indicators_average['cliques'],
                                       network_indicators_average['efficiency'],
                                       network_indicators_average['isolates']]
                    statistical_information.loc[index_symbol] = row_information
                    index_symbol += 1
                else:
                    description_information.loc[each_institutions, years_interval] = 0
            else:
                description_information.loc[each_institutions, years_interval] = None

    description_information = description_information.reset_index(drop=False)
    description_information.to_excel(excel_writer='../data/生成数据/07IPCMGSG网络/统计信息/03五年期IPCMG矩阵描述信息.xlsx',
                                     index=False)
    statistical_information.to_excel(excel_writer='../data/生成数据/07IPCMGSG网络/统计信息/04五年期IPCMG网络指标信息.xlsx',
                                     index=False)


def calculate_enterprise_indicators_three_year_ipcsg():
    """计算企业网络指标(三年期IPCSG)"""
    institutions = pd.read_excel(io='../data/生成数据/06专利数据/汽车产业(企业名称).xlsx')

    description_columns = [str(i) + '-' + str(i + 2) for i in range(1985, 2013)]
    description_information = pd.DataFrame(columns=description_columns, index=list(institutions['企业名称']))

    statistical_columns = ['企业名称', '年份', 'ipcsgdgr3yw', 'ipcsgbtw3yw', 'ipcsglcrch3yw', 'ipcsgcnstr3yw',
                           'ipcsgeffsz3yw', 'ipcsgtrgl3yw', 'ipcsgclust3yw', 'ipcsgtirstr3yw', 'ipcsgntsz3yw',
                           'ipcsgntdnst3yw', 'ipcsgcliq3yw', 'ipcsgeffc3yw', 'ipcsgisolt3yw']
    statistical_information = pd.DataFrame(columns=statistical_columns)
    index_symbol = 0

    for i in range(len(institutions)):
        each_institutions = institutions['企业名称'].iloc[i]
        for j in range(1985, 2013):
            years_interval = str(j) + '-' + str(j + 2)
            file_path = '../data/生成数据/07IPCMGSG网络/三年期IPCSG/' + each_institutions + '/' + years_interval + '年IPCSG矩阵.csv'
            if os.path.exists(file_path):
                graph = create_diagrams(file_path)
                if not nx.is_empty(graph):
                    description_information.loc[each_institutions, years_interval] = 1
                    network_indicators = calculate_networks_indicators(graph)
                    excel_path = '../data/生成数据/07IPCMGSG网络/三年期IPCSG(网络指标)/' + each_institutions + '/' + years_interval + '年IPCSG矩阵'
                    folder = os.path.exists(excel_path)
                    if not folder:
                        os.makedirs(excel_path)
                    network_indicators.to_excel(excel_writer=excel_path + '/相关指标.xlsx',
                                                index=False)
                    del network_indicators['nodes']
                    network_indicators_average = network_indicators.mean()
                    row_information = [each_institutions, j + 2,
                                       network_indicators_average['degree_centrality'],
                                       network_indicators_average['betweenness_centrality'],
                                       network_indicators_average['local_reaching_centrality'],
                                       network_indicators_average['constraint'],
                                       network_indicators_average['effective_size'],
                                       network_indicators_average['triangles'],
                                       network_indicators_average['clustering'],
                                       network_indicators_average['tie_strength'],
                                       network_indicators_average['number_of_node'],
                                       network_indicators_average['density'],
                                       network_indicators_average['cliques'],
                                       network_indicators_average['efficiency'],
                                       network_indicators_average['isolates']]
                    statistical_information.loc[index_symbol] = row_information
                    index_symbol += 1
                else:
                    description_information.loc[each_institutions, years_interval] = 0
            else:
                description_information.loc[each_institutions, years_interval] = None

    description_information = description_information.reset_index(drop=False)
    description_information.to_excel(excel_writer='../data/生成数据/07IPCMGSG网络/统计信息/05三年期IPCSG矩阵描述信息.xlsx',
                                     index=False)
    statistical_information.to_excel(excel_writer='../data/生成数据/07IPCMGSG网络/统计信息/06三年期IPCSG网络指标信息.xlsx',
                                     index=False)


def calculate_enterprise_indicators_five_year_ipcsg():
    """计算企业网络指标(五年期IPCSG)"""
    institutions = pd.read_excel(io='../data/生成数据/06专利数据/汽车产业(企业名称).xlsx')

    description_columns = [str(i) + '-' + str(i + 4) for i in range(1985, 2011)]
    description_information = pd.DataFrame(columns=description_columns, index=list(institutions['企业名称']))

    statistical_columns = ['企业名称', '年份', 'ipcsgdgr5yw', 'ipcsgbtw5yw', 'ipcsglcrch5yw', 'ipcsgcnstr5yw',
                           'ipcsgeffsz5yw', 'ipcsgtrgl5yw', 'ipcsgclust5yw', 'ipcsgtirstr5yw', 'ipcsgntsz5yw',
                           'ipcsgntdnst5yw', 'ipcsgcliq5yw', 'ipcsgeffc5yw', 'ipcsgisolt5yw']
    statistical_information = pd.DataFrame(columns=statistical_columns)
    index_symbol = 0

    for i in range(len(institutions)):
        each_institutions = institutions['企业名称'].iloc[i]
        for j in range(1985, 2011):
            years_interval = str(j) + '-' + str(j + 4)
            file_path = '../data/生成数据/07IPCMGSG网络/五年期IPCSG/' + each_institutions + '/' + years_interval + '年IPCSG矩阵.csv'
            if os.path.exists(file_path):
                graph = create_diagrams(file_path)
                if not nx.is_empty(graph):
                    description_information.loc[each_institutions, years_interval] = 1
                    network_indicators = calculate_networks_indicators(graph)
                    excel_path = '../data/生成数据/07IPCMGSG网络/五年期IPCSG(网络指标)/' + each_institutions + '/' + years_interval + '年IPCSG矩阵'
                    folder = os.path.exists(excel_path)
                    if not folder:
                        os.makedirs(excel_path)
                    network_indicators.to_excel(excel_writer=excel_path + '/相关指标.xlsx',
                                                index=False)
                    del network_indicators['nodes']
                    network_indicators_average = network_indicators.mean()
                    row_information = [each_institutions, j + 4,
                                       network_indicators_average['degree_centrality'],
                                       network_indicators_average['betweenness_centrality'],
                                       network_indicators_average['local_reaching_centrality'],
                                       network_indicators_average['constraint'],
                                       network_indicators_average['effective_size'],
                                       network_indicators_average['triangles'],
                                       network_indicators_average['clustering'],
                                       network_indicators_average['tie_strength'],
                                       network_indicators_average['number_of_node'],
                                       network_indicators_average['density'],
                                       network_indicators_average['cliques'],
                                       network_indicators_average['efficiency'],
                                       network_indicators_average['isolates']]
                    statistical_information.loc[index_symbol] = row_information
                    index_symbol += 1
                else:
                    description_information.loc[each_institutions, years_interval] = 0
            else:
                description_information.loc[each_institutions, years_interval] = None

    description_information = description_information.reset_index(drop=False)
    description_information.to_excel(excel_writer='../data/生成数据/07IPCMGSG网络/统计信息/07五年期IPCSG矩阵描述信息.xlsx',
                                     index=False)
    statistical_information.to_excel(excel_writer='../data/生成数据/07IPCMGSG网络/统计信息/08五年期IPCSG网络指标信息.xlsx',
                                     index=False)


def construct_panel_data():
    """向汽车产业面板数据添加三年期/五年期网络指标"""
    auto_panel = pd.read_excel(io='../data/生成数据/05面板数据/汽车产业面板数据(第1版).xlsx',
                               converters={'YEAR': int})

    ipcmg_three_networks = pd.read_excel(io='../data/生成数据/07IPCMGSG网络/统计信息/02三年期IPCMG网络指标信息.xlsx',
                                         converters={'企业名称': lambda x: x.strip(),
                                                     '年份': int}).rename(
        columns={'企业名称': 'OGNM', '年份': 'YEAR'})
    ipcmg_five_networks = pd.read_excel(io='../data/生成数据/07IPCMGSG网络/统计信息/04五年期IPCMG网络指标信息.xlsx',
                                        converters={'企业名称': lambda x: x.strip(),
                                                    '年份': int}).rename(
        columns={'企业名称': 'OGNM', '年份': 'YEAR'})
    ipcsg_three_networks = pd.read_excel(io='../data/生成数据/07IPCMGSG网络/统计信息/06三年期IPCSG网络指标信息.xlsx',
                                         converters={'企业名称': lambda x: x.strip(),
                                                     '年份': int}).rename(
        columns={'企业名称': 'OGNM', '年份': 'YEAR'})
    ipcsg_five_networks = pd.read_excel(io='../data/生成数据/07IPCMGSG网络/统计信息/08五年期IPCSG网络指标信息.xlsx',
                                        converters={'企业名称': lambda x: x.strip(),
                                                    '年份': int}).rename(
        columns={'企业名称': 'OGNM', '年份': 'YEAR'})
    auto_panel = pd.merge(left=auto_panel, right=ipcmg_three_networks, how='left', on=['OGNM', 'YEAR'])
    auto_panel = pd.merge(left=auto_panel, right=ipcmg_five_networks, how='left', on=['OGNM', 'YEAR'])
    auto_panel = pd.merge(left=auto_panel, right=ipcsg_three_networks, how='left', on=['OGNM', 'YEAR'])
    auto_panel = pd.merge(left=auto_panel, right=ipcsg_five_networks, how='left', on=['OGNM', 'YEAR'])
    auto_panel.to_excel(excel_writer='../data/生成数据/05面板数据/汽车产业面板数据(第2版).xlsx',
                        index=False)


if __name__ == "__main__":
    # 运行时间2.5小时
    start_time = datetime.datetime.now()
    calculate_enterprise_indicators_three_year_ipcmg()
    calculate_enterprise_indicators_five_year_ipcmg()
    calculate_enterprise_indicators_three_year_ipcsg()
    calculate_enterprise_indicators_five_year_ipcsg()
    # 下一步之前要将 成都汽车配件总厂郫县湓斐 的名字改写成 成都汽车配件总厂郫县湓斐?
    construct_panel_data()
    end_time = datetime.datetime.now()
    run_time = end_time - start_time
    print('程序开始时间：', start_time, '\n程序结束时间：', end_time, '\n程序运行时间：', run_time)
