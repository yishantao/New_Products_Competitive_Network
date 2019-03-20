# -*- coding:utf-8 -*-
"""第十一、十二、十三、十四步"""

import datetime
import pandas as pd


def eleven():
    """竞争对手三年期IPCMG的相关指标"""
    statistical_information_three = pd.DataFrame(
        columns=['ZZCMC编号', '年份', 'cptoripcmgdgr3yw', 'cptoripcmgbtw3yw', 'cptoripcmglcrch3yw', 'cptoripcmgcnstr3yw',
                 'cptoripcmgeffsz3yw', 'cptoripcmgtrgl3yw', 'cptoripcmgclust3yw', 'cptoripcmgntsz3yw',
                 'cptoripcmgntdnst3yw', 'cptoripcmgcliq3yw', 'cptoripcmgeffc3yw', 'cptoripcmgisolt3yw'])
    index_three = 0

    ipc_data = pd.read_excel(io='../data/生成数据/07IPCMGSG网络/统计信息/02三年期IPCMG网络指标信息.xlsx')

    vehicle_data = pd.read_excel(io='../data/生成数据/02整车数据/03整车数据(企业名称修改为ZZCMC编号).xlsx',
                                 converters={'ZZCMC': str,
                                             'CLLX': str},
                                 parse_dates=['CXSXRQ', 'GGSXRQ'])
    vehicle_data['CXSXRQ'] = vehicle_data['CXSXRQ'].fillna(pd.to_datetime('2017-12-31'))

    institutions = pd.read_excel(io='../data/生成数据/06专利数据/汽车产业(企业名称).xlsx')
    address = pd.read_excel(io='../data/生成数据/01企业名称修改表/09整车数据ZZCMCZ编码表.xlsx',
                            usecols=[0, 1],
                            converters={'ZZCMC编号': str,
                                        'ZZCMC企业名称': lambda x: x.strip()})
    address = address.rename(columns={'ZZCMC企业名称': '企业名称'})
    institutions = pd.merge(left=institutions, right=address, how='left', on=['企业名称'])
    institutions = institutions[institutions['ZZCMC编号'].notnull()].reset_index(drop=True)
    ipc_data = pd.merge(left=ipc_data, right=address, how='left', on=['企业名称'])

    for year in range(1985, 2013):
        filter_data = vehicle_data[((vehicle_data['GGSXRQ'] <= pd.to_datetime(str(year))) & (
                pd.to_datetime(str(year)) <= vehicle_data['CXSXRQ'])) | (
                                           (vehicle_data['GGSXRQ'] <= pd.to_datetime(str(year + 2) + '-12-31')) & (
                                           pd.to_datetime(str(year + 2) + '-12-31') <= vehicle_data['CXSXRQ']))]
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
                    original_car_type = company_car_type_counts[institution]
                    competitors = []
                    for i in range(0, len(company_index)):
                        if company_index[i] != institution:
                            compare_car_type = company_car_type_counts[company_index[i]]
                            company_key = original_car_type.keys() & compare_car_type.keys()
                            if company_key:
                                competitors.append(company_index[i])
                    if len(competitors) > 0:
                        part_information = ipc_data[
                            (ipc_data['ZZCMC编号'].apply(lambda x: True if x in competitors else False)) & (
                                    ipc_data['年份'] == year + 2)]
                        if not part_information.empty:
                            part_information.drop(['企业名称', 'ZZCMC编号', '年份'], axis=1, inplace=True)
                            part_information = part_information.mean()
                            statistical_information_three.loc[index_three] = [institution, year + 2,
                                                                              part_information['ipcmgdgr3yw'],
                                                                              part_information['ipcmgbtw3yw'],
                                                                              part_information['ipcmglcrch3yw'],
                                                                              part_information['ipcmgcnstr3yw'],
                                                                              part_information['ipcmgeffsz3yw'],
                                                                              part_information['ipcmgtrgl3yw'],
                                                                              part_information['ipcmgclust3yw'],
                                                                              part_information['ipcmgntsz3yw'],
                                                                              part_information['ipcmgntdnst3yw'],
                                                                              part_information['ipcmgcliq3yw'],
                                                                              part_information['ipcmgeffc3yw'],
                                                                              part_information['ipcmgisolt3yw']]
                            index_three += 1
    statistical_information_three = pd.merge(left=address, right=statistical_information_three, on=['ZZCMC编号'],
                                             how='right')
    statistical_information_three.to_excel(excel_writer='../data/生成数据/08竞争对手/01三年期IPCMG指标.xlsx',
                                           index=False)


def twelve():
    """竞争对手五年期IPCMG的相关指标"""
    statistical_information_five = pd.DataFrame(
        columns=['ZZCMC编号', '年份', 'cptoripcmgdgr5yw ', 'cptoripcmgbtw5yw', 'cptoripcmglcrch5yw', 'cptoripcmgcnstr5yw',
                 'cptoripcmgeffsz5yw', 'cptoripcmgtrgl5yw', 'cptoripcmgclust5yw', 'cptoripcmgntsz5yw',
                 'cptoripcmgntdnst5yw', 'cptoripcmgcliq5yw', 'cptoripcmgeffc5yw', 'cptoripcmgisolt5yw'])
    index_five = 0

    ipc_data = pd.read_excel(io='../data/生成数据/07IPCMGSG网络/统计信息/04五年期IPCMG网络指标信息.xlsx')

    vehicle_data = pd.read_excel(io='../data/生成数据/02整车数据/03整车数据(企业名称修改为ZZCMC编号).xlsx',
                                 converters={'ZZCMC': str,
                                             'CLLX': str},
                                 parse_dates=['CXSXRQ', 'GGSXRQ'])
    vehicle_data['CXSXRQ'] = vehicle_data['CXSXRQ'].fillna(pd.to_datetime('2017-12-31'))

    institutions = pd.read_excel(io='../data/生成数据/06专利数据/汽车产业(企业名称).xlsx')
    address = pd.read_excel(io='../data/生成数据/01企业名称修改表/09整车数据ZZCMCZ编码表.xlsx',
                            usecols=[0, 1],
                            converters={'ZZCMC编号': str,
                                        'ZZCMC企业名称': lambda x: x.strip()})
    address = address.rename(columns={'ZZCMC企业名称': '企业名称'})
    institutions = pd.merge(left=institutions, right=address, how='left', on=['企业名称'])
    institutions = institutions[institutions['ZZCMC编号'].notnull()].reset_index(drop=True)
    ipc_data = pd.merge(left=ipc_data, right=address, how='left', on=['企业名称'])

    for year in range(1985, 2011):
        filter_data = vehicle_data[((vehicle_data['GGSXRQ'] <= pd.to_datetime(str(year))) & (
                pd.to_datetime(str(year)) <= vehicle_data['CXSXRQ'])) | (
                                           (vehicle_data['GGSXRQ'] <= pd.to_datetime(str(year + 4) + '-12-31')) & (
                                           pd.to_datetime(str(year + 4) + '-12-31') <= vehicle_data['CXSXRQ']))]
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
                    original_car_type = company_car_type_counts[institution]
                    competitors = []
                    for i in range(0, len(company_index)):
                        if company_index[i] != institution:
                            compare_car_type = company_car_type_counts[company_index[i]]
                            company_key = original_car_type.keys() & compare_car_type.keys()
                            if company_key:
                                competitors.append(company_index[i])
                    if len(competitors) > 0:
                        part_information = ipc_data[
                            (ipc_data['ZZCMC编号'].apply(lambda x: True if x in competitors else False)) & (
                                    ipc_data['年份'] == year + 4)]
                        if not part_information.empty:
                            part_information.drop(['企业名称', 'ZZCMC编号', '年份'], axis=1, inplace=True)
                            part_information = part_information.mean()
                            statistical_information_five.loc[index_five] = [institution, year + 4,
                                                                            part_information['ipcmgdgr5yw'],
                                                                            part_information['ipcmgbtw5yw'],
                                                                            part_information['ipcmglcrch5yw'],
                                                                            part_information['ipcmgcnstr5yw'],
                                                                            part_information['ipcmgeffsz5yw'],
                                                                            part_information['ipcmgtrgl5yw'],
                                                                            part_information['ipcmgclust5yw'],
                                                                            part_information['ipcmgntsz5yw'],
                                                                            part_information['ipcmgntdnst5yw'],
                                                                            part_information['ipcmgcliq5yw'],
                                                                            part_information['ipcmgeffc5yw'],
                                                                            part_information['ipcmgisolt5yw']]
                            index_five += 1
    statistical_information_five = pd.merge(left=address, right=statistical_information_five, on=['ZZCMC编号'],
                                            how='right')
    statistical_information_five.to_excel(excel_writer='../data/生成数据/08竞争对手/02五年期IPCMG指标.xlsx',
                                          index=False)


def thirteen():
    """竞争对手三年期IPCSG的相关指标"""
    statistical_information_three = pd.DataFrame(
        columns=['ZZCMC编号', '年份', 'cptoripcsgdgr3yw', 'cptoripcsgbtw3yw', 'cptoripcsglcrch3yw', 'cptoripcsgcnstr3yw',
                 'cptoripcsgeffsz3yw', 'cptoripcsgtrgl3yw', 'cptoripcsgclust3yw', 'cptoripcsgntsz3yw',
                 'cptoripcsgntdnst3yw', 'cptoripcsgcliq3yw', 'cptoripcsgeffc3yw', 'cptoripcsgisolt3yw'])
    index_three = 0

    ipc_data = pd.read_excel(io='../data/生成数据/07IPCMGSG网络/统计信息/06三年期IPCSG网络指标信息.xlsx')

    vehicle_data = pd.read_excel(io='../data/生成数据/02整车数据/03整车数据(企业名称修改为ZZCMC编号).xlsx',
                                 converters={'ZZCMC': str,
                                             'CLLX': str},
                                 parse_dates=['CXSXRQ', 'GGSXRQ'])
    vehicle_data['CXSXRQ'] = vehicle_data['CXSXRQ'].fillna(pd.to_datetime('2017-12-31'))

    institutions = pd.read_excel(io='../data/生成数据/06专利数据/汽车产业(企业名称).xlsx')
    address = pd.read_excel(io='../data/生成数据/01企业名称修改表/09整车数据ZZCMCZ编码表.xlsx',
                            usecols=[0, 1],
                            converters={'ZZCMC编号': str,
                                        'ZZCMC企业名称': lambda x: x.strip()})
    address = address.rename(columns={'ZZCMC企业名称': '企业名称'})
    institutions = pd.merge(left=institutions, right=address, how='left', on=['企业名称'])
    institutions = institutions[institutions['ZZCMC编号'].notnull()].reset_index(drop=True)
    ipc_data = pd.merge(left=ipc_data, right=address, how='left', on=['企业名称'])

    for year in range(1985, 2013):
        filter_data = vehicle_data[((vehicle_data['GGSXRQ'] <= pd.to_datetime(str(year))) & (
                pd.to_datetime(str(year)) <= vehicle_data['CXSXRQ'])) | (
                                           (vehicle_data['GGSXRQ'] <= pd.to_datetime(str(year + 2) + '-12-31')) & (
                                           pd.to_datetime(str(year + 2) + '-12-31') <= vehicle_data['CXSXRQ']))]
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
                    original_car_type = company_car_type_counts[institution]
                    competitors = []
                    for i in range(0, len(company_index)):
                        if company_index[i] != institution:
                            compare_car_type = company_car_type_counts[company_index[i]]
                            company_key = original_car_type.keys() & compare_car_type.keys()
                            if company_key:
                                competitors.append(company_index[i])
                    if len(competitors) > 0:
                        part_information = ipc_data[
                            (ipc_data['ZZCMC编号'].apply(lambda x: True if x in competitors else False)) & (
                                    ipc_data['年份'] == year + 2)]
                        if not part_information.empty:
                            part_information.drop(['企业名称', 'ZZCMC编号', '年份'], axis=1, inplace=True)
                            part_information = part_information.mean()
                            statistical_information_three.loc[index_three] = [institution, year + 2,
                                                                              part_information['ipcsgdgr3yw'],
                                                                              part_information['ipcsgbtw3yw'],
                                                                              part_information['ipcsglcrch3yw'],
                                                                              part_information['ipcsgcnstr3yw'],
                                                                              part_information['ipcsgeffsz3yw'],
                                                                              part_information['ipcsgtrgl3yw'],
                                                                              part_information['ipcsgclust3yw'],
                                                                              part_information['ipcsgntsz3yw'],
                                                                              part_information['ipcsgntdnst3yw'],
                                                                              part_information['ipcsgcliq3yw'],
                                                                              part_information['ipcsgeffc3yw'],
                                                                              part_information['ipcsgisolt3yw']]
                            index_three += 1
    statistical_information_three = pd.merge(left=address, right=statistical_information_three, on=['ZZCMC编号'],
                                             how='right')
    statistical_information_three.to_excel(excel_writer='../data/生成数据/08竞争对手/03三年期IPCSG指标.xlsx',
                                           index=False)


def fourteen():
    """竞争对手五年期IPCSG的相关指标"""
    statistical_information_five = pd.DataFrame(
        columns=['ZZCMC编号', '年份', 'cptoripcsgdgr5yw', 'cptoripcsgbtw5yw', 'cptoripcsglcrch5yw', 'cptoripcsgcnstr5yw',
                 'cptoripcsgeffsz5yw', 'cptoripcsgtrgl5yw', 'cptoripcsgclust5yw', 'cptoripcsgntsz5yw',
                 'cptoripcsgntdnst5yw', 'cptoripcsgcliq5yw', 'cptoripcsgeffc5yw', 'cptoripcsgisolt5yw'])
    index_five = 0

    ipc_data = pd.read_excel(io='../data/生成数据/07IPCMGSG网络/统计信息/08五年期IPCSG网络指标信息.xlsx')

    vehicle_data = pd.read_excel(io='../data/生成数据/02整车数据/03整车数据(企业名称修改为ZZCMC编号).xlsx',
                                 converters={'ZZCMC': str,
                                             'CLLX': str},
                                 parse_dates=['CXSXRQ', 'GGSXRQ'])
    vehicle_data['CXSXRQ'] = vehicle_data['CXSXRQ'].fillna(pd.to_datetime('2017-12-31'))

    institutions = pd.read_excel(io='../data/生成数据/06专利数据/汽车产业(企业名称).xlsx')
    address = pd.read_excel(io='../data/生成数据/01企业名称修改表/09整车数据ZZCMCZ编码表.xlsx',
                            usecols=[0, 1],
                            converters={'ZZCMC编号': str,
                                        'ZZCMC企业名称': lambda x: x.strip()})
    address = address.rename(columns={'ZZCMC企业名称': '企业名称'})
    institutions = pd.merge(left=institutions, right=address, how='left', on=['企业名称'])
    institutions = institutions[institutions['ZZCMC编号'].notnull()].reset_index(drop=True)
    ipc_data = pd.merge(left=ipc_data, right=address, how='left', on=['企业名称'])

    for year in range(1985, 2011):
        filter_data = vehicle_data[((vehicle_data['GGSXRQ'] <= pd.to_datetime(str(year))) & (
                pd.to_datetime(str(year)) <= vehicle_data['CXSXRQ'])) | (
                                           (vehicle_data['GGSXRQ'] <= pd.to_datetime(str(year + 4) + '-12-31')) & (
                                           pd.to_datetime(str(year + 4) + '-12-31') <= vehicle_data['CXSXRQ']))]
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
                    original_car_type = company_car_type_counts[institution]
                    competitors = []
                    for i in range(0, len(company_index)):
                        if company_index[i] != institution:
                            compare_car_type = company_car_type_counts[company_index[i]]
                            company_key = original_car_type.keys() & compare_car_type.keys()
                            if company_key:
                                competitors.append(company_index[i])
                    if len(competitors) > 0:
                        part_information = ipc_data[
                            (ipc_data['ZZCMC编号'].apply(lambda x: True if x in competitors else False)) & (
                                    ipc_data['年份'] == year + 4)]
                        if not part_information.empty:
                            part_information.drop(['企业名称', 'ZZCMC编号', '年份'], axis=1, inplace=True)
                            part_information = part_information.mean()
                            statistical_information_five.loc[index_five] = [institution, year + 4,
                                                                            part_information['ipcsgdgr5yw'],
                                                                            part_information['ipcsgbtw5yw'],
                                                                            part_information['ipcsglcrch5yw'],
                                                                            part_information['ipcsgcnstr5yw'],
                                                                            part_information['ipcsgeffsz5yw'],
                                                                            part_information['ipcsgtrgl5yw'],
                                                                            part_information['ipcsgclust5yw'],
                                                                            part_information['ipcsgntsz5yw'],
                                                                            part_information['ipcsgntdnst5yw'],
                                                                            part_information['ipcsgcliq5yw'],
                                                                            part_information['ipcsgeffc5yw'],
                                                                            part_information['ipcsgisolt5yw']]
                            index_five += 1
    statistical_information_five = pd.merge(left=address, right=statistical_information_five, on=['ZZCMC编号'],
                                            how='right')
    statistical_information_five.to_excel(excel_writer='../data/生成数据/08竞争对手/04五年期IPCSG指标.xlsx',
                                          index=False)


def construct_panel_data():
    """向汽车产业面板数据添加竞争对手三年期/五年期网络指标"""
    auto_panel = pd.read_excel(io='../data/生成数据/05面板数据/汽车产业面板数据(第2版).xlsx',
                               converters={'YEAR': int})

    ipcmg_three_networks = pd.read_excel(io='../data/生成数据/08竞争对手/01三年期IPCMG指标.xlsx',
                                         converters={'企业名称': lambda x: x.strip(),
                                                     '年份': int}).rename(
        columns={'企业名称': 'OGNM', '年份': 'YEAR'})
    del ipcmg_three_networks['ZZCMC编号']
    ipcmg_five_networks = pd.read_excel(io='../data/生成数据/08竞争对手/02五年期IPCMG指标.xlsx',
                                        converters={'企业名称': lambda x: x.strip(),
                                                    '年份': int}).rename(
        columns={'企业名称': 'OGNM', '年份': 'YEAR'})
    del ipcmg_five_networks['ZZCMC编号']
    ipcsg_three_networks = pd.read_excel(io='../data/生成数据/08竞争对手/03三年期IPCSG指标.xlsx',
                                         converters={'企业名称': lambda x: x.strip(),
                                                     '年份': int}).rename(
        columns={'企业名称': 'OGNM', '年份': 'YEAR'})
    del ipcsg_three_networks['ZZCMC编号']
    ipcsg_five_networks = pd.read_excel(io='../data/生成数据/08竞争对手/04五年期IPCSG指标.xlsx',
                                        converters={'企业名称': lambda x: x.strip(),
                                                    '年份': int}).rename(
        columns={'企业名称': 'OGNM', '年份': 'YEAR'})
    del ipcsg_five_networks['ZZCMC编号']
    auto_panel = pd.merge(left=auto_panel, right=ipcmg_three_networks, how='left', on=['OGNM', 'YEAR'])
    auto_panel = pd.merge(left=auto_panel, right=ipcmg_five_networks, how='left', on=['OGNM', 'YEAR'])
    auto_panel = pd.merge(left=auto_panel, right=ipcsg_three_networks, how='left', on=['OGNM', 'YEAR'])
    auto_panel = pd.merge(left=auto_panel, right=ipcsg_five_networks, how='left', on=['OGNM', 'YEAR'])
    auto_panel.to_excel(excel_writer='../data/生成数据/05面板数据/汽车产业面板数据(第3版).xlsx',
                        index=False)


if __name__ == "__main__":
    # 运行时间45分钟
    start_time = datetime.datetime.now()
    eleven()
    twelve()
    thirteen()
    fourteen()
    # 下一步之前要将 成都汽车配件总厂郫县湓斐 的名字改写成 成都汽车配件总厂郫县湓斐?
    construct_panel_data()
    end_time = datetime.datetime.now()
    run_time = end_time - start_time
    print('程序开始时间：', start_time, '\n程序结束时间：', end_time, '\n程序运行时间：', run_time)
