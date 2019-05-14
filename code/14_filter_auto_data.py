# -*- coding:utf-8 -*-
"""筛选2006前后变化数据"""

import pandas as pd

auto = pd.read_excel(
    io='../data/原始数据/05附加数据/CompleteVehicleAndVehiclePartFirmSampleDropEngineFirm2006Shock20181104.xls',
    converters={'year': int})
# auto = pd.DataFrame(
#     {'ognm': ['zhong', 'zhong', 'zhong'], 'year': [2004, 2006, 2007], 'cplvhcdum': [0, 1, 1], 'vhcpartdum': [0, 1, 1]})

auto_columns = list(auto.columns)
new_data = pd.DataFrame(columns=auto_columns)
auto = auto.groupby(by=['ognm'])
for name, group in auto:
    group = group.sort_values(by=['year'], ascending=True).reset_index(drop=True)
    group_A = group['cplvhcdum'][group['year'] < 2006]
    if not group_A.empty:
        group_A = group_A.iloc[-1]
    else:
        group_A = 'False'
    group_B = group['cplvhcdum'][group['year'] == 2006]
    if not group_B.empty:
        group_B = group_B.iloc[0]
    else:
        group_B = 'False'
    group_C = group['cplvhcdum'][group['year'] > 2006]
    if not group_C.empty:
        group_C = group_C.iloc[0]
    else:
        group_C = 'False'
    if group_A != 'False' and group_B != 'False' and group_C != 'False':
        if len(set(list([group_A, group_B, group_C]))) >= 2:
            group['cplvhcchange'] = 1
        else:
            group['cplvhcchange'] = 0
    elif group_A != 'False' and group_C != 'False':
        if group_A != group_C:
            group['cplvhcchange'] = 1
        else:
            group['cplvhcchange'] = 0
    else:
        group['cplvhcchange'] = 0

    group_A = group['vhcpartdum'][group['year'] < 2006]
    if not group_A.empty:
        group_A = group_A.iloc[-1]
    else:
        group_A = 'False'
    group_B = group['vhcpartdum'][group['year'] == 2006]
    if not group_B.empty:
        group_B = group_B.iloc[0]
    else:
        group_B = 'False'
    group_C = group['vhcpartdum'][group['year'] > 2006]
    if not group_C.empty:
        group_C = group_C.iloc[0]
    else:
        group_C = 'False'
    if group_A != 'False' and group_B != 'False' and group_C != 'False':
        if len(set(list([group_A, group_B, group_C]))) >= 2:
            group['vhcpartchange'] = 1
        else:
            group['vhcpartchange'] = 0
    elif group_A != 'False' and group_C != 'False':
        if group_A != group_C:
            group['vhcpartchange'] = 1
        else:
            group['vhcpartchange'] = 0
    else:
        group['vhcpartchange'] = 0

    new_data = new_data.append(group, ignore_index=True)

auto_columns.extend(['cplvhcchange', 'vhcpartchange'])
new_data = new_data[auto_columns]
new_data.to_excel(
    excel_writer='../data/原始数据/05附加数据/CompleteVehicleAndVehiclePartFirmSampleDropEngineFirm2006Shock20190503.xlsx',
    index=False)
