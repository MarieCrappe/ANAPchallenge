# -*- coding: utf-8 -*-
import pandas as pd
import os


data_folder = os.getcwd()
years = ['2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015']

categorical_mapping = {'.z' : -3,
                       '.c' : -2,
                       '.m' : -1
                      }

def mapping(x):
    try:
        return categorical_mapping[x]
    except:
        return x

aggregated_dataframes = {}

for y in years:
    print('processing ',y)
    for filename in os.listdir(os.path.join(data_folder, 'perf_indice', y)):
        if filename.endswith(".csv") and filename.startswith('hd'):
            data_hd = pd.read_csv(os.path.join(data_folder, 'perf_indice', y, filename), encoding='utf-8', sep=';')
            continue

    for col in data_hd.columns:
        data_hd[col] = data_hd[col].map(mapping)

    data_hd.fillna(-4, inplace=True)
    data_hd[u'année'] = y
    aggregated_dataframes[y] = data_hd
    print()
    print('done ', y)
    print()
result = pd.concat(aggregated_dataframes)
result.to_csv(os.path.join(data_folder, 'data_hd.csv'), encoding='utf-8', sep=';')
