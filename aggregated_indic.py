# -*- coding: utf-8 -*-
import pandas as pd
import os
import re
from collections import defaultdict


data_folder = os.getcwd()
years = ['2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015']


def convert_point(x):
    return re.sub(',', '.', x)


def part_total_marche(x, col):
    return x[col] / float(x['count'])

aggregated_dataframes = {}

for y in years:
    print('processing for', y)
    for filename in os.listdir(os.path.join(data_folder, 'perf_indice', y)):
        if filename.endswith(".csv") and filename.startswith('pdmreg'):
            data_reg = pd.read_csv(os.path.join(data_folder, 'perf_indice', y, filename), encoding='utf-8', sep=';')
            continue
        elif filename.startswith('pdmza'):
            data_za = pd.read_csv(os.path.join(data_folder, 'perf_indice', y, filename), encoding='utf-8', sep=';')
            continue

    data_za.drop('A6', inplace=True, axis=1)
    data_za.fillna('0', inplace=True)
    print()
    print('nombre d\'hopitaux dans pmza', data_za['finess'].nunique())
    print('nombre d\'hopitaux dans pmreg', data_reg['finess'].nunique())
    print()
    for col in ['A1', 'A2', 'A3', 'A4', 'A5']:
        data_za[col] = pd.to_numeric(data_za[col].apply(convert_point))

    grouped = data_za.groupby('finess')
    zone_activite = {}
    for finess in grouped.groups:
        zone_activite[finess] = grouped.get_group(finess)['zone'].sum()

    # Get the id of the hospitals with the same zone
    new_dict = defaultdict(list)
    for k, v in zone_activite.items():
        new_dict[v].append(k)

    # Compute the size of the zone % the hospital
    nb_hopit_postal = data_za.groupby('finess').size()
    nb_hopit_postal = nb_hopit_postal.reset_index()
    nb_hopit_postal.columns = ['finess', 'count']

    # construct the dataframe that groups the postal code
    data_part = pd.merge(data_za, nb_hopit_postal, on='finess')
    for col in ['A1', 'A2', 'A3', 'A4', 'A5']:
        data_part[col] = data_part.apply(part_total_marche, args=(col,), axis=1)

    data_part.drop('count', inplace=True, axis=1)
    data_part_marche = data_part.groupby('finess').sum()
    data_part_marche.reset_index(inplace=True)

    # zone d'attractivit column
    for k, v in zone_activite.items():
        zone_activite[k] = abs(hash(v)) % (10 ** 8)

    data_part_marche['zone_attractivite'] = data_part_marche['finess'].apply(lambda x: zone_activite[x])

    # replace nan by zeros:
    data_reg.fillna('0', inplace=True)

    # Convert string to float
    for col in ['A1bis', 'A2bis', 'A3bis', 'A4bis', 'A5bis']:
        data_reg[col] = pd.to_numeric(data_reg[col].apply(convert_point))

    data_indicateur = pd.merge(data_part_marche, data_reg, on='finess')
    data_indicateur[u'année'] = y
    aggregated_dataframes[y] = data_indicateur
    print()
    print('done for', y)
    print()
result = pd.concat(aggregated_dataframes)
result.to_csv(os.path.join(data_folder, 'indicateur_part_marche.csv'), encoding='utf-8', sep=';')
