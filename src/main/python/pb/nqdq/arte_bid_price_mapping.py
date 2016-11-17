import re, sys
import pandas as pd
import numpy as np
from scipy.optimize import curve_fit
import ast
import csv as csv

input_delim = '\001'
output_delim = '\t'
min_bid = 0.3


def main(input_path, output_arte_path, output_log_path):
    # read in data and convert dimension_struct strings to array of dictionaries
    columns = ['strategy_id', 'algo_id', 'dimension_structs', 'probability_score', 'target_ecpm', 'dt', 'timestamp']
    df = pd.read_csv(input_path, sep=input_delim, header=None, names=columns)
    df['dimension_structs'] = df['dimension_structs'].map(lambda x: ast.literal_eval(x))
    df['dimension_structs'] = df['dimension_structs'].map(
        lambda x: [{'dimension_type_id': y['dimension_type_id'], 'dimension_value': int(y['dimension_value'])} for y in
                   x])

    # fit the curve for each strategy and get the multipliers

    dfm = df.groupby('strategy_id').apply(target_ecpm_seed_dampen)

    # concatenate dimensions to make the ARTE keys
    dfm['key'] = dfm['dimension_structs'].apply(lambda x: make_keys(x))

    # for each row, make a strategy_id:multiplier pair called value_string
    dfm.drop_duplicates(subset=['key', 'strategy_id', 'multiplier'], inplace=True)
    dfm['value_string'] = dfm.apply(
        lambda row: str(row['strategy_id']) + ':' + str(row['multiplier']) + ':' + str(row['baseline']), axis=1)

    # for each key, concatenate the value_strings
    df_to_write = dfm.groupby('key')['value_string'].apply(lambda x: ','.join(x)).reset_index()

    # write ARTE file
    with open(output_arte_path, "w") as f:
        f.write('\n'.join([output_delim.join([col for col in row[1]]) for row in df_to_write.iterrows()]))

    # write log
    dfm.drop(labels=['key', 'value_string'], axis=1, inplace=True)

    make_json(dfm, output_log_path)

    return


# define type of curve to fit
def line(x, m, b):
    return m * x + b


# seed target eCPM strategy list
def target_ecpm_seed(grp):
    target_ecpm = float(grp['target_ecpm'].min())
    p = target_ecpm / grp['probability_score'].max()

    grp['multiplier'] = [x * p for x in grp['probability_score']]

    baseline = sum(grp['multiplier']) / float(len(grp['multiplier']))

    grp['multiplier'] = [x / baseline for x in grp['multiplier']]
    grp['baseline'] = [baseline for x in grp['probability_score']]

    return grp


def target_ecpm_seed_dampen(grp):
    target_ecpm = float(grp['target_ecpm'].min())
    x_scores = np.linspace(grp['probability_score'].min(), grp['probability_score'].max())
    y_prices = np.linspace(min_bid, target_ecpm)

    popt, pcov = curve_fit(line, x_scores, y_prices)

    grp['multiplier'] = line(grp['probability_score'], *popt)

    baseline = min_bid

    grp['multiplier'] = [x / baseline for x in grp['multiplier']]
    grp['baseline'] = [baseline for x in grp['probability_score']]

    return grp


# fit the curve for each strategy and get the multipliers
def min_max_bid(grp):
    x_scores = np.linspace(grp['probability_score'].min(), grp['probability_score'].max())
    y_prices = np.linspace(grp['min_bid'].min(), grp['max_bid'].max())

    popt, pcov = curve_fit(line, x_scores, y_prices)

    grp['multiplier'] = line(grp['probability_score'], *popt)

    return grp


# concatenate dimensions to make the ARTE keys
def make_keys(dim_struct):
    return output_delim.join([str(item['dimension_value']) for item in dim_struct])


# write to line-delimited jsons
def make_json(df, output_path):
    f = open(output_path, "w")
    for row in df.iterrows():
        row[1].to_json(f, orient='index')
        f.write('\n')

    f.close()
    return


if __name__ == '__main__':
    sys.exit(main(sys.argv[1], sys.argv[2], sys.argv[3]))