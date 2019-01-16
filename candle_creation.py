#!/usr/bin/env python3

from datetime import *
import pandas as pd
import pdb

def conv_str_to_datetime(x):
    if len(x) < 26:
        x += '.'
        x = x.ljust(26,'0')
    return(datetime.strptime(x[:26], '%Y-%m-%d %H:%M:%S.%f'))

def load_csv(filename):
    #df = pd.read_csv('../data/AUDCAD.2016.03.csv', usecols=['RateAsk', 'RateBid'], converters={'RateDateTime': conv_str_to_datetime})
    df = pd.read_csv(filename, usecols=['RateDateTime', 'RateBid', 'RateAsk'], index_col='RateDateTime', parse_dates=True)
    return df

def tick_to_ohlc(tick_data, price='RateAsk'):
    ohlc = tick_data[price].resample('1h').ohlc()
    ohlc = ohlc.dropna()
    ohlc.columns = ['Open', 'High', 'Low', 'Close']
    ohlc.index.name = 'DateTime'
    return ohlc

def create_ohlc_file(pair_name, filenames, action='all_candles'):
    print('\nCreating file for', pair_name)

    if action == 'split_candles':
        for filename in filenames:
            print('%s: Loading tick data - %s' % (pair_name, filename))
            data = load_csv(filename)

            print('%s: Tranforming tick to OHLC - %s' % (pair_name, filename))
            data = tick_to_ohlc(data)

            hourly_data.to_csv(filename.replace('tick', 'split'), float_format='%.6f')

    else:
        outfile_name = '../data/' + pair_name + '.csv'
        hourly_data = pd.DataFrame()
        if action == 'update_candles':
            print(outfile_name)
            hourly_data = pd.read_csv(outfile_name, index_col = 'DateTime', parse_dates = True)

        with open(outfile_name, 'w') as outfile:
            for filename in filenames:
                print('%s: Loading tick data - %s' % (pair_name, filename))
                data = load_csv(filename)

                print('%s: Tranforming tick to OHLC - %s' % (pair_name, filename))
                data = tick_to_ohlc(data)

                hourly_data = pd.concat( [hourly_data, data] )

            hourly_data.sort_index(inplace=True)
            hourly_data.to_csv(outfile, float_format='%.6f')

