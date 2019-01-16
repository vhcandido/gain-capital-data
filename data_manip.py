import sys
import pandas as pd
import download_gaincapital as gaincapital
import candle_creation as candles
from joblib import Parallel, delayed
from math import sqrt


def main(args):
    pairs = [
        'AUDCAD',
        'AUDJPY',
        'AUDNZD',
        'AUDUSD',
        'CADJPY',
        'EURAUD',
        'EURCAD',
        'EURGBP',
        'EURJPY',
        'EURNZD',
        'EURUSD',
        'GBPAUD',
        'GBPCAD',
        'GBPJPY',
        'GBPNZD',
        'GBPUSD',
        'NZDCAD',
        'NZDJPY',
        'NZDUSD',
        'USDCAD',
        'USDJPY',
    ]
    # periods = { 2013: range(8,13),
    #        2014: range(1,13),
    #        2015: range(1,13),
    #        2016: range(1,8) }
    periods = {2016: [8]}
    data_dir = '../data/'
    tick_dir = data_dir + 'tick/'

    for arg in args:
        if arg == 'download':
            print('Downloading')
            gaincapital.download_data(pairs, periods, tick_dir)
        else:
            if arg == 'all_candles':
                print('All candles in one')
                action = 'all_candles'
            elif arg == 'update_candles':
                print('Update candles')
                action = 'update_candles'
            elif arg == 'split_candles':
                print('Split candles')
                action = 'split_candles'

            filenames = gaincapital.build_file_names(pairs, periods, tick_dir)

            print(
                '\n'.join(
                    "%s: %s" % (p, f)
                    for p in filenames.keys()
                    for f in filenames[p]
                )
            )
            Parallel(n_jobs=6)(
                delayed(candles.create_ohlc_file)(
                    pair, filenames[pair], action
                )
                for pair in pairs
            )


if __name__ == '__main__':
    if len(sys.argv) > 1:
        main(sys.argv[1:])
    else:
        exit('Not enough arguments passed')
