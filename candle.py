import sys
import pandas as pd
import os

from joblib import Parallel, delayed
from enum import Enum
from utils import path_fmt, build_combn, load_watchfile, mktree


def load_csv(filename):
    # df = pd.read_csv('../data/AUDCAD.2016.03.csv', usecols=['RateAsk', 'RateBid'], converters={'RateDateTime': conv_str_to_datetime})
    df = pd.read_csv(
        filename,
        usecols=['RateDateTime', 'RateBid', 'RateAsk'],
        index_col='RateDateTime',
        parse_dates=True,
    )
    return df


def tick_to_ohlc(tick_data, price='RateAsk', period='1h'):
    ohlc = tick_data[price].resample(period).ohlc()
    ohlc = ohlc.dropna()
    ohlc.columns = ['Open', 'High', 'Low', 'Close']
    ohlc.index.name = 'DateTime'
    return ohlc


def main(wfile, period='1h', per_file = 'year'):
    watching = load_watchfile(wfile)
    dled = build_combn(watching['start'], watching['end'], watching['pairs'])
    dled = [(*ym, p, w) for ym, p, w in dled]
    #dled = [
    #    ('2013', '01 January', 'AUD_CAD', '2'),
    #    ('2013', '01 January', 'AUD_CAD', '3'),
    #    ('2013', '01 January', 'AUD_CAD', '4'),
    #    ('2013', '01 January', 'AUD_CAD', '5'),
    #]
    df_dled = pd.DataFrame(dled, columns=['year', 'month', 'pair', 'week'])
    infile = os.path.join(watching['pathto']['tick'], path_fmt)

    if per_file == 'year':
        gcol = ['year', 'pair']
        ohlc_fmt = "./data/{}/{}/{}.csv"
    elif per_file == 'month':
        gcol = ['year', 'month', 'pair']
        ohlc_fmt = "./data/{}/{}/{}-{}.csv"
    elif per_file == 'week':
        gcol = ['year', 'month', 'pair']
        ohlc_fmt = "./data/{}/{}/{}-{}-{}.csv"

    def parse_ohlc(name, df):
        print()
        print(name)

        y, m, p, w = df.values[0]
        outfile = ohlc_fmt.format(period, p, y, m[:2], w)

        outlist = []
        for y, m, p, w in df.values:
            filename = infile.format(y, m, p, w, 'csv')
            if os.path.exists(filename):
                print("Parsing '{}'".format(filename))
                outlist.append(tick_to_ohlc(load_csv(filename), period='1h'))
            else:
                print("Couldn't find '{}'".format(filename))

        if not outlist:
            return

        print("Writing to '{}'\n".format(outfile))
        mktree(outfile)
        pd.concat(outlist).sort_index().to_csv(outfile, float_format="%.6f")


    Parallel(n_jobs=6)(
        delayed(parse_ohlc)(name, df)
        for name, df in df_dled.groupby(gcol)
    )
    #for name, df in df_dled.groupby(['year', 'month', 'pair']):
    #    print()
    #    print(name)

    #    y, m, p, _ = df.values[0]
    #    outfile = ohlc_fmt.format(period, p, y, m[:2])

    #    outlist = []
    #    for y, m, p, w in df.values:
    #        filename = infile.format(y, m, p, w, 'csv')
    #        if os.path.exists(infile):
    #            print("Parsing '{}'".format(filename))
    #            outlist.append(tick_to_ohlc(load_csv(filename), period='1h'))
    #        else:
    #            print("Couldn't find '{}'".format(filename))

    #    if not outlist:
    #        continue

    #    print("Writing to '{}'\n".format(outfile))
    #    mktree(outfile)
    #    pd.concat(outlist).sort_index().to_csv(outfile)


class Args(Enum):
    PROG = 0
    FILE = 1
    PERIOD = 2
    PER = 3
    N = 4


if __name__ == '__main__':
    if len(sys.argv) < Args.N.value:
        print("usage: ./{} file period per_file".format(sys.argv[Args.PROG.value]))
        exit(1)
    else:
        main(
            sys.argv[Args.FILE.value],
            sys.argv[Args.PERIOD.value],
            sys.argv[Args.PER.value]
        )







