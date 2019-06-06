#!/usr/bin/env python

import sys
import pandas as pd
import os

from joblib import Parallel, delayed
from enum import Enum
from utils import path_fmt, build_combn, load_watchfile, mktree
from enum import Enum


class ApplyRemove(Enum):
    NOT_ASSIGNED = -1
    ASK = 0
    KEEP_ALL = 1
    RM_ALL = 2


APPLY_ALL_CSV = ApplyRemove.NOT_ASSIGNED


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


def ask_remove(filename):
    if APPLY_ALL_CSV == ApplyRemove.RM_ALL:
        print("Removing it...")
        os.unlink(src)
    elif (
        APPLY_ALL_CSV == ApplyRemove.NOT_ASSIGNED
        or APPLY_ALL_CSV == ApplyRemove.ASK
    ):
        answer = query_yes_no("Remove it?", default='no')
        if answer:
            print("Removing it...")
            os.unlink(src)
        if APPLY_ALL_CSV == ApplyRemove.NOT_ASSIGNED:
            if query_yes_no("Apply to all?", default='yes'):
                APPLY_ALL_CSV = (
                    ApplyRemove.RM_ALL if answer else ApplyRemove.KEEP_ALL
                )
            else:
                APPLY_ALL_CSV = ApplyRemove.ASK


def main(wfile, period='1h', per_file='year'):
    watching = load_watchfile(wfile)

    dled = [
        (*ym, p, w)
        for ym, p, w in build_combn(
            watching['start'], watching['end'], watching['pairs']
        )
    ]

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
                df = load_csv(filename)
                if df.empty:
                    print("Removing empty file: '{}'".format(filename))
                    #ask_remove(filename)  # won't be used when in parallel
                    os.unlink(filename)
                    continue
                outlist.append(tick_to_ohlc(df, period=period))
            else:
                print("Couldn't find '{}'".format(filename))

        if not outlist:
            return

        print("Writing to '{}'\n".format(outfile))
        mktree(outfile)
        pd.concat(outlist).sort_index().to_csv(outfile, float_format="%.6f")

    Parallel(n_jobs=5)(
        delayed(parse_ohlc)(name, df) for name, df in df_dled.groupby(gcol)
    )


class Args(Enum):
    PROG = 0
    FILE = 1
    PERIOD = 2
    PER = 3
    N = 4


if __name__ == '__main__':
    if len(sys.argv) < Args.N.value:
        print(
            "usage: ./{} file period per_file".format(
                sys.argv[Args.PROG.value]
            )
        )
        exit(1)
    else:
        main(
            sys.argv[Args.FILE.value],
            sys.argv[Args.PERIOD.value],
            sys.argv[Args.PER.value],
        )
