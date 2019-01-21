import os
import csv
import sys
import json
import itertools

from datetime import datetime


baseurl = 'http://ratedata.gaincapital.com/'
path_fmt = '{}/{}/{}_Week{}.{}'


def monthlist(start, end):
    str_months = [
        '01 January',
        '02 February',
        '03 March',
        '04 April',
        '05 May',
        '06 June',
        '07 July',
        '08 August',
        '09 September',
        '10 October',
        '11 November',
        '12 December',
    ]

    if start['year'] == end['year']:
        mlist = [
            (end['year'], m) for m in range(start['month'] - 1, end['month'])
        ]
    else:
        mlist = []
        for y in range(start['year'], end['year'] + 1):
            if y == start['year']:
                months = range(start['month'] - 1, 12)
            elif y == end['year']:
                months = range(end['month'])

            mlist += [(y, m) for m in months]

    # Format as ('%Y', '%m %B')
    ymlist = [(str(y), str_months[m]) for y, m in mlist]
    return ymlist


def build_combn(start, end, pairs):
    # Building the lists with parameters to loop through
    ymlist = monthlist(start, end)  # months
    plist = [p[:3] + '_' + p[3:] for p in pairs]  # pairs
    wlist = [str(i) for i in range(1, 6)]  # weeks
    # note: wlist is list of string to keep compatibility with data loaded
    # from the CSV file (as string).

    return itertools.product(ymlist, plist, wlist)


def load_watchfile(wfile):
    # Load information of pairs and dates to keep track of
    with open(wfile, 'r') as f:
        watching = json.load(f)

    # Download until this month
    if not 'end' in watching:
        today = datetime.today()
        watching['end'] = {'year': today.year, 'month': today.month}

    return watching


def mktree(path):
    '''
    Create directory structure

    :param path:
    :return:
    '''

    # Save directory structure
    dirpath = os.path.dirname(path)

    # Build the same directory structure (if it doesn't exist)
    if dirpath and not os.path.exists(dirpath):
        print("Creating tree: '{}'".format(dirpath))
        os.makedirs(dirpath)


def wlog_csv(data, filename, mode='w'):
    mktree(filename)
    with open(filename, mode, newline='') as csvfile:
        fieldnames = ['year', 'month', 'pair', 'week']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        if mode == 'w':
            writer.writeheader()

        for row in sorted(data):
            writer.writerow({k: v for k, v in zip(fieldnames, row)})


def rlog_csv(filename):
    try:
        with open(filename, newline='') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=',')
            data = [tuple(row.values()) for row in reader]
    except FileNotFoundError as e:
        data = []
    return data


def query_yes_no(question, default="yes"):
    """Ask a yes/no question via raw_input() and return their answer.
    https://stackoverflow.com/questions/3041986/apt-command-line-interface-like-yes-no-input

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
        It must be "yes" (the default), "no" or None (meaning
        an answer is required of the user).

    The "answer" return value is True for "yes" or False for "no".
    """

    valid = {"yes": True, "y": True, "ye": True, "no": False, "n": False}

    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        choice = input(question + prompt).lower().strip()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write(
                "Please respond with 'yes' or 'no' (or 'y' or 'n').\n"
            )
