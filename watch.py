import os
import json
import csv
import itertools

from datetime import datetime
from urllib.parse import urljoin, quote

from download import download_and_check


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
    mlist = [(str(y), str_months[m]) for y, m in mlist]
    return mlist


def watch_and_download(
    dfile='metadata/downloaded.csv', wfile='metadata/watching_test.json'
):
    baseurl = 'http://ratedata.gaincapital.com/'
    path_fmt = '{}/{}/{}_Week{}.zip'

    # Load information of pairs and dates to keep track of
    with open(wfile, 'r') as f:
        watching = json.load(f)

    # Download until this month
    if not 'end' in watching:
        today = datetime.today()
        watching['end'] = {'year': today.year, 'month': today.month}

    # Building the lists with parameters to loop through
    mlist = monthlist(watching['start'], watching['end'])  # months
    plist = [p[:3] + '_' + p[3:] for p in watching['pairs']]  # pairs
    wlist = [str(i) for i in range(1, 6)]  # weeks
    # note: wlist is list of string to keep compatibility with data loaded
    # from the CSV file (as string).

    # Load the files already downloaded so they can be skipped
    with open(dfile, newline='') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',')
        downloaded = [tuple(row) for row in spamreader]

    # Loop through all combinations
    for ym, p, w in itertools.product(mlist, plist, wlist):
        new_download = (*ym, p, w)
        path = path_fmt.format(*ym, p, w)

        if new_download in downloaded:  # skip download
            print("Skipping '{}'".format(path))
        else:
            # Join path and base url
            url = urljoin(baseurl, quote(path))
            print("\n'{}'".format(url))

            # Keep GainCapital's structure (path) inside local directory
            filepath = os.path.join(watching['pathto']['tick'], path)

            # Check if it has been successfully downloaded and loaded locally
            if download_and_check(url, filepath):
                print("Done - '{}'".format(filepath))

                # Save the progress
                downloaded.append(new_download)

    # Save metadata about downloaded files
    with open('metadata/downloaded.csv', 'w', newline='') as csvfile:
        # TODO sort downloaded content
        spamwriter = csv.writer(csvfile, delimiter=',')
        for row in downloaded:
            spamwriter.writerow(row)


if __name__ == '__main__':
    watch_and_download()
