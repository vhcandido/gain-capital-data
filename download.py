import os
import shutil
import json
import itertools

from datetime import datetime
from urllib.parse import urljoin, quote
from urllib.request import urlopen
from urllib.error import HTTPError

from utils import wlog_csv, rlog_csv, mktree
from utils import baseurl, path_fmt


def download_file(url, filepath=None):
    if not filepath:
        filepath = os.path.basename(url)

    # Try to download a file and save it locally
    try:
        # TODO ensure url safeness [?]
        mktree(filepath)
        with urlopen(url) as response, open(filepath, "wb") as target:
            # Check (and create) directory structure
            shutil.copyfileobj(response, target)  # Save it locally
    except IOError as e:
        print("[Error] Couldn't retrieve %r to %r\n%s" % (url, filepath, e))
        return False
    except HTTPError as e:
        print("[Error] Couldn't download %r\n%s" % (url, e))
        return False

    return True


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


def main(wfile='metadata/watching_test.json', force=False):
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

    # Keeping track of files already processed
    downloaded = []
    to_unzip = []

    # If force is True, download everything
    if not force:
        # Load the files already downloaded so they can be skipped
        downloaded = rlog_csv(watching['log']['downloaded'])

    # Loop through all combinations
    for (y, m), p, w in itertools.product(mlist, plist, wlist):
        new_download = (y, m, p, w)
        path = path_fmt.format(y, m, p, w, 'zip')

        if new_download in downloaded:  # skip download
            print("Skipping '{}'".format(path))
        else:
            # Join path and base url
            url = urljoin(baseurl, quote(path))
            print("\n'{}'".format(url))

            # Keep GainCapital's structure (path) inside local directory
            filepath = os.path.join(watching['pathto']['zip'], path)

            # Check if it has been successfully downloaded and loaded locally
            if download_file(url, filepath):
                print("Done - '{}'".format(filepath))

                # Save the progress
                downloaded.append(new_download)
                to_unzip.append(new_download)

    # Save metadata about processed files
    wlog_csv(downloaded, watching['log']['downloaded'])
    wlog_csv(to_unzip, watching['log']['to_unzip'], 'a')


if __name__ == '__main__':
    main()



