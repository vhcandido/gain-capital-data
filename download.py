import sys
import os
import shutil
import json
import itertools

from datetime import datetime
from urllib.parse import urljoin, quote
from urllib.request import urlopen
from urllib.error import HTTPError

from utils import wlog_csv, rlog_csv, mktree, build_combn, load_watchfile
from utils import baseurl, path_fmt

from enum import Enum


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


def main(wfile='metadata/watching_test.json', force=False):
    watching = load_watchfile(wfile)
    combinations = build_combn(watching['start'], watching['end'], watching['pairs'], range(1, 6))

    # Keeping track of files already processed
    downloaded = []
    to_unzip = []

    # If force is True, download everything
    if not force:
        # Load the files already downloaded so they can be skipped
        downloaded = rlog_csv(watching['log']['downloaded'])

    # Loop through all combinations
    for (y, m), p, w in combinations:
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


class Args(Enum):
    PROG = 0
    FILE = 1
    FORCE = 2
    N = 3


if __name__ == '__main__':
    if len(sys.argv) == Args.N.value - 1:
        main(sys.argv[Args.FILE.value])
    elif len(sys.argv) == Args.N.value:
        main(sys.argv[Args.FILE.value], True)
    else:
        print("usage: ./{} file [--force]".format(sys.argv[Args.PROG.value]))
        exit(1)

