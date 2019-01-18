import os
import json
import zipfile

from utils import rlog_csv, wlog_csv, mktree, query_yes_no
from utils import path_fmt

from enum import Enum

class ApplyRemove(Enum):
    EMPTY = -1
    ASK = 0
    KEEP_ALL = 1
    RM_ALL = 2

APPLY_ALL_CSV = ApplyRemove.EMPTY

def is_empty_csv(data):
    from io import StringIO
    reader = StringIO(data)

    # Try to read header
    if reader.readline():
        return (not any(reader))
    return True


def load_zip(filepath, askrm=True):
    '''

    :param filepath:
    :param rmbad: Remove if there's an error loading it
    :return:
    '''
    # Try to load it from disk
    try:
        z = zipfile.ZipFile(filepath)
        return z
    except zipfile.error as e:
        print("[Error] {}: '{}'".format(e, filepath))
        if not askrm or (askrm and query_yes_no("Remove it?", default='no')):
            print("Removing it...")
            os.unlink(filepath)
    except FileNotFoundError as e:
        print(e)


def unzip(src, dst):
    global APPLY_ALL_CSV
    print("\nExtracting from '{}'".format(src))

    z = load_zip(src)
    if not z or not z.namelist():
        return False

    # Read zip content (as string)
    csv_data = z.read(z.namelist()[0]).decode('ascii')
    z.close()

    # Check if it contains data
    if is_empty_csv(csv_data):
        print("CSV content is empty: '{}'".format(src))
        if APPLY_ALL_CSV == ApplyRemove.RM_ALL:
            print("Removing it...")
            os.unlink(src)
        elif APPLY_ALL_CSV == ApplyRemove.EMPTY or APPLY_ALL_CSV == ApplyRemove.ASK:
            answer = query_yes_no("Remove it?", default='no')
            if answer:
                print("Removing it...")
                os.unlink(src)
            if APPLY_ALL_CSV == ApplyRemove.EMPTY:
                if query_yes_no("Apply to all?", default='yes'):
                    APPLY_ALL_CSV = ApplyRemove.RM_ALL if answer else ApplyRemove.KEEP_ALL
                else:
                    APPLY_ALL_CSV = ApplyRemove.ASK
        return False

    # Create directory structure and write data to disk
    mktree(dst)
    with open(dst, 'w') as f:
        f.write(csv_data)

    return True


def main(wfile='metadata/watching_test.json'):
    with open(wfile) as f:
        watching = json.load(f)

    to_unzip = rlog_csv(watching['log']['to_unzip'])
    downloaded = rlog_csv(watching['log']['downloaded'])

    for row in to_unzip:
        zipname = path_fmt.format(*row, 'zip')
        csvname = path_fmt.format(*row, 'csv')
        if not unzip(
                src=os.path.join(watching['pathto']['zip'], zipname),
                dst=os.path.join(watching['pathto']['tick'], csvname)
        ) and row in downloaded:
            print("Download it again in the future")
            downloaded.remove(row)


    wlog_csv([], watching['log']['to_unzip'])
    wlog_csv(downloaded, watching['log']['downloaded'])


if __name__ == '__main__':
    main()
