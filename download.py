import os
import zipfile
import shutil

from urllib.request import urlopen
from urllib.error import HTTPError

# from urllib.parse import quote


def download_file(url, filepath):
    if not os.path.exists(os.path.dirname(filepath)):
        os.makedirs(os.path.dirname(filepath))

    # TODO ensure url safeness
    with urlopen(url) as response, open(filepath, "wb") as target:
        shutil.copyfileobj(response, target)


def download_and_check(url, filepath):
    # Try to download the zipfile and save it locally
    try:
        download_file(url, filepath)
    except IOError as e:
        print("Couldn't retrieve %r to %r\n%s" % (url, filepath, e))
        return None
    except HTTPError as e:
        print("Couldn't retrieve %r to %r\n%s" % (url, filepath, e))
        return None

    # Try to load it from disk
    try:
        z = zipfile.ZipFile(filepath)
    except zipfile.error as e:
        print("Bad zipfile (from %r): %s" % (url, e))
        os.unlink(filepath)
        return None
    except FileNotFoundError as e:
        print("Couldn't load %s\n%s" % (filepath, e))
        return None

    return z
