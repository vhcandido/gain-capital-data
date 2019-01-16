import os
import zipfile
import shutil
from urllib.request import urlopen


# Download all weeks of a month and create a .csv file containing them
def download_month_unzipped(url, filename, target_dir='.'):
    # name of the .csv file to be created
    # 'AAABBB.9999.00.csv'
    target_file = os.path.join(target_dir, filename[:14] + '.csv')
    f = open(target_file, 'w')
    print('\n' + '-' * 50)
    print('Creating \'%s\'' % (target_file))

    zip_list = dict()
    # download files and unzip them
    first_week = True
    for week in [1, 2, 3, 4, 5]:
        url3 = url + '_Week' + str(week) + '.zip'
        filename3 = filename + '.' + str(week) + '.zip'

        # download the .zip file related to a specific week
        print('-----')
        print('Downloading \'%s\'' % (filename3))
        name = os.path.join(target_dir, filename3)
        z = download_zip(url3, name)
        if not z:
            continue
        # loop through all files within the .zip
        for n in z.namelist():
            print('Unzipping')
            data = z.read(n).decode('ascii')
            print('Appending to csv')
            # without the first line if it's a header (not in the first week)
            if not first_week and not data[0].isdigit():
                data = data.splitlines(True)[1:]
                f.writelines(data)
            else:
                f.write(data)
                first_week = False
            # remove it
            z.close()
            os.unlink(name)
    f.close()


def download_zip(url, file_name):
    try:
        with urlopen(url.replace(' ', '%20')) as response, open(
            file_name, 'wb'
        ) as out_file:
            shutil.copyfileobj(response, out_file)
    except IOError as e:
        print("Can't retrieve %r to %r\n%s" % (url, file_name, e))
        return False

    try:
        z = zipfile.ZipFile(file_name)
    except zipfile.error as e:
        print("Bad zipfile (from %r): %s" % (url, e))
        os.unlink(file_name)
        return False

    return z


def download_data(pairs, periods, target_dir='.'):
    file_names = build_file_names(pairs, periods, target_dir, download=True)
    return file_names


def build_file_names(pairs, periods, target_dir='.', download=False):
    file_names = dict()

    avail_months = [
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

    url = 'http://ratedata.gaincapital.com/'
    years = list(periods.keys())
    years.sort()
    for pair in pairs:
        file_names[pair] = list()
        base_cur = pair[:3]
        quote_cur = pair[3:]
        # 'AAABBB.'
        filename = base_cur + quote_cur + '.'
        for year in years:
            url1 = url + str(year) + '/'
            # 'AAABBB.9999.'
            filename1 = filename + str(year) + '.'
            months = [avail_months[m - 1] for m in periods[year]]
            for month in months:
                url2 = url1 + month + '/'
                url2 += base_cur + '_' + quote_cur
                # 'AAABBB.9999.00 MMMMMM'
                filename2 = filename1 + month
                if download:
                    download_month_unzipped(url2, filename2, target_dir)
                file_names[pair].append(
                    os.path.join(target_dir, filename2[:14] + '.csv')
                )
    return file_names
