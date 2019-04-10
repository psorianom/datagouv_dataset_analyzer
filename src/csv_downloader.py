'''Downloads the URLs specified found in a csv (given the right column) and saves it with the resource id

Usage:
    csv_downloader.py <i> <o> <c> <r> [options]

Arguments:
    <i>             An input csv that contains a column with urls to download
    <o>             The folder path where the files are gonna be downloaded
    <c>             The column that contains the URLs
    <r>             The column containing the resource id
    --cores CORES   The number of cores to use in a parallel setting [default:2:int]
'''


import subprocess
from asyncore import file_dispatcher

import pandas as pds
from argopt import argopt
from joblib import Parallel, delayed


def downloader(url, id, output_folder):

    p = subprocess.Popen(["wget", "-O", "{0}/{1}.csv".format(output_folder, idl), url])
    p.communicate()  # now wait plus that you can send commands to process


def get_files(file_path, output_folder, url_column, id_column, n_jobs):
    df = pds.read_csv(file_path)
    urls = df[url_column].values[:]
    ids = df[id_column].values[:]
    assert(len(urls) == len(ids))

    Parallel(n_jobs=n_jobs)(
        delayed(downloader)(url, id, output_folder) for url, id in zip(urls, ids))

if __name__ == '__main__':
    # try_detective(begin_from="DM1_2018_EHPAD")
    parser = argopt(__doc__).parse_args()
    file_path = parser.i
    output_folder = parser.o
    url_column = parser.c
    id_column = parser.r
    n_jobs = int(parser.cores)

    get_files(file_path, output_folder, url_column, id_column, n_jobs)

