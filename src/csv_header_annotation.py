'''This script generates a file in order to annotate the columns of CSVs. That is, manually say what a column is about.
Reads CSVs in a folder and output a single file with all the column of said CSVs, the first lines of said CSVs and
a column to annotate each column.

Usage:
    csv_header_annotation.py <i> [options]

Arguments:
    <i>             An input folder with CSVs to analyze
    <o>             The folder path where the files are gonna be downloaded
    --cores CORES   The number of cores to use in a parallel setting [default:2:int]
'''


__author__ = 'Pavel Soriano'
__mail__ = 'pavel.soriano@data.gouv.fr'


import logging
from src.utils import get_files
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)

import pandas as pd
from argopt import argopt
from joblib import Parallel, delayed


def header_extractor(file_path):
    try:
        df = pd.read_csv(file_path)
        return 1
    except Exception as e:
        logger.error(e)
        logger.debug("\t" + file_path)

        return 0
    pass

def header_analysis(files_paths, n_jobs):

    Parallel(n_jobs=n_jobs)(
        delayed(header_extractor)(url, id, output_folder) for url, id in zip(urls, ids))

def header_analysis_single(files_paths):
    succes_read = 0
    for i, f in enumerate(files_paths):
        succes_read += header_extractor(f)
    logger.info("{} files succesfully read from {}".format(succes_read, len(files_paths)))

if __name__ == '__main__':
    parser = argopt(__doc__).parse_args()
    files_path = parser.i
    n_jobs = int(parser.cores)

    files = get_files(files_path)

    if n_jobs == 1:
        header_analysis_single(files)
    else:
        header_analysis(files, n_jobs=n_jobs)
    pass