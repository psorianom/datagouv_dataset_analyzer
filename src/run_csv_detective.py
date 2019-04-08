'''Example programme description.

Usage:
    run_csv_detective.py <x> [<y>...]

Arguments:
    <x>                   The path where the CSVs are stored.
    --anarg=<a>           int, Description here [default: 1e3].
    -p PAT, --patts PAT   file, Or (default: None).
    --bar=<b>             str, Another [default: something] should
                          assume str like everything else.
    -f, --force           Force.
    -h, --help            Show this help message and exit.
    -v, --version         Show program's version number and exit.

'''

from collections import defaultdict
from docopt import docopt
from csv_detective.explore_csv import routine
from joblib import Parallel, delayed
import pickle
from functools import partial
from tqdm import tqdm
import logging
logger=logging.getLogger()
logger.setLevel(logging.DEBUG)

from utils import get_files


def run_csv_detective(file_path):

    logger.info(file_path)
    try:
        inspection_results = routine(file_path)
    except Exception as e:
        logger.info(e)
        return

    if len(inspection_results) > 2 and len(inspection_results["columns"]):
        inspection_results["file"] = file_path
        print(file_path, inspection_results["columns"])
        return inspection_results


def get_csv_detective_analysis(files_path="./data/unpop_datasets", begin_from=None, n_datasets=None, n_jobs=2):
    list_files = get_files(files_path)
    if n_datasets:
        list_files = list_files[:n_datasets]

    if begin_from:
        indx_begin = [i for i, path in enumerate(list_files) if begin_from in path]
        if indx_begin:
            list_files = list_files[indx_begin[0]:]

    run_csv_detective_p = partial(run_csv_detective)
    list_dict_result = Parallel(n_jobs=n_jobs)(delayed(run_csv_detective_p)(file_path) for file_path in tqdm(list_files))
    list_dict_result = [d for d in list_dict_result if d]
    pickle.dump(list_dict_result, open("list_csv_detective_results.pkl", "wb"))


def try_detective(begin_from=None):
    dict_column_dataset = {}
    list_files = get_files("./data/unpop_datasets")
    for file_path in list_files:
        if begin_from and begin_from not in file_path:
            continue

        logger.info(file_path)
        # with open(file_path, "r") as filo:
        inspection_results = routine(file_path)
        if len(inspection_results) > 2 and len(inspection_results["columns"]):
            logger.info(file_path, inspection_results["columns"])
            for k, v in inspection_results["columns"].items():
                for v2 in v:
                    dict_column_dataset[v2] = (file_path, k)
            pickle.dump(dict_column_dataset, open("dict_col_dataset.pkl", "wb"))

        else:
            logger.info(file_path, inspection_results)


def analyze_detected_csvs():
    list_csv_detective = pickle.load(open("list_csvdetective_results.pkl", "rb"))
    list_csv_detective = [l for l in list_csv_detective if l]
    dict_column_dataset = defaultdict(list)
    for l in list_csv_detective:
        for k, v in l["columns"].items():
            if "       " in k:
                continue
            for v2 in v:
                dict_column_dataset[v2].append((l["file"], k))

    pickle.dump(dict_column_dataset, open("dict_column_dataset.pkl", "wb"))




if __name__ == '__main__':
    # try_detective(begin_from="DM1_2018_EHPAD")
    args = docopt(__doc__)
    files_path = args["<x>"]

    get_csv_detective_analysis(files_path, begin_from=None, n_datasets=None, n_jobs=1)
    # analyze_detected_csvs()
    pass