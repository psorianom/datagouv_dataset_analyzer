'''Transforms XML files into format CoNLL (one token per line)

Usage:
    run_csv_detective.py <i> [options]

Arguments:
    <i>                                An input file or directory (if dir it will convert all txt files inside).
    --output FOLDER                    Folder where to store the output structures [default:.]
    --cores=<n> CORES                  Number of cores to use [default: 2]
    -s                                 Create structures only, after a csv_analysis
'''
import os
from collections import defaultdict
from argopt import argopt
from csv_detective.explore_csv import routine
from joblib import Parallel, delayed
import pickle
from functools import partial
from tqdm import tqdm
import logging
import pandas as pd

from src.utils import get_id

logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

from utils import get_files


def run_csv_detective(file_path):
    logger.info("Treating file {}".format(file_path))
    try:
        inspection_results = routine(file_path)
    except Exception as e:
        logger.info(e)
        return

    if len(inspection_results) > 2 and len(inspection_results["columns"]):
        inspection_results["file"] = file_path
        logger.info(file_path, inspection_results)
        return inspection_results
    else:
        logger.info("Analysis output of file {} was empty".format(files_path))


def build_structures(list_dict_results):
    """
    Builds two dataframes from a list of csv_detective results
    :param list_dict_results:
    :return:
    """

    # uniq_csv_detective_cols = set([])
    # for res in list_dict_results:
    #     uniq_csv_detective_cols.update(res.keys())
    # print(uniq_csv_detective_cols)

    type_dict_csv = build_type_dict(list_dict_results)

    list_dict = []
    for dicto in list_dict_results:
        dicto.pop("columns")
        list_dict.append(dicto)

    csv_detective_df = pd.DataFrame(list_dict)
    print(csv_detective_df)
    return type_dict_csv, csv_detective_df


def get_csv_detective_analysis_single(files_path="./data/unpop_datasets", begin_from=None, n_datasets=None):
    list_files = get_files(files_path)
    if n_datasets:
        list_files = list_files[:n_datasets]

    if begin_from:
        indx_begin = [i for i, path in enumerate(list_files) if begin_from in path]
        if indx_begin:
            list_files = list_files[indx_begin[0]:]

    list_dict_result = []
    for f in tqdm(list_files):
        output_csv_detective = run_csv_detective(f)
        list_dict_result.append(output_csv_detective)

    list_dict_result = [d for d in list_dict_result if d]
    return list_dict_result


def get_csv_detective_analysis(files_path="./data/unpop_datasets", begin_from=None, n_datasets=None, n_jobs=2):
    list_files = get_files(files_path)
    if n_datasets:
        list_files = list_files[:n_datasets]

    if begin_from:
        indx_begin = [i for i, path in enumerate(list_files) if begin_from in path]
        if indx_begin:
            list_files = list_files[indx_begin[0]:]

    run_csv_detective_p = partial(run_csv_detective)
    list_dict_result = Parallel(n_jobs=n_jobs)(
        delayed(run_csv_detective_p)(file_path) for file_path in tqdm(list_files))
    list_dict_result = [d for d in list_dict_result if d]
    return list_dict_result


def build_type_dict(list_dict_results):
    dict_column_dataset = defaultdict(list)
    for l in list_dict_results:
        for k, v in l["columns"].items():
            if "       " in k:
                continue
            l["file_id"] = get_id(l["file"])
            for v2 in v:
                dict_column_dataset[v2].append((l["file_id"], k))

    return dict_column_dataset


if __name__ == '__main__':
    # try_detective(begin_from="DM1_2018_EHPAD")
    parser = argopt(__doc__).parse_args()
    files_path = parser.i
    output_path = parser.output
    n_cores = int(parser.cores)
    structures_only = parser.s

    if not structures_only:
        if n_cores > 1:
            list_dict_result = get_csv_detective_analysis(files_path, begin_from=None, n_datasets=None, n_jobs=n_cores)
        else:
            list_dict_result = get_csv_detective_analysis_single(files_path, begin_from=None, n_datasets=None)

        pickle.dump(list_dict_result, open(os.path.join(output_path, "list_csv_detective_results.pkl"), "wb"))

    assert (os.path.isfile(os.path.join(output_path, "list_csv_detective_results.pkl")))

    type_dict_csv, csv_detective_df = build_structures(pickle.load(
        open(os.path.join(output_path, "list_csv_detective_results.pkl"), "rb")))

    pickle.dump(type_dict_csv, open(os.path.join(output_path, "csv_detective_column_type.pkl"), "wb"))

    csv_detective_df.to_csv(os.path.join(output_path, "csv_detective_attributes.tsv"), sep="\t")

    pass
