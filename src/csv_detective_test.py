from csv_detective.explore_csv import routine
from joblib import Parallel, delayed
import pickle
from functools import partial
from tqdm import tqdm

from utils import get_files


def run_csv_detective(file_path):

    print(file_path)
    try:
        inspection_results = routine(file_path)
    except Exception as e:
        print(e)
        return

    if len(inspection_results) > 2 and len(inspection_results["columns"]):
        inspection_results["file"] = file_path
        print(file_path, inspection_results["columns"])
        return inspection_results


def get_detective_analysis(begin_from=None, n_datasets=None, n_jobs=2):
    list_files = get_files("./data/unpop_datasets")
    if n_datasets:
        list_files = list_files[:n_datasets]

    if begin_from:
        indx_begin = [i for i, path in enumerate(list_files) if begin_from in path]
        if indx_begin:
            list_files = list_files[indx_begin:]

    run_csv_detective_p = partial(run_csv_detective)
    list_dict_result = Parallel(n_jobs=n_jobs)(delayed(run_csv_detective_p)(file_path) for file_path in tqdm(list_files))
    pickle.dump(list_dict_result, open("list_csvdetective_results.pkl", "wb"))


def try_detective(begin_from=None):
    dict_column_dataset = {}
    list_files = get_files("./data/unpop_datasets")
    for file_path in list_files:
        if begin_from and begin_from not in file_path:
            continue

        print(file_path)
        # with open(file_path, "r") as filo:
        inspection_results = routine(file_path)
        if len(inspection_results) > 2 and len(inspection_results["columns"]):
            print(file_path, inspection_results["columns"])
            for k, v in inspection_results["columns"].items():
                for v2 in v:
                    dict_column_dataset[v2] = (file_path, k)
            pickle.dump(dict_column_dataset, open("dict_col_dataset.pkl", "wb"))

        else:
            print(file_path, inspection_results)



if __name__ == '__main__':
    # try_detective(begin_from="DM1_2018_EHPAD")
    get_detective_analysis(n_datasets=None, n_jobs=8)
