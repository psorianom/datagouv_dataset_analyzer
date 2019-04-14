import glob
import os
import shutil
from collections import defaultdict


def stringio2file(stringio, file_path):
    with open(file_path, 'w') as fd:
        stringio.seek(0)
        shutil.copyfileobj(stringio, fd)


def get_files(data_path, ext="csv"):
    return glob.glob(data_path + "/*.{}".format(ext))


def get_id(file_name):
    return os.path.basename(file_name)[:-4]


def inverse_type_dict(type_dict: dict):
    dataset_type_dict = defaultdict(dict)
    for type_, datasets in type_dict.items():
        for dataset_id, dataset_col in datasets:
            dataset_type_dict[dataset_id][dataset_col.replace('"', "")] = type_
    return dataset_type_dict