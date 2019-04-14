'''This script generates a file in order to annotate the columns of CSVs. That is, manually say what a column is about.
Reads CSVs in a folder and output a single file with all the column of said CSVs, the first lines of said CSVs and
a column to annotate each column.

Usage:
    csv_header_annotation.py <i> [options]

Arguments:
    <i>                     An input folder with CSVs to analyze
    --detective DETECT      A tsv file path with the info recovered from previously running csv_detective over the input CSVs folder
    --columns COLS          A pickle file with a dict of csv_detective types as keys and a list of corresponding datasets as values
    --cores CORES           The number of cores to use in a parallel setting [default:2:int]
'''
import pickle

__author__ = 'Pavel Soriano'
__mail__ = 'pavel.soriano@data.gouv.fr'

import logging
from src.utils import get_files, get_id, inverse_type_dict

logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)

import pandas as pd
from argopt import argopt
from joblib import Parallel, delayed

DATA_TYPES = []


def make_xls_file(df, xls_file_name="test"):

    import xlsxwriter
    workbook = xlsxwriter.Workbook('{}.xlsx'.format(xls_file_name))
    worksheet = workbook.add_worksheet()
    header_format = workbook.add_format({
    'border': 1,
    'bg_color': '#C6EFCE',
    'bold': True,
    'text_wrap': True,
    'valign': 'vcenter',
    'indent': 1,
    })

    # Set lenght of columns
    worksheet.set_column(0, len(df.columns), 40)

    # Set dummy column of data for the drop down list
    worksheet.write_column("AA1", DATA_TYPES)
    for idx, col_name in enumerate(df.columns):


        worksheet.write(0, idx, col_name, header_format)
        if col_name == "human_detected":
            worksheet.data_validation(1, idx, len(df), idx, {'validate': 'list',
                                                             'source': '=$AA$1:$AA${}'.format(1+len(df))})

        elif col_name == "sample":
            worksheet.write_column(1, idx, df[col_name].apply(lambda x: '"[{}]"'.format(", ".join(map(str, x)))))
        else:
            worksheet.write_column(1, idx, df[col_name])

    workbook.close()


def columns_extractor(file_path, detective_df=None, datasets_types_dict=None):
    sep = ","
    encoding = "utf8"
    dataset_id = get_id(file_path)

    # Try to get encoding and separator
    if detective_df is not None:
        detected_info = detective_df[detective_df.file_id == dataset_id]
        if len(detected_info):
            sep = detected_info.separator.item()
            if not sep:
                sep = "\t"
            encoding = detected_info.encoding.item()

    # Try to get the columns detected by csv_detective
    detected_columns = {}
    if datasets_types_dict and dataset_id in datasets_types_dict:
        detected_columns = datasets_types_dict[dataset_id]

        # Try to open the file
    try:
        dataset_df = pd.read_csv(file_path, encoding=encoding, sep=sep)
    except Exception as e:
        logger.error(e)
        logger.debug("\t" + file_path)
        return 0

    dataset_dict = dataset_df.apply(lambda x: list(set(x.dropna()))[:10]).to_dict()
    csv_detected_dict = {"csv_detected": [detected_columns[d] if d in detected_columns else ""
                                          for d in dataset_dict.keys()]}

    new_df = {"columns": list(dataset_dict.keys()), "sample": list(dataset_dict.values()),
                  "human_detected": [""] * len(dataset_dict)}
    new_df.update(csv_detected_dict)
    new_df.update({"id":[dataset_id] * len(dataset_dict)})
    return pd.DataFrame(new_df)


def header_analysis(files_paths, n_jobs=2, detective_df=None, datasets_types_dict=None):
    Parallel(n_jobs=n_jobs)(
        delayed(columns_extractor)(path) for path in files_paths)


def header_analysis_single(files_paths, detective_df=None, datasets_types_dict=None):
    df_list = []
    for i, f in enumerate(files_paths):
        df_list.append(columns_extractor(f, detective_df=detective_df, datasets_types_dict=datasets_types_dict))

    # logger.info("{} files succesfully read from {}".format(succes_read, len(files_paths)))
    return df_list

if __name__ == '__main__':
    parser = argopt(__doc__).parse_args()
    files_path = parser.i
    if parser.columns:
        types_datasets_dict = pickle.load(open(parser.columns, "rb"))
        datasets_types_dict = inverse_type_dict(types_datasets_dict)
        DATA_TYPES = sorted(list(types_datasets_dict.keys()))
    else:
        types_datasets_dict = None

    if parser.detective:
        detective_df = pd.read_csv(parser.detective, sep="\t")
    else:
        detective_df = None

    n_jobs = int(parser.cores)

    files = get_files(files_path)[:2]

    if n_jobs == 1:
        listo = header_analysis_single(files, detective_df=detective_df, datasets_types_dict=datasets_types_dict)
    else:
        header_analysis(files, n_jobs=n_jobs, detective_df=detective_df, datasets_types_dict=datasets_types_dict)
    pass
    # make_xls_file(new_df)
