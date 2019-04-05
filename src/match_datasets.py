import io
from collections import defaultdict

import pandas as pd
import subprocess
import numpy as np
import logging

import os

import tqdm
import glob
from csvkit.utilities.csvclean import CSVClean


ENCODINGS = ["latin1", "ascii", "utf-16-be"]
FNULL = open(os.devnull, 'w')
logger = logging.getLogger()


def find_urls():
    not_popular_data = pd.read_csv("./data/datasets.csv", delimiter=";")
    all_resources = pd.read_csv("./data/resources-2019-02-22-12-21.csv", delimiter=";")

    unpop_ids = not_popular_data._id.dropna().values
    all_res_ids = all_resources["dataset.id"].dropna().values
    intersect = np.intersect1d(unpop_ids, all_res_ids)
    unpop_df = all_resources[all_resources["dataset.id"].isin(intersect)]
    filtered_df = unpop_df[["dataset.id", "url"]].dropna()
    return filtered_df


def download_urls(filtered_df: pd.DataFrame):

    downloaded_extensions = defaultdict(int)
    num_unpop_data = len(filtered_df)
    logger.info("Number of 'unpopular datasets' found: {}".format(num_unpop_data))
    for _, row in tqdm.tqdm(list(filtered_df.iterrows())[:10000]):
        try:
            dir_path = "./data/unpop_datasets/{}".format(row["dataset.id"])
            if not os.path.isdir(dir_path):
                os.mkdir("./data/unpop_datasets/{}".format(row["dataset.id"]))
            subprocess.run(["wget", "-N", "-T", "10", "-P",
                            "./data/unpop_datasets/{}/".format(row["dataset.id"]), row["url"]], stdout=FNULL)
            logger.info("Downloaded {}".format(row["url"]))
            extension = row["url"].split(".")[-1]
            if len(extension) <= 4:
                downloaded_extensions[extension] += 1
        except Exception as e:
            logger.error(e)

    logger.info(downloaded_extensions)

def clean_csv(csv_path, dry_run=True, encoding=None):
    args = [csv_path]
    if dry_run:
        args.append("-n")
    if encoding:
        args.extend(["-e", encoding])


    output_stream = io.StringIO()

    utility = CSVClean(args, output_stream)
    try:
        utility.run()
        return 0
    except UnicodeDecodeError as ud:
        logger.error(ud)
        for enc in ENCODINGS:
            exit_code = clean_csv(csv_path, dry_run=False, encoding=enc)
            if exit_code == 0:
                break
    finally:
        print("soy yo")


def clean_csvs(csv_path):
    for csv in csv_path:
        if "_cleaned" in csv or "out" in csv:
            continue
        clean_csv(csv, dry_run=False, encoding=None)
        pass



def get_files(data_path, ext="csv"):
    return glob.iglob(data_path + "/*/" + "*.{}".format(ext))


if __name__ == '__main__':
    list_urls = find_urls()
    download_urls(list_urls)
    # list_csvs = get_files(data_path="./data/unpop_datasets")
    # clean_csvs(list_csvs)