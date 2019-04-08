import subprocess
import pandas as pds
from joblib import Parallel, delayed
from tqdm import tqdm


def downloader(url, id):

    p = subprocess.Popen(["wget", "-N", "-O", "/data/datagouv/csv/{}.csv".format(id[2:-2]), url])
    p.communicate()  # now wait plus that you can send commands to process

def main():
    df = pds.read_csv("all_datagouv_csv.csv")

    urls = df.url.values[:1000]
    ids = df._id.values[:1000]
    assert(len(urls) == len(ids))
    Parallel(n_jobs=1)(
        delayed(downloader)(url,id) for url,id in zip(urls, ids))

if __name__ == '__main__':
    main()
