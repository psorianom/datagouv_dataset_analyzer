import glob
import shutil


def stringio2file(stringio, file_path):
    with open(file_path, 'w') as fd:
        stringio.seek(0)
        shutil.copyfileobj(stringio, fd)


def get_files(data_path, ext="csv"):
    return glob.glob(data_path + "/*.{}".format(ext))

