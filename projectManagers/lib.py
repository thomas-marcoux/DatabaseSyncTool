import humanize
from sqlalchemy import create_engine
from os.path import isfile, join, splitext
from os import listdir
import json
from json import JSONDecodeError
from time import time
from tqdm import tqdm

import os
import pandas as pd

ACCEPTED_TYPES = ('.csv', '.xlsx', '.json', '.txt')

def get_engine(settings):
    url = get_engine_url(settings['database'], settings['host'], settings['user'], settings['password'])
    return create_engine(url, echo = False)

def get_engine_url(dbname, host, user, password, dialect="mysql", driver="pymysql", charset="utf8mb4"):
    return f"{dialect}+{driver}://{user}:{password}@{host}/{dbname}?charset={charset}"

def readJSON(path, encoding):
    data = []
    try:
        with open(path, encoding=encoding) as f:
            data = json.load(f)
    # Usually this is a sign of an error in the json file
    # The behavior below will read all lines until the issue arises
    except JSONDecodeError:
        try:
            for line in open(path, 'r'):
                data.append(json.loads(line))
        except JSONDecodeError:
            print(len(data))
            pass
    return [pd.DataFrame(data)]

def readFile(path, settings, id_field = None, existing_ids = None, json_orientation = 'columns', start_chunk=0):
    chunksize = settings['chunksize']
    encoding = settings['encoding'] if 'encoding' in settings else 'UTF-8'
    # print(f"\nReading '{path}' file.")
    # print(f"Chunk size: {chunksize}. Encoding: {encoding}.")
    filetype = splitext(path)[1]
    if filetype == ACCEPTED_TYPES[0]:
        chunks = pd.read_csv(path, chunksize=chunksize, na_filter=False, encoding=encoding)
    if filetype == ACCEPTED_TYPES[1]:
        chunks = [pd.read_excel(path, index_col=0)]
    if filetype == ACCEPTED_TYPES[2]:
        chunks = pd.read_json(path, chunksize=chunksize, orient=json_orientation, encoding=encoding, lines=True)
        # Uncomment below for problematic json
        # chunks = readJSON(path, encoding)
    if filetype == ACCEPTED_TYPES[3]:
        chunks = pd.read_csv(path, chunksize=chunksize, sep="\n", header=None)
    try:
        for i, df in enumerate(chunks):
            if i >= start_chunk:
                yield df[~df[id_field].isin(existing_ids)].copy() if id_field else df
    # If chunks cannot be read, the file will be logged as skipped
    except ValueError as e:
        with open("skippedFiles.txt", "a") as f:
            f.write(f"Skipped {path}\n")
    # print(f"Finished processing {path}.")

def getFiles(directory):
    print(f"Opening '{directory}' folder.")
    dir = os.path.join(os.getcwd(), directory)
    files = [f for f in listdir(dir) if isfile(join(dir, f)) and splitext(join(dir, f))[1] in ACCEPTED_TYPES]
    return files

def readDirectory(directory, settings, id_field = None, existing_ids = None, start_chunk=0):
    progression = tqdm(getFiles(directory))
    for file in progression:
        progression.set_description(desc=f"Opening {file}", refresh=True)
        path = os.path.join(directory, file)
        yield from readFile(path, settings, id_field, existing_ids, start_chunk)

def timer(func):
    def wrap_func(*args, **kwargs):
        t1 = time()
        result = func(*args, **kwargs)
        t2 = time()
        print(f'Function {func.__name__!r} executed in {(t2-t1):.4f}s')
        return result
    return wrap_func

def updateProgress(days_progression, current_day, target_date, day_total, total):
    """Update progress bar to show chronological progression & total packets moved."""
    days_progression.set_description(desc=f"Migrating day {current_day.strftime('%d-%m-%Y')} / {target_date.strftime('%d-%m-%Y')} : [Last day size: {humanize.intcomma(day_total)} / Total : {humanize.intcomma(total)}]", refresh=True)
