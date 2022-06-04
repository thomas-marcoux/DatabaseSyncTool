from dateutil.parser import parse
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import Table
from tqdm import tqdm
import json
import numpy as np
import pandas as pd
import re

from .baseHandler import BaseHandler
from ..lib import readDirectory, readFile

class FileToDatabaseHandler(BaseHandler):

    def __init__(self, meta, table, file=None, dir=None, id_field=None):
        self.meta = meta
        self.source_file = file
        self.dir = dir
        self.id_field = id_field
        self.existing_ids = list()
        self.table = getTable(table, meta['target_engine'])

    def build(self, **kwargs):
        return self.table(**kwargs)

    def getTableName(self):
        return self.table.__table__.name

    def getData(self):
        if self.id_field:
            self.loadIds()
        if self.source_file:
            yield from readFile(self.source_file, self.meta, self.id_field, self.existing_ids)
        if self.dir:
            yield from readDirectory(self.dir, self.meta, self.id_field, self.existing_ids)
        del self.existing_ids
        return

    def format_data(self, df):
        if df.shape[0] == 0:
            return df
        #Detect date and json columns
        for col in df.select_dtypes(include=['object']):
            sample_val = df[col].loc[~df[col].isnull()].iloc[0]
            try:
                # Check if string is int or float
                if re.match("^[0-9. ]+$", sample_val):
                    pass
                elif is_date(sample_val):
                    df[col] = pd.to_datetime(df[col])
                elif is_json(sample_val):
                    df[col] = df.apply(lambda row : json.loads(row[col]), axis=1)
            except (ValueError, TypeError):
                pass
        return super().format_data(df)

    def loadIds(self):
        print(f"Querying existing {self.id_field}...")
        pk = getattr(self.table, self.id_field)
        self.existing_ids = list()
        for ids in tqdm(self.meta['target_session'].query(pk).yield_per(1000)):
            self.existing_ids.append(ids)
        self.existing_ids = {id[0] for id in self.existing_ids}

def constructor(self, **kwargs):
    self.__dict__.update(kwargs)

def getTable(table, target_engine):
    Base = declarative_base()
    return type("DynamicTable", (Base, ), {
        "__init__": constructor,
        "__table__": Table(table, Base.metadata, autoload=True, autoload_with=target_engine),
    })

def is_date(string):
    try: 
        parse(str(string))
        return True
    except (ValueError, TypeError):
        return False

def is_json(string):
    try: 
        json.loads(str(string))
        return True
    except ValueError:
        return False
