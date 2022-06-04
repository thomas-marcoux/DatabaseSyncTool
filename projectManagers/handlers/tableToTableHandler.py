from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import Table
from tqdm import tqdm
import numpy as np
import pandas as pd

from .baseHandler import BaseHandler
class TableToTableHandler(BaseHandler):

    def __init__(self, meta, table, query_pk=False):
        self.meta = meta
        self.existing_ids = list()
        self.query_pk = query_pk
        self.table = getTable(table, meta['target_engine'])

    def build(self, **kwargs):
        return self.table(**kwargs)

    def getTableName(self):
        return self.table.__table__.name

    def getData(self):
        if self.query_pk:
            self.loadIds()
        print(f'Starting table {self.getTableName()}.')
        chunks = pd.read_sql(self.getTableName(), con=self.meta['source_engine'], chunksize=self.meta['chunksize'])
        for df in tqdm(chunks):
            yield df[~df[self.id_field].isin(self.existing_ids)].copy() if self.query_pk else df
        del self.existing_ids
        print(f'Finished processing {self.getTableName()}.')
        return

    def loadIds(self):
        pk = self.table.__mapper__.primary_key[0]
        self.id_field = pk.name
        print(f"Querying existing {self.id_field}...")
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
