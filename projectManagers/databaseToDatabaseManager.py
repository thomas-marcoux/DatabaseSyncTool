from .baseManager import BaseManager
from .handlers import TableToTableHandler
from .lib import get_engine

from sqlalchemy.ext.automap import automap_base

class DatabaseToDatabaseManager(BaseManager):

    def __init__(self, settings):

        meta = settings.copy()
        meta['chunksize'] = 10000

        meta['dbs']['cosmos_1']['database'] = 'blogtrackers'
        meta['source_engine'] = get_engine(meta['dbs']['cosmos_1'])
        meta['target_engine'] = get_engine(meta['dbs']['bt_vm'])

        super().__init__(meta)

        # self.TableHandlers['comments'] = TableToTableHandler(meta, table='comments', query_pk=True)
        # self.TableHandlers['blogsites'] = TableToTableHandler(meta, table='blogsites')

        Base = automap_base()
        Base.prepare(meta['source_engine'], reflect=True)

        relationships = {}

        for t in Base.classes:
            tableName = t.__table__.name
            self.TableHandlers[tableName] = TableToTableHandler(meta, table=tableName)
            relationships[tableName] = []
            relationships[tableName].extend(t.__mapper__.relationships.keys())

        # self.TableHandlers = {'blogsites': self.TableHandlers['blogsites']}

        print(relationships)

        # self.TableHandlers = {'blogposts': TableToTableHandler(meta, table='blogposts', query_pk=True)}
