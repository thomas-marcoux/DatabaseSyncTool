from .baseManager import BaseManager
from .handlers import PostsHandler
from .lib import get_engine

CHUNK_SIZE = 10000
TWITTER_TABLE = 'twitter'

class TwitterProjectManager(BaseManager):

    def __init__(self, settings):

        test_settings = None
        meta = settings.copy()
        meta['dbs']['cosmos_db']['database'] = TWITTER_TABLE
        meta['target_engine'] = get_engine(meta['dbs']['cosmos_db']) if meta['production'] else get_engine(test_settings)
        meta['chunksize'] = CHUNK_SIZE
        meta['dir'] = 'data'

        dataset_id = 0

        super().__init__(meta)

        self.TableHandlers['posts'] = PostsHandler(meta, dataset_id)
