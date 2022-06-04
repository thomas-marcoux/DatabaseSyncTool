from .baseManager import BaseManager
from .handlers import MisinfoHandler, TipsHandler
from .lib import get_engine


class CovidProjectManager(BaseManager):

    def __init__(self, settings):

        meta = settings.copy()
        production_settings = meta['dbs']['covid_prod']
        test_settings = meta['dbs']['covid_test']
        meta['target_engine'] = get_engine(production_settings) if meta['production'] else get_engine(test_settings)
        misinfo_file_id = '1venAutfE8I-Yja8OzWX4Lv9VHfMhvkvPoTUgCwRJeck'
        tips_file_id = '1urTOZOlHIthihEE1yKf3MP4EDGbMXclg3Wrj1D6SlJY'
        self.TableHandlers['misinfo'] = MisinfoHandler(meta, misinfo_file_id)
        self.TableHandlers['tips'] = TipsHandler(meta, tips_file_id)
        super().__init__(meta)
