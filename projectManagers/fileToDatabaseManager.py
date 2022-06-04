from .baseManager import BaseManager
from .handlers import FileToDatabaseHandler, TrackerRelationShipHandler
from .lib import get_engine

class FileToDatabaseProjectManager(BaseManager):

    def __init__(self, settings):

        test_settings = None
        meta = settings.copy()
        meta['chunksize'] = 10000
        meta['target_engine'] = get_engine(meta['dbs']['cosmos_nebula']) if meta['production'] else get_engine(test_settings)
        meta['tracker_id'] = 228

        # meta['encoding'] = 'utf-8'
        # meta['encoding'] = 'latin-1'
        # meta['encoding'] = 'cp1252'

        # meta['video_file'] = 'The_canada_files_Videos.csv'
        # meta['channel_file'] = 'The_canada_files_Channels.csv'

        super().__init__(meta)

        self.TableHandlers['1'] = FileToDatabaseHandler(meta, 'challenge_data_1', file='challenge_data_1.csv')
        # self.TableHandlers['2'] = FileToDatabaseHandler(meta, 'challenge_data_2', file='challenge_data_2.csv')
        # self.TableHandlers['3'] = FileToDatabaseHandler(meta, 'challenge_data_3', file='challenge_data_3.csv')

        # self.TableHandlers['videos'] = FileToDatabaseHandler(meta, 'channels_videos', file='Fox_BBC_Videos.csv')
        # self.TableHandlers['comments'] = FileToDatabaseHandler(meta, 'youtube_comments', dir='Fox_BBC_Comments_chunked', id_field='comment_id')
        # self.TableHandlers['comments'] = FileToDatabaseHandler(meta, 'youtube_comments', file='Nihal_data_Comments.csv', id_field='comment_id')
        # self.TableHandlers['channels'] = FileToDatabaseHandler(meta, 'channels', file='The_canada_files_Channels.csv')
        # self.TableHandlers['channels_daily'] = FileToDatabaseHandler(meta, 'channels_daily', file='The_canada_files_Channels_stats.csv')
        # self.TableHandlers['comments'] = FileToDatabaseHandler(meta, 'comments', file='The_canada_files_Comments.csv')

        # self.TableHandlers['related_videos'] = FileToDatabaseHandler(meta, 'related_videos', file='The_canada_files_Related_Videos.json')
        # self.TableHandlers['videos'] = FileToDatabaseHandler(meta, 'videos', file='The_canada_files_Videos.csv', id_field='video_id')
        # self.TableHandlers['videos_daily'] = FileToDatabaseHandler(meta, 'videos_daily', file='The_canada_files_Videos_stats.csv')
        # self.TableHandlers['tracker_relationship'] = TrackerRelationShipHandler(meta)
