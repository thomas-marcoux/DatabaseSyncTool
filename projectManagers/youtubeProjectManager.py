from datetime import datetime
from sqlalchemy.orm import sessionmaker
from .baseManager import BaseManager
from .handlers import ChannelsHandler, ChannelsDailyHandler, CommentsHandler, RelatedVideosHandler, VideosHandler, VideosDailyHandler, TrackerRelationShipHandler
from .handlers import Tracker
from .lib import get_engine

CHUNK_SIZE = 10000

TRACKERS = {
    # 207: 'australian_covid',
    # 437: 'australian_dod',
    # 438: 'behaviors_on_youtube',
    # 193: 'covid_disinfo',
    # 194: 'covid_keywords',
    # 439: 'minerva',
    231: 'tracker_231',
    # 237: 'tracker_237',
    # 440: 'vaccination',
}

class YouTubeProjectManager(BaseManager):

    def __init__(self, settings):

        self.source_session = None
        meta = settings.copy()
        meta['target_engine'] = get_engine(meta['dbs']['vt_db'])
        meta['chunksize'] = CHUNK_SIZE
        super().__init__(meta)

    def run(self):
        for id in TRACKERS:
            start_time = datetime.now()
            last_update = self.get_last_update_date(id)
            message = f"Syncing tracker {id}: {TRACKERS[id]} - " + (f"Last updated on {last_update}" if last_update else "First time running")
            print(message)
            self.meta['tracker_id'] = id
            self.meta['last_update'] = last_update
            db_name = TRACKERS[id]
            self.set_source_engine(db_name)
            self.TableHandlers['channels'] = ChannelsHandler(self.meta)
            self.TableHandlers['channels_daily'] = ChannelsDailyHandler(self.meta)
            self.TableHandlers['videos'] = VideosHandler(self.meta)
            self.TableHandlers['tracker_relationship'] = TrackerRelationShipHandler(self.meta)
            self.TableHandlers['videos_daily'] = VideosDailyHandler(self.meta)
            self.TableHandlers['related_videos'] = RelatedVideosHandler(self.meta)
            self.TableHandlers['comments'] = CommentsHandler(self.meta)
            super().run()
            self.set_last_update_date(id, start_time)
            delta = datetime.now() - start_time
            print(f"Tracker updated (took {delta.seconds/86400:.0f}d {(delta.seconds % 86400) / 3600:.0f}h {(delta.seconds % 3600) / 60:.0f}m {delta.seconds % 60}s)")

    def set_source_engine(self, db_name):
        if self.source_session:
            self.source_session.close()
        self.meta['dbs']['cosmos_db']['database'] = db_name
        self.meta['source_engine'] = get_engine(self.meta['dbs']['cosmos_db'])
        Session = sessionmaker()
        Session.configure(bind=self.meta['source_engine'])
        self.source_session = Session()
        self.meta['source_session'] = self.source_session

    def __del__(self):
        self.source_session.close()

    def get_last_update_date(self, tracker_id):
        date = self.meta['target_session'].query(Tracker.content_last_updated).filter(Tracker.tracker_id == tracker_id).first()[0]
        return date

    def set_last_update_date(self, tracker_id, start_time):
        date = start_time.replace(second=0, hour=0, minute=0).strftime('%Y-%m-%d %H:%M:%S')
        self.meta['target_session'].query(Tracker).filter(Tracker.tracker_id == tracker_id).update({Tracker.content_last_updated: date})
        if self.meta['commit']:
            self.meta['target_session'].commit()
