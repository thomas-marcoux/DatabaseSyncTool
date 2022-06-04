from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.inspection import inspect
from sqlalchemy import Column, String, Integer, Float, ForeignKey, DateTime, and_

from datetime import timedelta
import humanize
import pandas as pd
from tqdm import tqdm

# Ignores Duplicate warnings for TrackerRelationship entries
import pymysql
import warnings
warnings.filterwarnings(
  action="ignore", 
  message=".*Duplicate entry.*", 
  category=pymysql.Warning
)

Base = declarative_base()

from ..lib import updateProgress
from .baseHandler import BaseHandler
class YoutubeHandler(BaseHandler):

    def __init__(self, meta):
        self.meta = meta
        self.__tablename__ = self.Mapper.__tablename__

    def build(self, **kwargs):
        return self.Mapper(**kwargs)

    def getData(self):
        table = self.Mapper
        source_engine = self.meta['source_engine']
        source_session = self.meta['source_session']
        target_date_field = 'modified_to_db_time'
        update_field = getattr(table, target_date_field)
        total = 0
        # Determine target day - the last day for which data was collected in source DB
        target_entry = source_session.query(table).order_by(update_field.desc()).first()
        if target_entry is None:
            print(f'No data found.')
            return
        target_date = getattr(target_entry, target_date_field).replace(second=0, hour=0, minute=0)
        # If provided, start from last time tracker was updated
        if 'last_update' in self.meta and self.meta['last_update'] is not None:
            last_date = self.meta['last_update']
        # Otherwise, start from oldest source entry
        else:
            latest_entry = source_session.query(table).order_by(update_field.asc()).first()
            last_date = getattr(latest_entry, target_date_field).replace(second=0, hour=0, minute=0)
            print(f'No last_date found in the tracker. Updating since the beginning of collection ({last_date})')
        if target_date < last_date:
            print('Table is up to date.')
            return
        # Yield daily data for each day until target day is reached
        days_progression = tqdm(pd.date_range(last_date, target_date, freq='d'))
        day_total = 0
        for current_day in days_progression:
            updateProgress(days_progression, current_day, target_date, day_total, total)
            low_bound_date = current_day.strftime('%Y-%m-%d')
            high_bound_date = (current_day + timedelta(days=1)).strftime('%Y-%m-%d')
            query = source_session.query(table).filter(and_(update_field >= low_bound_date, update_field < high_bound_date))
            day_total = 0
            chunks = pd.read_sql(query.statement, con=source_engine, chunksize=self.meta['chunksize'])
            pks = [pk for pk in inspect(table).primary_key]
            count_query = source_session.query(*pks).filter(and_(update_field >= low_bound_date, update_field < high_bound_date))
            try:
                day_count = count_query.count()
                chunks_progression = tqdm(chunks, leave=False, total=(day_count // self.meta['chunksize']), desc=f'Packets (total {humanize.intcomma(day_count)})')
            except OperationalError:
                chunks_progression = tqdm(chunks, leave=False, desc=f'Packets (too large to show total)')
            for df in chunks_progression:
                day_total += df.shape[0]
                total += df.shape[0]
                yield df
            updateProgress(days_progression, current_day, target_date, day_total, total)

class TrackerRelationShipHandler(YoutubeHandler):

    class TrackerRelationship(Base):

        __tablename__ = 'tracker_relationship'

        tracker_relationship_id = Column(Integer, primary_key=True)
        tracker = Column(Integer)
        content_type = Column(String)
        content_id = Column(String)
        keyword = Column(String)

        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    def __init__(self, meta):
        self.Mapper = self.TrackerRelationship
        super().__init__(meta)
        self.video_file = meta['video_file'] if 'video_file' in meta else None
        self.channel_file = meta['channel_file'] if 'channel_file' in meta else None

    def getData(self):
        def getRelationShips(content_type, id_field, table=None, file=None):
            content_in_tracker = self.meta['target_session'].query(self.TrackerRelationship.content_id).filter(and_(self.TrackerRelationship.tracker == self.meta['tracker_id'], self.TrackerRelationship.content_type == content_type))
            content_in_tracker = set([row.content_id for row in content_in_tracker])
            if file:
                chunks = [pd.read_csv(file, usecols=[id_field], na_filter=False)]
            else:
                chunks = pd.read_sql(table, con=self.meta['source_engine'], columns=[id_field], chunksize=self.meta['chunksize'])
            total_rows_added = 0
            progression = tqdm(chunks)
            for df in progression:
                df['tracker'] = self.meta['tracker_id']
                df['content_type'] = content_type
                df = df.rename(columns={id_field: 'content_id'})
                df = df[~df['content_id'].isin(content_in_tracker)]
                total_rows_added += df.shape[0]
                progression.set_description(desc=f'Tracker currently has {len(content_in_tracker)} {content_type}s. [Adding {df.shape[0]} rows / Total {total_rows_added}]', refresh=True)
                yield df
        print("Video relationships.")
        if self.video_file is None:
            yield from getRelationShips('video', VideosHandler.Videos.video_id.key, table=VideosHandler.Videos.__tablename__)
        else:
            yield from getRelationShips('video', VideosHandler.Videos.video_id.key, file=self.video_file)
        print("Channel relationships.")
        if self.channel_file is None:
            yield from getRelationShips('channel', ChannelsHandler.Channels.channel_id.key, table=ChannelsHandler.Channels.__tablename__)
        else:
            yield from getRelationShips('channel', ChannelsHandler.Channels.channel_id.key, file=self.channel_file)

class VideosHandler(YoutubeHandler):

    class Videos(Base):

        __tablename__ = 'videos'

        video_id = Column(String, primary_key=True)
        video_title = Column(String)
        title_sentiment = Column(Float)
        title_toxicity = Column(Float)
        transcript = Column(String)
        transcript_sentiment = Column(Float)
        transcript_toxicity = Column(Float)
        category = Column(String)
        duration = Column(Integer)
        published_date = Column(DateTime)
        thumbnails_medium_url = Column(String)
        description = Column(String)
        description_sentiment = Column(Float)
        description_toxicity = Column(Float)
        channel_id = Column(String)
        source_id = Column(String)
        modified_to_db_time = Column(DateTime)

        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    def __init__(self, meta):
        self.Mapper = self.Videos
        super().__init__(meta)

class CommentsHandler(YoutubeHandler):

    class Comments(Base):

        __tablename__ = 'comments'

        comment_id = Column(String, primary_key=True)
        commenter_name = Column(String)
        commenter_id = Column(String)
        comment_displayed = Column(String)
        comment_original = Column(String)
        likes = Column(Integer)
        total_replies = Column(Integer)
        published_date = Column(DateTime)
        updated_date = Column(DateTime)
        reply_to = Column(String)
        video_id = Column(String)
        sentiment = Column(Float)
        toxicity = Column(Float)
        modified_to_db_time = Column(DateTime)

        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    def __init__(self, meta):
        self.Mapper = self.Comments
        super().__init__(meta)

class ChannelsHandler(YoutubeHandler):

    class Channels(Base):

        __tablename__ = 'channels'

        channel_id = Column(String, primary_key=True)
        channel_title = Column(String)
        title_sentiment = Column(Float)
        title_toxicity = Column(Float)
        thumbnails_medium_url = Column(String)
        description = Column(String)
        description_sentiment = Column(Float)
        description_toxicity = Column(Float)
        joined_date = Column(DateTime)
        location = Column(String)
        language = Column(String)
        modified_to_db_time = Column(DateTime)

        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    def __init__(self, meta):
        self.Mapper = self.Channels
        super().__init__(meta)

class VideosDailyHandler(YoutubeHandler):

    class VideosDaily(Base):

        __tablename__ = 'videos_daily'

        video_id = Column(String, primary_key=True)
        total_views = Column(Integer)
        total_likes = Column(Integer)
        total_dislikes = Column(Integer)
        total_comments = Column(Integer)
        extracted_date = Column(DateTime, primary_key=True)
        videos_daily_views = Column(Integer)
        videos_daily_likes = Column(Integer)
        videos_daily_dislikes = Column(Integer)
        videos_daily_comments = Column(Integer)
        modified_to_db_time = Column(DateTime)

        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    def __init__(self, meta):
        self.Mapper = self.VideosDaily
        super().__init__(meta)

class ChannelsDailyHandler(YoutubeHandler):

    class ChannelsDaily(Base):

        __tablename__ = 'channels_daily'

        channel_id = Column(String, primary_key=True)
        total_views = Column(Integer)
        total_subscribers = Column(Integer)
        total_videos = Column(Integer)
        extracted_date = Column(DateTime, primary_key=True)
        channels_daily_views = Column(Integer)
        channels_daily_subscribers = Column(Integer)
        channels_daily_videos = Column(Integer)
        modified_to_db_time = Column(DateTime)

        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    def __init__(self, meta):
        self.Mapper = self.ChannelsDaily
        super().__init__(meta)

class RelatedVideosHandler(YoutubeHandler):

    class RelatedVideos(Base):

        __tablename__ = 'related_videos'

        video_id = Column(String, primary_key=True)
        parent_video = Column(String, primary_key=True)
        title = Column(String)
        title_sentiment = Column(Float)
        title_toxicity = Column(Float)
        transcript = Column(String)
        transcript_sentiment = Column(Float)
        transcript_toxicity = Column(Float)
        thumbnails_medium_url = Column(String)
        published_date = Column(DateTime)
        channel_id = Column(String)
        channel_title = Column(String)
        channel_title_sentiment = Column(Float)
        channel_title_toxicity = Column(Float)
        modified_to_db_time = Column(DateTime)

        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    def __init__(self, meta):
        self.Mapper = self.RelatedVideos
        super().__init__(meta)

class Tracker(Base):

    __tablename__ = 'tracker'

    tracker_id = Column(Integer, primary_key=True)
    content_last_updated = Column(DateTime)
