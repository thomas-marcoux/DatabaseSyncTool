import pandas as pd
import json
from os.path import join
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import Column, String, Integer, Boolean, DateTime, JSON, Float, ForeignKey

Base = declarative_base()

OUTPUT_IDS = True

from .baseHandler import BaseHandler
from .twitterUtils import Hydrator
from .twitterUtils2 import Hydrator2
from ..lib import getFiles, readDirectory, readFile, timer

class TwitterHandler(BaseHandler):
    pass

class PostsHandler(TwitterHandler):

    class Posts(Base):

        # Twitter v2 Migration guide - https://developer.twitter.com/en/docs/twitter-api/tweets/search/migrate
        # Unofficial Migration guide - https://dev.to/twitterdev/understanding-the-new-tweet-payload-in-the-twitter-api-v2-1fg5
        # v1 Tweet object - https://developer.twitter.com/en/docs/twitter-api/v1/data-dictionary/object-model/tweet
        # v2 Tweet object - https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/tweet
        # v2 All Fields - https://developer.twitter.com/en/docs/twitter-api/fields
        # v2 Examples:
        # https://developer.twitter.com/en/docs/twitter-api/expansions
        # https://developer.twitter.com/en/docs/twitter-api/tweets/lookup/quick-start

        __tablename__ = 'posts'

        id = Column(String, primary_key=True)
        source_id = Column(Integer, primary_key=True)
        created_at = Column(DateTime)
        # geo_source = Column(String)
        truncated = Column(Boolean)
        text = Column(String)
        text_sentiment = Column(Float)
        # text_toxicity = Column(Float)
        lang = Column(String)
        user = Column(String, ForeignKey('users.id')) # author_id in v2
        retweet_count = Column(Integer) # In "public_metrics" in v2
        favorite_count = Column(Integer) # Renamed to 'like_count' in v2 - in "public_metrics"
        # quote_count = Column(Integer) # In "public_metrics" in v2
        # reply_count = Column(Integer) # In "public_metrics" in v2
        retweet_id_str = Column(String)
        is_quote_status = Column(Boolean)
        quoted_status_id_str = Column(String)
        in_reply_to_status_id_str = Column(String)
        in_reply_to_user_id_str = Column(String) # in_reply_to_user_id in v2
        in_reply_to_screen_name = Column(String) # Unavailable in v2
        possibly_sensitive = Column(Boolean)
        # JSON Fields - Specifies the use of SQL NULL as opposed to JSON "null"
        entities = Column(JSON(none_as_null=True))
        coordinates = Column(JSON(none_as_null=True)) # In "geo" field in v2
        place = Column(JSON(none_as_null=True)) # In "geo" field in v2

        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    def __init__(self, meta, dataset_id):
        super().__init__(meta)
        self.__tablename__ = self.Posts.__tablename__
        self.dataset_id = dataset_id
        self.usersHandler = UsersHandler(meta)
        self.hydrator = Hydrator()

    def getData(self):
        directory = self.meta['dir']
        files = getFiles(directory)
        if OUTPUT_IDS:
            tweet_ids = []
            for df in readDirectory(directory, self.meta):
                tweet_ids += df.iloc[:, 0].tolist()
            json_object = json.dumps(tweet_ids)
            with open("tweet_ids.json", "w") as outfile:
                outfile.write(json_object)
            return
        start_chunk = 0
        for file in files:
            self.total_read = 0
            self.total_unique_ids  = 0
            self.total_fetched = 0
            self.total_duplicates = 0
            path = join(directory, file)
            for df in readFile(path, settings=self.meta, start_chunk=start_chunk):
                tweet_ids = df.iloc[:, 0].tolist()
                yield self.process(tweet_ids, df)
            start_chunk = 0
            print(f"Read a total of {self.total_read} seeds, queried {self.total_unique_ids} unique ids.")
            print(f"Fetched {self.total_fetched} tweet objects back and found {self.total_duplicates} duplicates (removed).")
            print(f"{self.total_fetched - self.total_duplicates} total tweets")

    # This function will merge tweet request results and data from source files
    def append_func(self, tweets_df, file_df):
        file_df.columns = ['id', 'text_sentiment']
        return tweets_df.merge(file_df, left_on='id', right_on='id', how='left')

    def process(self, tweet_ids, file_df=None):
        unique_ids = set(tweet_ids)
        tweets_generator = self.hydrator.hydrate(unique_ids)
        tweets_df = pd.DataFrame(tweets_generator)
        if tweets_df.shape[0] == 0:
            return tweets_df
        tweets_received = tweets_df.shape[0]
        # Add dataset key
        tweets_df['source_id'] = self.dataset_id
        # If source file data needs to be appended to the result
        if file_df is not None:
            tweets_df = self.append_func(tweets_df, file_df)
        # Filtering duplicates as there are cases of duplicate tweets after appending file data
        tweets_df.drop_duplicates(subset='id', inplace=True)
        self.total_read += len(tweet_ids)
        self.total_unique_ids += len(unique_ids)
        self.total_fetched += tweets_received
        self.total_duplicates += tweets_received - tweets_df.shape[0]
        # Extracting users
        self.users_df = pd.DataFrame(tweets_df['user'].tolist())
        return tweets_df

    def format_data(self, df):
        # Drop integer 'id' column and replace with str version
        df = df.drop(columns=["id"])
        df = df.rename(columns={"id_str": "id", "full_text": "text"})
        df['created_at'] = pd.to_datetime(df.created_at, format="%a %b %d %H:%M:%S %z %Y")
        # Initialize nullable fields
        df['quoted_status_id_str'] = None
        df['possibly_sensitive'] = None
        # Extract retweet id
        df['retweet_id_str'] = df.apply(lambda row : row['retweeted_status']['id_str'] if 'retweeted_status' in row and isinstance(row['retweeted_status'], dict) else None, axis=1)
        # df['retweet_id_str'] = df.apply(lambda row : row['retweeted_status']['id_str'] if 'retweeted_status' in row else None, axis=1)
        df['user'] = df.apply(lambda row : row['user']['id_str'], axis=1)
        return super().format_data(df)

    def execute(self, session, items):
        # First insert users
        self.users_df = self.usersHandler.format_data(self.users_df)
        users = [self.usersHandler.build(**kwargs) for kwargs in self.users_df.to_dict(orient='records')]
        self.usersHandler.execute(session, users)
        # Attempting without ID pre-loading, merging duplicates instead
        # items = [item for item in items if item.id not in self.existing_ids]
        # Insert tweets
        super().execute(session, items)
        # session.add_all(items)
        # if self.meta['commit']:
        #     session.commit()
    
    def build(self, **kwargs):
        return self.Posts(**kwargs)

class UsersHandler(TwitterHandler):

    class Users(Base):

        __tablename__ = 'users'

        id = Column(String, primary_key=True)
        created_at = Column(DateTime)
        name = Column(String)
        screen_name = Column(String)
        location = Column(String)
        url = Column(String)
        description = Column(String)
        verified = Column(Boolean)
        followers_count = Column(Integer)
        friends_count = Column(Integer)
        listed_count = Column(Integer)
        favourites_count = Column(Integer)
        statuses_count = Column(Integer)
        geo_enabled = Column(Boolean)

        tweets = relationship("Posts")

        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    def __init__(self, meta):
        super().__init__(meta)
        self.__tablename__ = self.Users.__tablename__
        # Attempting without ID pre-loading, merging duplicates instead
        # self.existing_ids = list(meta['session'].query(self.Users.id))
        # self.existing_ids = {id[0] for id in self.existing_ids}
    
    def build(self, **kwargs):
        return self.Users(**kwargs)

    def format_data(self, df):
        df.drop_duplicates("id", inplace=True)
        df = df.drop(columns=["id"])
        df = df.rename(columns={"id_str": "id"})
        df['created_at'] = pd.to_datetime(df.created_at, format="%a %b %d %H:%M:%S %z %Y")
        return super().format_data(df)

    def execute(self, session, items):
        # Attempting without ID pre-loading, merging duplicates instead
        # Remove this method if successful
        super().execute(session, items)

        # items = [item for item in items if item.id not in self.existing_ids]
        # session.bulk_save_objects(items)
        # Append added ids to existing ones
        # self.existing_ids |= {item.id for item in items}
        # if self.meta['commit']:
        #     session.commit()
