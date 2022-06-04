from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle
import os.path
import io
import numpy as np
import pandas as pd
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer, Boolean, DateTime

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

Base = declarative_base()

from .baseHandler import BaseHandler
class COVIDHandler(BaseHandler):

    def __init__(self, meta, file_id):
        self.meta = meta
        self.file_id = file_id
        self.service = self.get_service()

    def execute(self, session, items, _):
        for item in items:
            session.merge(item)
        if self.meta['commit']:
            session.commit()

    def getData(self):
        # Call the Sheets API
        SAMPLE_RANGE_NAME = 'Sheet1'
        sheet = self.service.spreadsheets()
        result = sheet.values().get(spreadsheetId=self.file_id, range=SAMPLE_RANGE_NAME).execute()
        values = result.get('values', [])
        if not values:
            print('No data found.')
            yield None
        else:
            yield pd.DataFrame(values[1:], columns=values[0])

    def get_service(self):
        creds = None
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.pickle', 'wb') as token:
                pickle.dump(creds, token)
        return build('sheets', 'v4', credentials=creds)


class MisinfoHandler(COVIDHandler):

    # Keep track of titles to delete old entries
    incoming_titles = []

    class MisinfoData(Base):

        __tablename__ = 'misinformation_data'

        title = Column(String, primary_key=True)
        category = Column(String)
        summary = Column(String)
        debunking_date = Column(DateTime)
        debunking_link = Column(String)
        debunking_name = Column(String)
        country = Column(String)
        misinfo_sources = Column(String)
        keywords = Column(String)
        flagged = Column(String)
        flag_description = Column(String)

        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    def __init__(self, meta, file_id):
        super().__init__(meta, file_id)
        self.__tablename__ = self.MisinfoData.__tablename__
    
    def build(self, **kwargs):
        item = self.MisinfoData(**kwargs)
        # Keep track of titles to delete any that are not matching
        self.incoming_titles.append(item.title)
        return item

    def postOperations(self, session):
        # Deletion
        current_entries = session.query(self.MisinfoData.title).all()
        current_titles = [item[0] for item in current_entries]
        titles_to_delete = np.setdiff1d(current_titles, self.incoming_titles).tolist()
        session.query(self.MisinfoData).filter(self.MisinfoData.title.in_(titles_to_delete)).delete(synchronize_session=False)
        if self.meta['commit']:
            session.commit()

class TipsHandler(COVIDHandler):

    class Tips(Base):

        __tablename__ = 'tips'

        id = Column(Integer, primary_key=True)
        tip_text = Column(String)
        tip_source = Column(String)
        tip_link = Column(String)

        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    def __init__(self, meta, file_id):
        super().__init__(meta, file_id)
        self.__tablename__ = self.Tips.__tablename__

    def build(self, **kwargs):
        return self.Tips(**kwargs)
