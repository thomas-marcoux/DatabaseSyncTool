from sqlalchemy.orm import sessionmaker
from datetime import datetime

class BaseManager():
    TableHandlers = {}

    def __init__(self, meta):
        self.meta = meta
        Session = sessionmaker()
        Session.configure(bind=meta['target_engine'])
        self.target_session = Session()
        self.meta['target_session'] = self.target_session

    def __del__(self):
        self.target_session.close()

    def __len__(self):
        return len(self.TableHandlers)

    def __iter__(self):
        for table in self.TableHandlers:
            yield self.TableHandlers[table]

    def run(self):
        for handler in self:
            self.transfer(handler)

    def transfer(self, handler):
        print(f"Starting Table {handler.getTableName()} at {datetime.now()}")
        with self.target_session.no_autoflush:
            for df in handler.getData():
                df = handler.format_data(df)
                dict_items = df.to_dict(orient='records')
                items = [handler.build(**kwargs) for kwargs in dict_items]
                handler.execute(self.target_session, items, dict_items)
            # Execute post-merge operations, if any
            handler.postOperations(self.target_session)

