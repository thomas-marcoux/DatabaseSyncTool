import pickle, tempfile, time
from sqlalchemy.exc import IntegrityError, OperationalError, InternalError
from sqlalchemy.orm.exc import StaleDataError
from tqdm import tqdm

class BaseHandler():
    __tablename__ = None

    def __init__(self, meta):
        self.meta = meta
        self.Mapper = None

    def getTableName(self):
        return self.__tablename__

    def get_target_engine(self):
        return self.meta['target_engine']

    def getData(self):
        pass

    def push(self, session):
        session.flush()
        if self.meta['commit']:
            session.commit()

    def format_data(self, df):
        for col in df.select_dtypes(include=['datetime64[ns]', 'datetime64[ns, UTC]']):
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        # Defaults any 'nan' values to 0 for numeric fields. This is because MySQL will not accept 'nan' fields.
        for col in df.select_dtypes(include=['int', 'float']):
            df[col].fillna(0, inplace=True)
        return df

    def execute(self, session, items, dict_items):
        # Bulk insert
        session.add_all(items)
        maxTries = 5
        message = ""
        for i in range(0, maxTries):
            try:
                # Attempts to push bulk insert
                self.push(session)
                return
            # Handles duplicates if 'update' is ON
            except IntegrityError as e:
                if self.meta['update'] is False:
                    raise e
                session.rollback()
                try:
                    # Attempts to mass update
                    session.bulk_update_mappings(self.Mapper, dict_items)
                    self.push(session)
                    return
                # If there was a mix of new and existing rows and mass update fails, split items between inserts and updates
                except StaleDataError as e:
                    session.rollback()
                    added_items = []
                    updated_items = []
                    for item in tqdm(items, desc='Bulk operations failed, updating items individually', leave=False):
                        session.add(item)
                        try:
                            session.flush()
                            added_items.append(item)
                        except IntegrityError:
                            session.rollback()
                            updated_items.append(item)
                        # Try to make OperationalError go up a clause
                        except Exception as e:
                            raise e
                    session.rollback()
                    session.add_all(added_items)
                    updated_items = [vars(item) for item in updated_items]
                    session.bulk_update_mappings(self.Mapper, updated_items)
                    session.flush()
                    if self.meta['commit']:
                        session.commit()
                    return
            # Accounts for server errors
            except (OperationalError, InternalError)  as e:
                message = str(e)
            # Log errors and wait before resuming
            print(f"\nError: {message}\nRetry {i+1}...")
            session.rollback()
            time.sleep(600)
        # If multiple calls to the server fail, log items
        with tempfile.NamedTemporaryFile(suffix='pkl') as f:
            with open("DB_tools.log", "a") as logf:
                    logf.write("{0}\n".format(message))
            print(f"Pickling {len(items)} items to {f.name}")
            pickle.dump(items, f)

    def postOperations(self, session=None):
        pass
