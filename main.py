from datetime import datetime
from projectManagers import YouTubeProjectManager, CovidProjectManager, TwitterProjectManager, FileToDatabaseProjectManager
import yaml

YMLPATH = 'credentials.yml'

def main():
    credentials = None
    with open(YMLPATH, "r") as ymlfile:
        credentials = yaml.load(ymlfile, Loader=yaml.FullLoader)
    settings = {
        # If production is True, save to the specified Live environment
        'production': True,
        # If commit is False no changes are made. For debugging.
        'commit': False,
        # If update is True, any existing DB entry will be updated.
        # If update is False and no logic is defined for duplicate entries an error will be raised.
        # Be careful with updating composite keys that include dates.
        'update': True,
        'dbs': credentials,
    }

    projectManager = YouTubeProjectManager(settings)
    # projectManager = CovidProjectManager(settings)
    # projectManager = TwitterProjectManager(settings)
    # projectManager = DatabaseToDatabaseManager(settings)
    # projectManager = DatabaseToDatabaseManager(settings)
    # projectManager = FileToDatabaseProjectManager(settings)

    # Start timer
    start = datetime.now()

    projectManager.run()
    
    delta = datetime.now() - start
    print(f"Task ran for {delta.seconds/86400:.0f}d {(delta.seconds % 86400) / 3600:.0f}h {(delta.seconds % 3600) / 60:.0f}m {delta.seconds % 60}s.")

if __name__ == "__main__":
    main()
