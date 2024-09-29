from dataclasses import dataclass
from datetime import datetime
import os

TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")


class EnvironmentVariable:
    mongo_db_url = os.getenv('MONGO_DB_URL')
    print(mongo_db_url)

env_var = EnvironmentVariable()
