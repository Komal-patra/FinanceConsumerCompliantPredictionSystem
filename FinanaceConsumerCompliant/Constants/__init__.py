import pymongo
import os
import certifi
from FinanaceConsumerCompliant.Constants.constant import env_var

ca = certifi.where()

mongo_client = pymongo.MongoClient(env_var.mongo_db_url, tlsCAFile=ca)