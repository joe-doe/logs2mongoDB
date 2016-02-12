from pymongo import (
    MongoClient,
    errors
)
from logs2mongoDB import Log2MongoDB


class MongoDB(object):

    mongo_client = None
    mongodb = None 

    def __init__(self, uri, database):
        """
        Connect to mongo database
        """
        try:
            self.mongo_client = MongoClient(uri)
            self.mongodb = self.mongo_client[database]
            print "Connected successfully to: ".format(database)
        except errors.ConnectionFailure, e:
            print "Could not connect to database: %s" % e

