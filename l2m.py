import time
from db_mongo import MongoDB
from logs2mongoDB import Log2MongoDB

uri = 'mongodb://localhost:27017/'
logs = {
    '/tmp/log1.log': 'log1',
    '/tmp/log2.log': 'log2'}


db = MongoDB(uri, 'mylogs')


if __name__ == "__main__":
	{Log2MongoDB(log_name, db.mongodb, collection_name).start() 
	for log_name, collection_name in logs.items()}

	while True:
		time.sleep(60)
