import time
import csv

from pymongo import MongoClient, errors, DESCENDING

from .config import DB_INFO

class DataBase:
    def __init__(self):
        self.client = None
        self.db = None
        self.collection = None

    def initialize(self):
        host = DB_INFO['host']
        port = DB_INFO['port']
        name = DB_INFO['name']

        while True:
            try:
                self.client = MongoClient(f'mongodb://{host}:{port}')
                self.db = self.client[name]
                self.collection = self.db['zno']
                self.collection.drop()
                break
            except errors.ConnectionFailure as e:
                print(f'Connection failure with "{e}" error')
                time.sleep(5)

    def load_csv_to_db(self, csv_file):
        with open(csv_file, 'r') as f:
            reader = csv.reader(f, delimiter=';')

            header = next(reader)

            for row in reader:
                doc={}
                for n in range(0, len(header)):
                    doc[header[n]] = row[n]
                self.collection.insert(doc)

    def get_statistic(self):
        return self.collection.aggregate([
                {'$match' : {'UkrTestStatus' : 'Зараховано'}}, 
                {'$group' : {'_id' : '$REGNAME', 'MaxUkrBal' : {'$max' : '$UkrBall100'}}},
                {'$sort' : {'MaxUkrBal' : DESCENDING}}
            ]
        )

    def close(self):
        self.client.close()