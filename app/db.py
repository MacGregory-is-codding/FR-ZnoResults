import time
import csv
from typing import Counter

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
                break
            except errors.ConnectionFailure as e:
                print(f'Connection failure with "{e}" error')
                time.sleep(5)

    def load_csv_to_db(self, csv_file):
        with open(csv_file, 'r') as f:
            reader = csv.reader(f, delimiter=';')

            header = next(reader)

            for row in reader:
                c = 0
                doc={}
                inner_arr={}
                for n in range(0, len(header)):
                    c += 1
                    if row[n] != 'null':
                        if c > 15:
                            inner_arr[header[n]] = row[n]
                        else:
                            doc[header[n]] = row[n]
                doc['results'] = [].append(inner_arr)
                try:
                    self.collection.insert_one(doc)
                    #print(doc)
                except Exception as e:
                    print(f'Insert error:\n{doc}\n{e}')

    def get_statistic(self):
        return self.collection.aggregate([
                {'$match'  : {'results.UkrTestStatus' : 'Зараховано'}}, 
                {'$unwind' : '$results'},
                {'$group'  : {'_id' : '$REGNAME', 'MaxUkrBal' : {'$max' : '$results.UkrBall100'}}},
                {'$sort'   : {'MaxUkrBal' : DESCENDING}}
            ]
        )

    def close(self):
        self.client.close()