import csv
import os

from app.downloader import Downloader
from app.extracter import Extracter
from app.db import DataBase

from app.config import YEARS, STATISTIC_CSV

def write_statistic_to_csv(cursor):
    with open(STATISTIC_CSV, 'w') as f:
        header = ['RegName', 'MaxUkrBal100']
        writer = csv.writer(f)
        writer.writerow(header)

        for row in cursor:
            writer.writerow(list(row.values()))



def main():
    downloader = Downloader()
    extracter = Extracter()
    db = DataBase()

    archives = list()
    csv_files = list()

    for year in YEARS:
        print(f'Starting to download ZNO data for {year} year.')
        archive = downloader.download(year)
        print(f'{archive} is successfully downloaded.')
        archives.append(archive)

    for archive in archives:
        csv_file = extracter.extract(archive)
        print(f'{csv_file} is successfully extracted from {archive}.')
        os.remove(archive)
        print(f'{archive} is successfully remowed.')
        clean_csv_file = extracter.clean(csv_file)
        print(f"{csv_file} is successfully cleaned. \n (cp1251 -> utf-8, ',' -> '.')")
        csv_files.append(clean_csv_file)

    db.initialize()
    print('Successfully connected to database.')
    
    for csv_file in csv_files:
        db.load_csv_to_db(csv_file)
        print(f'Data from {csv_file} is successfully loaded to database.')
        os.remove(csv_file)
        print(f'{csv_file} is successfully remowed.')

    statistic = db.get_statistic()
    write_statistic_to_csv(statistic)
    print('Statistic is successfully loaded to csv file.')

    db.close()
    print('Database is closed.')

if __name__ == "__main__":
    main()
