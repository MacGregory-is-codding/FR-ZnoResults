import os
import re

import py7zr

class Extracter:
    def _get_csv_name(self, archive_name):
        csv_file = archive_name.split('.')[0][-4:]
        return f'Odata{csv_file}File.csv'

    def extract(self, archive_name):
        csv_file = self._get_csv_name(archive_name)

        with py7zr.SevenZipFile(archive_name, 'r') as a:
            a.extract(targets=[csv_file])

        return csv_file

    def clean(self, csv_file):
        path_to_clean_csv = 'Clean' + csv_file

        with open(csv_file, 'r', encoding='cp1251') as input_f:
            with open(path_to_clean_csv, 'w') as output_f:
                for line in input_f:
                    output_f.write(re.sub(',', '.', line))

        os.remove(csv_file)

        return path_to_clean_csv