import requests

from .config import BASE_URL

class Downloader:
    def __init__(self):
        self.base_url = BASE_URL

    def _get_full_url(self, year):
        full_url = f'{self.base_url}{year}.7z'

        return full_url

    def _get_archive_name(self, url):
        archive_name = url.split('/')[-1]
        
        return archive_name

    def download(self, year):
        url = self._get_full_url(year)
        file = self._get_archive_name(url)
        
        with requests.get(url, stream=True) as r:
            with open(file, "wb") as f:
                for chunk in r.iter_content(chunk_size=2*1024):
                    f.write(chunk)
        
        return file
