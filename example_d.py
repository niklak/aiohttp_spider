import os
import asyncio
from urllib.parse import urljoin
from downloader import Downloader

BASE_URL = 'https://upload.wikimedia.org/'

PATH = os.path.join(os.path.dirname(__file__), 'media')


def get_urls():
    # Any custom logic that represents list with links
    with open('links.txt') as file:
        return (line.strip() for line in file.readlines())

if __name__ == '__main__':

    links = get_urls()

    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) "
                             "AppleWebKit 537.36 (KHTML, like Gecko) Chrome",
               "Accept": "text/html,application/xhtml+xml,"
                         "application/xml;q=0.9,image/webp,*/*;q=0.8"}

    # USELESS
    exclude_file_path = urljoin(BASE_URL, 'wikipedia/commons/thumb/')

    downloader = Downloader(links, PATH, exclude=exclude_file_path, concurrency=10,
                            sub_dirs=False, headers=headers, streaming=True)

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(downloader.run())
    finally:
        loop.close()
