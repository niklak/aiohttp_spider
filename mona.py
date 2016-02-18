import requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import re
BASE_URL = 'https://en.wikipedia.org/'


def fetch_links():
    r = requests.get('https://en.wikipedia.org/wiki/Mona_Lisa')
    bs = BeautifulSoup(r.text, 'lxml')
    urls = []
    for img in bs.find_all('img', {'src': re.compile(r'commons/thumb')}):
        print(img)
        urls.append(urljoin(BASE_URL, img['src'])+'\n')
    return urls


if __name__ == '__main__':
    links = fetch_links()

    with open('links.txt', 'w') as file:
        file.writelines(links)