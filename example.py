from bs4 import BeautifulSoup
from base import AWebSpider
import asyncio


class WikiCrawler(AWebSpider):

    async def get_parsed_content(self, url):
        html = await self.get_html_from_url(url)
        data = {}
        bs = BeautifulSoup(html, 'lxml')
        title = bs.find('title')

        if title:
            title = title.get_text().replace('- Wikipedia, the free encyclopedia', '')
        lastmod = bs.find('', {'id': 'footer-info-lastmod'})
        if lastmod:
            lastmod = lastmod.get_text().replace('This page was last modified on ', '')
        data.update({'title': title, 'lastmod': lastmod})
        return data


if __name__ == '__main__':

    base_url = 'https://en.wikipedia.org/'
    capture = r'/wiki/'
    exclude = [':']
    concurrency = 2
    max_crawl = 10
    max_parse = 10
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) "
                             "AppleWebKit 537.36 (KHTML, like Gecko) Chrome",
               "Accept": "text/html,application/xhtml+xml,"
                         "application/xml;q=0.9,image/webp,*/*;q=0.8"}
    web_crawler = WikiCrawler(base_url, capture, concurrency, timeout=60,
                              verbose=True, headers=headers, exclude=exclude,
                              max_crawl=max_crawl, max_parse=max_parse)
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(web_crawler.run())
    finally:
        loop.close()
