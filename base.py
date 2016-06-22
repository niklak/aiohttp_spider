import time
import logging
import json
import csv
import re
import asyncio
import aiohttp
from collections import defaultdict
from urllib.parse import urljoin, urldefrag
from urllib.parse import unquote
from lxml import html


class BQueue(asyncio.Queue):
    """ Bureaucratic queue """

    def __init__(self, maxsize=0, capacity=0, *, loop=None):
        """
        :param maxsize: a default maxsize from tornado.queues.Queue,
            means maximum queue members at the same time.
        :param capacity: means a quantity of income tries before will refuse
            accepting incoming data
        """
        super().__init__(maxsize, loop=None)
        if capacity is None:
            raise TypeError("capacity can't be None")

        if capacity < 0:
            raise ValueError("capacity can't be negative")

        self.capacity = capacity
        self.put_counter = 0
        self.is_reached = False

    def put_nowait(self, item):

        if not self.is_reached:
            super().put_nowait(item)
            self.put_counter += 1

            if 0 < self.capacity == self.put_counter:
                self.is_reached = True


class AWebSpider:
    OUTPUT_FORMATS = ['json', 'csv']

    def __init__(self, base_url, capture_pattern, concurrency=2, timeout=300,
                 delay=0, headers=None, exclude=None, verbose=True,
                 output='json', max_crawl=0, max_parse=0, cookies=None,
                 start_url=None, retries=2):

        assert output in self.OUTPUT_FORMATS, 'Unsupported output format'

        self.output = output

        self.base = base_url
        self.start_url = self.base if not start_url else start_url
        self.capture = re.compile(capture_pattern)
        self.exclude = exclude if isinstance(exclude, list) else []

        self.concurrency = concurrency
        self.timeout = timeout
        self.delay = delay
        self.retries = retries

        self.q_crawl = BQueue(capacity=max_crawl)
        self.q_parse = BQueue(capacity=max_parse)

        self.brief = defaultdict(set)
        self.data = []

        self.can_parse = False

        logging.basicConfig(level='INFO')
        self.log = logging.getLogger()

        if not verbose:
            self.log.disabled = True

        if not cookies:
            cookies = dict()
        self.client = aiohttp.ClientSession(headers=headers, cookies=cookies)

    def get_parsed_content(self, url):
        """
        :param url: an url from which html will be parsed.
        :return: it has to return a dict with data.
        It must be a coroutine.
        """
        raise NotImplementedError

    def get_urls(self, document):
        urls = []
        urls_to_parse = []
        dom = html.fromstring(document)
        for href in dom.xpath('//a/@href'):
            if any(e in href for e in self.exclude):
                    continue
            url = unquote(urljoin(self.base, urldefrag(href)[0]))
            if url.startswith(self.base):
                if self.capture.search(url):
                    urls_to_parse.append(url)
                else:
                    urls.append(url)
        return urls, urls_to_parse

    async def get_html_from_url(self, url):
        async with self.client.get(url) as response:

            if response.status != 200:
                self.log.error('BAD RESPONSE: {}'.format(response.status))
                return

            return await response.text()

    async def get_links_from_url(self, url):
        document = await self.get_html_from_url(url)
        return self.get_urls(document)

    async def __wait(self, name):

        if self.delay > 0:
            self.log.info('{} waits for {} sec.'.format(name, self.delay))
            await asyncio.sleep(self.delay)

    async def crawl_url(self):
        current_url = await self.q_crawl.get()
        try:
            if current_url in self.brief['crawling']:
                return    # go to finally block first and then return

            self.log.info('Crawling: {}'.format(current_url))
            self.brief['crawling'].add(current_url)
            urls, urls_to_parse = await self.get_links_from_url(current_url)
            self.brief['crawled'].add(current_url)

            for url in urls:
                if self.q_crawl.is_reached:
                    self.log.warning('Maximum crawl length has been reached')
                    break
                await self.q_crawl.put(url)

            for url in urls_to_parse:
                if self.q_parse.is_reached:
                    self.log.warning('Maximum parse length has been reached')
                    break

                if url not in self.brief['parsing']:
                    await self.q_parse.put(url)
                    self.brief['parsing'].add(url)
                    self.log.info('Captured: {}'.format(url))

            if not self.can_parse and self.q_parse.qsize() > 0:
                    self.can_parse = True
        except Exception as exc:
            self.log.warn('Exception {}:'.format(exc))

        finally:
            self.q_crawl.task_done()

    async def parse_url(self):
        url_to_parse = await self.q_parse.get()
        self.log.info('Parsing: {}'.format(url_to_parse))

        try:
            content = await self.get_parsed_content(url_to_parse)
            self.data.append(content)

        except Exception:
            self.log.error('An error has occurred during parsing',
                           exc_info=True)
        finally:
            self.q_parse.task_done()

    async def crawler(self):
        while True:
            await self.crawl_url()
            await self.__wait('Crawler')
        return

    async def parser(self):
        retries = self.retries
        while True:
            if self.can_parse:
                await self.parse_url()
            elif retries > 0:
                await asyncio.sleep(0.5)
                retries -= 1
            else:
                break
            await self.__wait('Parser')
        return

    def _write_json(self, name):

        with open('{}-{}.json'.format(name, time.time()), 'w') as file:
            json.dump(self.data, file)

    def _write_csv(self, name):
        headers = self.data[0].keys()
        with open('{}-{}.csv'.format(name, time.time()), 'w') as csvfile:
            writer = csv.DictWriter(csvfile, headers)
            writer.writeheader()
            writer.writerows(self.data)

    async def run(self):
        start = time.time()

        print('Start working')

        await self.q_crawl.put(self.start_url)

        def task_completed(future):
            # This function should never be called in right case.
            # The only reason why it is invoking is uncaught exception.
            exc = future.exception()
            if exc:
                self.log.error('Worker has finished with error: {} '
                               .format(exc), exc_info=True)

        tasks = []

        for _ in range(self.concurrency):
            fut_crawl = asyncio.ensure_future(self.crawler())
            fut_crawl.add_done_callback(task_completed)
            tasks.append(fut_crawl)
            fut_parse = asyncio.ensure_future(self.parser())
            fut_parse.add_done_callback(task_completed)
            tasks.append(fut_parse)

        await asyncio.wait_for(self.q_crawl.join(), self.timeout)
        await self.q_parse.join()

        for task in tasks:
            task.cancel()

        self.client.close()

        end = time.time()
        print('Done in {} seconds'.format(end - start))

        assert self.brief['crawling'] == self.brief['crawled'], \
            'Crawling and crawled urls do not match'

        assert len(self.brief['parsing']) == len(self.data), \
            'Parsing length does not equal parsed length'

        self.log.info('Total crawled: {}'.format(len(self.brief['crawled'])))
        self.log.info('Total parsed: {}'.format(len(self.data)))
        self.log.info('Starting write to file')

        name = self.base.split('//')[1].replace('www', '').replace('/', '')

        if self.output == 'json':
            self._write_json(name)
        elif self.output == 'csv':
            self._write_csv(name)

        print('Parsed data has been stored.')
        print('Task done!')
