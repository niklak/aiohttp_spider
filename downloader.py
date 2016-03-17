import time
import logging
import os
import asyncio
import aiohttp


class Downloader:
    def __init__(self, links, download_to, exclude=None, headers=None,
                 concurrency=2, streaming=False, sub_dirs=True, verbose=True):

        self.links = links
        self.path = download_to
        self.exclude = exclude if exclude is not None else ''

        self.client = aiohttp.ClientSession(headers=headers)
        self.queue = asyncio.Queue()
        self.concurrency = concurrency
        self._is_streaming = streaming
        self.sub_dirs = sub_dirs

        logging.basicConfig(level='INFO')
        self.log = logging.getLogger()
        if not verbose:
            self.log.disabled = True

    async def download(self, url, path):
        async with self.client.get(url) as response:
            if response.status != 200:
                self.log.error('BAD RESPONSE: {}'.format(response.status))
                return
            content = await response.read()

        with open(path, 'wb') as file:
            file.write(content)

        logging.info('File has been stored at {}'.format(path))

    async def stream_download(self, url, path):
        async with self.client.get(url) as response:
            if response.status != 200:
                self.log.error('BAD RESPONSE: {}'.format(response.status))
                return
            # Here we write file in blocking mode.
            # The only benefit here is that, memory consumed more carefully.
            tmp = path + '.chunks'
            with open(tmp, 'ab') as file:
                while True:
                    chunk = await response.content.read(1024)
                    if not chunk:
                        break
                    file.write(chunk)
            os.rename(tmp, path)
        logging.info('FILE HAS BEEN STORED AT {}'.format(path))

    async def worker(self):
        logging.info('Starting worker')
        while True:
            link = await self.queue.get()

            try:
                logging.info('PROCESSING {}'.format(link))

                if self.sub_dirs:
                    path = os.path.join(self.path,
                                        link.replace(self.exclude, ''))
                else:
                    path = os.path.join(self.path, link.split('/')[-1])

                if not os.path.isdir(os.path.dirname(path)):
                    os.makedirs(os.path.dirname(path))

                if self._is_streaming:
                    await self.stream_download(link, path)
                else:
                    await self.download(link, path)
                logging.info('REMAINED {}'.format(self.queue.qsize()))

            except Exception:
                logging.error('An error has occurred during downloading {}'.
                              format(link), exc_info=True)
            finally:
                self.queue.task_done()

    async def run(self):
        start = time.time()
        print('Starting downloading')

        await asyncio.wait([self.queue.put(link) for link in self.links])

        tasks = [asyncio.ensure_future(self.worker())
                 for _ in range(self.concurrency)]

        await self.queue.join()

        logging.info('Finishing...')
        for task in tasks:
            task.cancel()

        self.client.close()

        end = time.time()
        print('FINISHED AT {} secs'.format(end-start))