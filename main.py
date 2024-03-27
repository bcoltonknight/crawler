import asyncio
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import aiosqlite
import argparse
import random


with open('ua.txt', 'r') as f:
    agents = f.read().split('\n')


def init_args():
    parser = argparse.ArgumentParser(
            prog='main.py',
            description='A web crawler that takes a base url as input',
        )
    parser.add_argument('-b', '--base',
                        help='The base URL to spider from',
                        required=True)
    parser.add_argument('-w', '--workers',
                        help='The number of scraping workers',
                        default=50)

    parser.add_argument('-d', '--db-workers',
                        help='The number of async database workers to spawn',
                        dest='dw',
                        default=3)

    return parser.parse_args()

async def init():
    async with aiosqlite.connect("./records.db") as db:
        await db.execute("CREATE TABLE IF NOT EXISTS sites (id INTEGER PRIMARY KEY, url TEXT, title TEXT, parent TEXT)")
        await db.commit()


async def init_queue(urls, visited, db):
    target = await urls.get()
    # print(target)
    async with aiohttp.ClientSession() as session:
        async with session.get(target[0]) as r:
            if r.status == 404:
                pass
            body = await r.text()
            soup = BeautifulSoup(body, "html.parser")
    print(f"\"{target[0]}\",\"{soup.find('title').string}\"")
    for element in soup.select('a[href]'):
        url = element['href']
        # print([item for item in urls.queue])
        # print([item for item in visited.queue])
        # print(url)
        if url.startswith('/'):
            working = urlparse(target[0])
            url = '://'.join(working[:2]) + url
        if url.startswith('http') and url not in [item for item in visited._queue]:
            # print(url)
            await visited.put(url)
            # print(target)
            # print(f'{(url, target[0])}')
            await urls.put((url, target[0]))
    await db.put((target[0], soup.find('title').string, target[1]))

    urls.task_done()


async def db_worker(dbq: asyncio.Queue):
    # print('Worker started')
    while True:
        # print('Getting from queue')
        # print(dbq.qsize())
        site = await dbq.get()
        # print(site)
        # print(dbq.qsize())
        async with aiosqlite.connect("./records.db") as db:
            try:
                    await db.execute("INSERT INTO sites (url, title, parent) VALUES (?, ?, ?)", (site[0], site[1], site[2],))
                    await db.commit()
                    # print('Committed')
            except Exception as e:
                print(e)
                pass
        dbq.task_done()


async def worker(urls: asyncio.Queue, visited: asyncio.Queue, db: asyncio.Queue):
    first = True
    # print('Worker started')
    # print([item for item in urls.queue])
    while not urls.empty() or first:
        target = await urls.get()
        # print(target)
        # print(urls.qsize())
        try:
            # print(f'Requesting {target[0]}')
            data = {
                'User-Agent': random.choice(agents)
            }
            print(data)
            async with aiohttp.ClientSession(headers=data) as session:
                async with session.get(target[0]) as r:
                    # print('Get')
                    # print(r.status)
                    if r.status >= 404 and r.status < 500:
                        continue
                    body = await r.text()
                    # print(body)
                    soup = BeautifulSoup(body, "html.parser")
            print(f"\"{target[0]}\", \"{soup.find('title').string}\"")
            # print(f"Putting {(target[0], soup.find('title').string, target[1])} into DB Queue")
            await db.put((target[0], soup.find('title').string, target[1]))
            # print("Put")
            for element in soup.select('a[href]'):
                url = element['href']
                # print([item for item in urls.queue])
                # print([item for item in visited.queue])
                # print(url)
                if url.startswith('/'):
                    working = urlparse(target[0])
                    url = '://'.join(working[:2]) + url

                if url.startswith('http') and url not in [item for item in visited._queue]:
                    await visited.put(url)
                    await urls.put((url, target[0]))

        except Exception as e:
            # print(e)
            # print('Exception')
            pass
        urls.task_done()
        if first:
            await asyncio.sleep(2)
        first = False
    print('Worker dying')
    return


async def main():
    await init()
    args = init_args()
    visited = asyncio.Queue()
    urls = asyncio.Queue()
    db = asyncio.Queue(maxsize=100)
    # urls.put('https://scrapeme.live/shop/')
    await urls.put((args.base, ''))
    print('Initializing queue')
    await init_queue(urls, visited, db)
    # sleep(5)
    # print('Generating workers')

    db_workers = asyncio.gather(*[asyncio.create_task(db_worker(db)) for i in range(args.dw)])
    await asyncio.gather(*[asyncio.create_task(worker(urls, visited, db)) for i in range(args.workers)])
    return


if __name__ == '__main__':
    # asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())