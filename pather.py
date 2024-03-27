import aiosqlite
import asyncio
import urllib.parse
import argparse

def init_args():
    parser = argparse.ArgumentParser(
        prog='pather.py',
        description='Trace from a base URL in records.db',
        epilog='Be gay, do crime :)'
    )
    parser.add_argument('-u', '--url',
                        help='The url to trace to its starting url',
                        required=True)

    return parser.parse_args()

async def get_parent(site: str):
    async with aiosqlite.connect('records.db') as db:
      async with db.execute("SELECT * FROM sites where url = ?", (site,)) as cursor:
        row = await cursor.fetchone()
        if row:
            return row[3]
        else:
            return None

async def main():
    args = init_args()
    print(f'{urllib.parse.unquote(args.url)}', end='')
    parent = await get_parent(args.url)
    while parent != '':
        print(f'<-{urllib.parse.unquote(parent)}', end='')
        parent = await get_parent(parent)

asyncio.run(main())