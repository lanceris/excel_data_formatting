import asyncio
import logging
import sys

import aiohttp
from aiohttp import ClientSession
from openpyxl import load_workbook
from sqlalchemy import Column, Integer, Float, String, DateTime, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

import utils

config = utils.config
def_log = utils.def_log
con_log = utils.con_log

MAX_CONNECTIONS = 100


def process_xlsx(filename):
    wb = load_workbook(filename=filename)
    ws = wb.active
    n = 1
    urls_to_fetch = []

    config = utils.load_config()

    while ws[f'{config["url_col"]}{n}'].value is not None:
        if ws[f'{config["fetch_col"]}{n}'].value == 1:
            urls_to_fetch.append((ws[f'{config["url_col"]}{n}'].value, ws[f'{config["label_col"]}{n}'].value))
        n += 1
    return urls_to_fetch


async def process_urls(session):
    main_queue = asyncio.Queue(maxsize=1000)
    responses = []
    urls_to_fetch = process_xlsx(sys.argv[1])[:5]
    # we init the consumers, as the queues are empty at first,
    # they will be blocked on the main_queue.get()
    consumers = [asyncio.ensure_future(consumer(main_queue=main_queue,
                                                session=session,
                                                responses=responses)) for _ in range(min(MAX_CONNECTION,
                                                                                         len(urls_to_fetch)))]
    await producer(queue=main_queue, urls_to_fetch=urls_to_fetch)
    # wait for all item's inside the main_queue to get task_done
    await main_queue.join()
    # cancel all coroutines
    for consumer_future in consumers:
        consumer_future.cancel()
    print(responses)
    return responses


async def run(loop):
    # we init more connectors to get better performance
    async with ClientSession(loop=loop, connector=aiohttp.TCPConnector(limit=MAX_CONNECTIONS)) as session:
        resps = await process_urls(session)
    return resps


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop))