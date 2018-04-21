import asyncio
import async_timeout
import datetime
import json
import logging
import time
import traceback
import sys

import aiohttp
from openpyxl import load_workbook
from sqlalchemy import Column, Integer, Float, String, DateTime, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

#Load config
with open('config.json') as json_config:
    config = json.load(json_config)


def setup_logger(name, log_file, level=logging.INFO):
    """Function setup as many loggers as you want"""

    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
logger = setup_logger('first_logger', 'log.log')
error_logger = setup_logger('error_logger', 'error_log.json')


def process_xlsx(filename):
    wb = load_workbook(filename = filename)
    ws = wb.active
    n=1
    urls_to_fetch = []

    while ws[f'A{n}'].value is not None:
        if ws[f'C{n}'].value == 1:
            urls_to_fetch.append((ws[f'A{n}'].value, ws[f'B{n}'].value))
        n += 1
    return urls_to_fetch


async def get(url, label):
    async with aiohttp.ClientSession() as session:
        try:
            start = time.time()
            with async_timeout.timeout(config['request_timeout']):
                async with session.get(url) as response:
                    dct = dict()
                    dct['ts'] = datetime.datetime.now()
                    dct['url'] = str(response.url)
                    dct['status_code'] = response.status
                    dct['label'] = label
                    dct['response_time'] = "{:.2f}".format((time.time() - start) * 1000)
                    dct['content_length'] = len(await response.read()) if response.status == 200 else None
                    info.append(dct)
        except Exception as e:
            error_logger.log(logging.ERROR,
                        {
                        'timestamp': datetime.datetime.now(),
                        'url' : str(url),
                        'error':
                            {
                                'exception_type': e.__class__.__name__,
                                'exception_value': str(e),
                                'stack_info': traceback.format_exc()
                            }
                        })

info = []

loop = asyncio.get_event_loop()
tasks = [asyncio.ensure_future(get(url[0], url[1])) for url in process_xlsx(sys.argv[1])]
loop.run_until_complete(asyncio.wait(tasks))

# Save to db
Base = declarative_base()


class Monitoring(Base):

    __tablename__ = 'monitoring'

    id = Column('id', Integer, primary_key=True)
    ts = Column('timestamp', DateTime)
    url = Column('url', String, nullable=False)
    label = Column('label', String, nullable=False)
    response_time = Column('response_time', Float)
    status_code = Column('status_code', Integer)
    content_length = Column('content_length', Integer)


engine = create_engine(f'sqlite:///{config["db_path"]}')
Base.metadata.create_all(bind=engine)
Session = sessionmaker(bind=engine)
db_session = Session()
for each in info:
    monitoring = Monitoring(**each)
    db_session.add(monitoring)
    db_session.commit()
db_session.close()

