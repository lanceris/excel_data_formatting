import asyncio
import datetime
import logging
import time
import traceback

import aiohttp
from openpyxl import load_workbook
from sqlalchemy import Column, Integer, Float, String, DateTime, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

#TODO: Exception handling, logging, settings, 

def process_xlsx(filename):
    wb = load_workbook(filename = filename)
    ws = wb.active
    n=1
    urls_to_fetch = []
    while ws[f'A{n}'].value is not None:
        if ws[f'C{n}'].value == 1:
            urls_to_fetch.append(ws[f'A{n}'].value)
        n += 1
    return urls_to_fetch

urls_to_fetch = process_xlsx('raw_data.xlsx') #TODO: Configurable path

async def get(url):
    async with aiohttp.ClientSession() as session:
        try:
            start = time.time()
            async with session.request('GET', url) as response:
                dct = dict()
                dct['ts'] = datetime.datetime.now()
                dct['url'] = str(response.url)
                dct['status_code'] = response.status
                dct['label'] = 'aaa'
                dct['response_time'] = "{:.2f}".format((time.time() - start) * 1000)
                dct['content_length'] = len(await response.read()) if response.status == 200 else None
                info.append(dct)
        except aiohttp.client_exceptions.ClientConnectionError as e:
            pass
            """{
                'timestamp': datetime.datetime.now(),
                'url' : str(url),
                'error':
                    {
                        'exception_type': e.__class__.__name__,
                        'exception_value': str(e),
                        'stack_info': traceback.format_exc()
                    } 
                }""" #TODO: Log this to .json

info = []
loop = asyncio.get_event_loop()
tasks = [asyncio.ensure_future(get(url)) for url in urls_to_fetch]
loop.run_until_complete(asyncio.wait(tasks))

#Save to db
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

engine = create_engine('sqlite:///sqlite3.db')#TODO: Configurable path
Base.metadata.create_all(bind=engine)
Session = sessionmaker(bind=engine)
db_session = Session()
for each in info:
    monitoring = Monitoring(**each)
    db_session.add(monitoring)
    db_session.commit()
db_session.close()
