from openpyxl import load_workbook
import grequests
from datetime import datetime
from sqlalchemy import Column, Integer, Float, String, DateTime, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

#TODO: Exception handling, logging, settings, 

Base = declarative_base()


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

class Monitoring(Base):

    __tablename__ = 'monitoring'

    id = Column('id', Integer, primary_key=True)
    ts = Column('timestamp', DateTime)
    url = Column('url', String, nullable=False)
    label = Column('label', String, nullable=False)
    response_time = Column('response_time', Float)
    status_code = Column('status_code', Integer)
    content_length = Column('content_length', Integer)

engine = create_engine('sqlite:///sqlite3.db')
Base.metadata.create_all(bind=engine)
Session = sessionmaker(bind=engine)

urls_to_fetch = process_xlsx('raw_data.xlsx')

r = (grequests.get(url) for url in urls_to_fetch)
a = grequests.map(r)

for i in range(len(a)):
    session = Session()
    try:
        monitoring = Monitoring()
        monitoring.ts = datetime.now()
        monitoring.url = a[i].url #FIXME
        monitoring.status_code = a[i].status_code
        monitoring.label = 'aaa'
        monitoring.response_time = a[i].elapsed.total_seconds() * 1000
        monitoring.content_length = len(a[i].content) if monitoring.status_code==200 else None

        session.add(monitoring)
        session.commit()
    except:
        
        pass
    finally:
        session.close()
