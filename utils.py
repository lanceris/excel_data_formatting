from datetime import datetime
import json
import logging
from time import time
import traceback


def setup_logger(name,
                 log_handler,
                 log_file=None,
                 log_format='%(asctime)s:%(levelname)s:%(message)s',
                 log_date_format='%d/%m/%Y %H:%M:%S',
                 level=logging.INFO):
    """Function to setup many loggers"""

    handler = log_handler(log_file)
    handler.setFormatter(logging.Formatter(log_format, datefmt=log_date_format))
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


def log_time(loggers):
    def timeit(method):
        def timed(*args, **kw):
            ts = time()
            result = method(*args, **kw)
            te = time()
            try: # check if loggers is iterable
                for logger in loggers:
                    logger.log(logging.INFO, '{}: {:.2f} ms'.format(method.__name__, round((te - ts) * 1000, 2)))
            except TypeError: # not iterable
                loggers.log(logging.INFO, '{}: {:.2f} ms'.format(method.__name__, round((te - ts) * 1000, 2)))
            return result
        return timed
    return timeit


def load_config(config_path='config.json'):
    try:
        with open(config_path) as json_config:
            config = json.load(json_config)
    except FileNotFoundError:
        with open(config_path, 'w') as json_config:
            config = {
                        "url_col": "A",
                        "label_col": "B",
                        "fetch_col": "C",
                        "request_timeout": 1,
                        "error_log_path": "error_dump.json",
                        "log_path": "log.log",
                        "db_path": "sqlite3.db",
                        "urls_amount": 100
                     }
            json.dump(config, json_config, indent=4)
    return config


async def consumer(main_queue, session, responses, errors):
    start = time()
    url = await main_queue.get()
    try:
        async with session.head(url[0], timeout=1) as response:
                dct = dict()
                dct['ts'] = datetime.now()
                dct['url'] = str(response.url)
                dct['status_code'] = response.status
                dct['label'] = url[1]
                dct['response_time'] = "{:.2f}".format((time() - start) * 1000)
                dct['content_length'] = len(await response.read()) \
                    if response.status == 200 else None
                responses.append(dct)
                main_queue.task_done()
    except Exception as e:
        error = {
            'timestamp': str(datetime.now().strftime('%d/%m/%Y %H:%M:%S')),
            'url': str(url),
            'error':
                {
                    'exception_type': str(e.__class__.__name__),
                    'exception_value': str(e),
                    'stack_info': str(traceback.format_exc())
                }
        }
        errors.append(error)
        main_queue.task_done()


async def producer(queue, urls_to_fetch):
    for url in urls_to_fetch:
        # if the queue is full, this line will be blocked
        # until a consumer will finish processing a url
        await queue.put(url)




config = load_config()
con_log = setup_logger('con_log',
                       log_handler=logging.StreamHandler,
                       log_date_format='[%H:%M:%S]')
def_log = setup_logger('def_log',
                       log_handler=logging.FileHandler,
                       log_file=config['log_path'],
                       log_date_format='%d/%m/%Y [%H:%M:%S]')



