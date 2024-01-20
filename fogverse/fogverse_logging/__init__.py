import inspect
import logging

from .handler import CsvRotatingFileHandler
from .formatter import CsvFormatter

from pathlib import Path

logging.FOGV_STDOUT = logging.INFO + 3
logging.FOGV_CSV = logging.INFO + 2
logging.FOGV_FILE = logging.INFO + 1

logging.addLevelName(logging.FOGV_STDOUT, "FOGV_STDOUT")
logging.addLevelName(logging.FOGV_CSV, "FOGV_CSV")
logging.addLevelName(logging.FOGV_FILE, "FOGV_FILE")

DEFAULT_FMT = '[%(asctime)s][%(levelname)s][%(name)s] %(message)s'

def get_logger(name=None,
               level=logging.DEBUG,
               handlers=[],
               formatter=None):
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not handlers:
        handler = logging.StreamHandler()
        formatter = formatter or logging.Formatter(fmt=DEFAULT_FMT)
        handler.setFormatter(formatter)

        handlers = [handler]

    if type(handlers) not in (list,tuple): handlers = [handlers]
    for h in handlers:
        logger.addHandler(h)
    return logger

def get_file_logger(name=None,
                    dirname='logs',
                    filename=None,
                    handler=None,
                    formatter=None,
                    mode='w',
                    **kwargs):
    if filename is None:
        filename = f'{name}.txt'
        if not filename.startswith('log'):
            filename = f'log_{filename}'
    filename = Path(dirname) / filename
    filename.parent.mkdir(parents=True, exist_ok=True)

    if not handler:
        handler = logging.FileHandler(filename=filename,
                                      mode=mode)
        formatter = formatter or logging.Formatter(fmt=DEFAULT_FMT)
        handler.setFormatter(formatter)
    return get_logger(name=name,handlers=handler,**kwargs)

def get_csv_logger(name=None,
                   dirname='logs', # relative to the file's dir
                   filename=None,
                   handler=None,
                   mode='w',
                   fmt=None,
                   delimiter=',',
                   datefmt='%Y/%m/%d %H:%M:%S',
                   csv_header=['asctime','name'],
                   header=[],
                   **kwargs):
    if fmt is None:
        fmt = f'%(asctime)s.%(msecs)03d{delimiter}%(name)s{delimiter}%(message)s'

    if filename is None:
        filename = f'{name}.csv'
        if not filename.startswith('log'):
            filename = f'log_{filename}'
    filename = Path(dirname) / filename
    filename.parent.mkdir(parents=True, exist_ok=True)

    if not handler:
        handler = CsvRotatingFileHandler(filename,
                                         fmt=fmt,
                                         datefmt=datefmt,
                                         header=csv_header+header,
                                         delimiter=delimiter,
                                         mode=mode)
        formatter = CsvFormatter(fmt=fmt,
                                     datefmt=datefmt,
                                     delimiter=delimiter)
        handler.setFormatter(formatter)
    return get_logger(name=name,handlers=handler,**kwargs)

class FogVerseLogging:
    def __init__(self,
                 name=None,
                 dirname='logs',
                 csv_header=[],
                 level=logging.FOGV_STDOUT,
                 std_log_kwargs={},
                 csv_log_kwargs={},
                 file_log_kwargs={}):
        self._std_log = get_logger(name=f'std_{name}',level=level,
                                   **std_log_kwargs)
        self._file_log = get_file_logger(name=f'file_{name}',level=level,
                                         dirname=dirname,**file_log_kwargs)
        self._csv_log = get_csv_logger(name=f'csv_{name}',level=level,
                                       dirname=dirname,header=csv_header,
                                       **csv_log_kwargs)

    def setLevel(self, level):
        self._std_log.setLevel(level)
        self._file_log.setLevel(level)
        self._csv_log.setLevel(level)

    def std_log(self, message, *args, **kwargs):
        if self._std_log.isEnabledFor(logging.FOGV_STDOUT):
            self._std_log._log(logging.FOGV_STDOUT, message,
                               args, **kwargs)
        if self._file_log.isEnabledFor(logging.FOGV_FILE):
            self._file_log._log(logging.FOGV_FILE, message,
                                args, **kwargs)

    def csv_log(self, message, *args, **kwargs):
        if self._csv_log.isEnabledFor(logging.FOGV_CSV):
            self._csv_log._log(logging.FOGV_CSV, message,
                                args, **kwargs)

    def file_log(self, message, *args, **kwargs):
        if self._file_log.isEnabledFor(logging.FOGV_FILE):
            self._file_log._log(logging.FOGV_FILE, message,
                                args, **kwargs)
