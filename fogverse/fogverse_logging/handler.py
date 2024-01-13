from .formatter import CsvFormatter

from logging.handlers import RotatingFileHandler
from os import path

class CsvRotatingFileHandler(RotatingFileHandler):
    def __init__(self,
                 filename,
                 fmt=None,
                 datefmt=None,
                 max_size=0,
                 header=None,
                 delimiter=',',
                 mode='a',
                 **kwargs):
        # check if file exists
        self.file_pre_exists = path.exists(filename)
        # call parent file handler __init__
        super().__init__(filename, maxBytes=max_size, mode=mode, **kwargs)
        self.formatter = CsvFormatter(fmt, datefmt, delimiter)
        # Format header string if needed
        self._header = header and self.formatter.format_msg(header)
        # Write the header if delay is False and a file stream was created.
        if self.stream is not None and \
                (not self.file_pre_exists or mode == 'w'):
            self.stream.write('%s\n' % self._header)

    def doRollover(self):
        ''' prepend header string to each log file '''
        super().doRollover()
        if self._header is None:
            return
        # temporarily overwrite format function with a straight
        # pass-through lambda function handle header without
        # formatting, then reset format function to what it was
        f = self.formatter.format
        self.formatter.format = lambda x: x
        self.handle(self._header)
        self.formatter.format = f
