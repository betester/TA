from logging import Formatter

class CsvFormatter(Formatter):
    def __init__(self, fmt=None, datefmt=None, delimiter=','):
        super().__init__(fmt, datefmt)
        self._delimiter = delimiter

    def format_msg(self, msg):
        ''' format the msg to csv string from list if list '''
        if isinstance(msg, str): return msg
        try:
            msg = self._delimiter.join(map(str, msg))
        except TypeError:
            pass
        return msg

    def format(self, record):
        ''' run format_msg on record to get string
        before passing to Formatter '''
        record.msg = self.format_msg(record.msg)
        return super().format(record)
