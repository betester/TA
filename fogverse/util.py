import base64
import cv2
import inspect
import numpy as np
import os
import sys
import uuid

from datetime import datetime
from io import BytesIO

def get_cam_id():
    return f"cam_{os.getenv('CAM_ID', str(uuid.uuid4()))}"

def bytes_to_numpy(bbytes):
    f = BytesIO(bbytes)
    return np.load(f, allow_pickle=True)

def numpy_to_bytes(arr):
    f = BytesIO()
    np.save(f,arr)
    return f.getvalue()

def get_size(obj, seen=None):
    """Recursively finds size of objects in bytes.\n
    Modified version of
    https://github.com/bosswissam/pysize/blob/master/pysize.py"""
    size = sys.getsizeof(obj)
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    # Important mark as seen *before* entering recursion to gracefully handle
    # self-referential objects
    seen.add(obj_id)
    if hasattr(obj, '__dict__'):
        for cls in obj.__class__.__mro__:
            if '__dict__' in cls.__dict__:
                d = cls.__dict__['__dict__']
                if inspect.isgetsetdescriptor(d) \
                    or inspect.ismemberdescriptor(d):
                    size += get_size(obj.__dict__, seen)
                break
    if isinstance(obj, dict):
        size += sum((get_size(v, seen) for v in obj.values()))
        size += sum((get_size(k, seen) for k in obj.keys()))
    elif hasattr(obj, '__iter__') and not isinstance(obj,
                                                     (str, bytes, bytearray)):
        try:
            size += sum((get_size(i, seen) for i in obj))
        except TypeError:
            pass
    if hasattr(obj, '__slots__'): # can have __slots__ with __dict__
        size += sum(get_size(getattr(obj, s), seen)\
                    for s in obj.__slots__ if hasattr(obj, s))

    return size

def size_kb(obj, decimals=3):
    _size = get_size(obj)
    return round(_size/1e3, decimals)

DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S.%f'

def get_timestamp(utc=True):
    if utc:
        _date = datetime.utcnow()
    else:
        _date = datetime.now()
    return _date

def get_timestamp_str(date=None, utc=True, format=DATETIME_FORMAT):
    date = date or get_timestamp(utc=utc)
    return datetime.strftime(date, format)

def timestamp_to_datetime(timestamp, format=DATETIME_FORMAT):
    if isinstance(timestamp, bytes):
        timestamp = timestamp.decode()
    return datetime.strptime(timestamp, format)

def calc_datetime(start, end=None, format=DATETIME_FORMAT, decimals=2,
                  utc=True):
    if start is None: return -1
    if end is None:
        end = get_timestamp(utc=utc)
    elif isinstance(end, str):
        end = datetime.strptime(end, format)
    if isinstance(start, str):
        start = datetime.strptime(start, format)
    diff = (end - start).total_seconds()*1e3
    return round(diff, decimals)

def get_header(headers, key, default=None, decoder=None):
    if headers is None or key is None: return default
    for header in headers:
        if header[0] == key:
            val = header[1]
            if callable(decoder):
                return decoder(val)
            if isinstance(val, bytes):
                return val.decode()
            return val
    return default

def _encode(img, encoding, *args):
    _, encoded = cv2.imencode(f'.{encoding}', img, *args)
    return encoded

def compress_encoding(img, encoding, *args):
    encoded = _encode(img, encoding, *args)
    return numpy_to_bytes(encoded)

def _decode(img):
    return cv2.imdecode(img, cv2.IMREAD_COLOR)

def recover_encoding(img_bytes):
    img = bytes_to_numpy(img_bytes)
    return _decode(img)

def numpy_to_base64_url(img, encoding, *args):
    img = _encode(img, encoding, *args)
    b64 = base64.b64encode(img).decode()
    return f'data:image/{encoding};base64,{b64}'

class _Null:
    pass
_null = _Null()

def get_config(config_name: str, cls: object=None, default=None):
    ret = os.getenv(config_name, _null)
    if not isinstance(ret, _Null): return ret
    if cls is None: return ret or default
    return getattr(cls, config_name.lower(), default)
