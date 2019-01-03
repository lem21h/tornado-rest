# coding=utf-8
import hashlib
import html
import random
import re
import string
import time
import unicodedata
from calendar import timegm
from datetime import datetime, timedelta
from random import randint
from typing import Optional, NamedTuple, Any, Union
from uuid import uuid4, UUID

from bson import ObjectId

from maio.core import iso8601

PHONE_RE = re.compile(r'^\+?([0-9 ])+$')
PHONE_9_RE = re.compile(r'^\+?([0-9 ]){9}$')
TAG_RE = re.compile(r'(<!--.*?-->|<[^>]*>)')
CHARACTERS = string.ascii_letters + string.digits


def random_printable(length: int) -> str:
    return ''.join([random.choice(CHARACTERS) for _ in range(0, length)])


def parse_uuid(str_uuid: str, default=None) -> UUID:
    str_uuid = str_uuid.decode() if isinstance(str_uuid, bytes) else str_uuid
    try:
        return str_uuid if isinstance(str_uuid, UUID) else UUID(str_uuid)
    except (ValueError, TypeError, AttributeError):
        return default


def parse_objectId(str_objectId: str, default=None) -> ObjectId:
    try:
        return ObjectId(str_objectId.decode() if isinstance(str_objectId, bytes) else str_objectId)
    except (ValueError, TypeError, AttributeError):
        return default


def parse_phone_number(phone_number: str, plPhone: bool = False):
    try:
        if plPhone:
            result = PHONE_9_RE.match(phone_number)
        else:
            result = PHONE_RE.match(phone_number)
    except TypeError:
        return None
    if result:
        return result.string
    else:
        return None


def remove_tags(text: str) -> str:
    if not text or not isinstance(text, str):
        return ''
    return html.escape(TAG_RE.sub('', text))


_BOOL_MAPPING = {
    int: lambda x: x == 1,
    str: lambda x: x in ('1', 'True', 'true', True),
    bool: lambda x: x,
}


def parse_bool(value: Optional[Union[int, str, bool]], default=None) -> bool:
    if value is None:
        # quick escape for None
        return default
    fn = _BOOL_MAPPING.get(type(value))
    if fn is None:
        return default
    else:
        return fn(value)


def parse_int(value: Union[float, int, str], default: Optional[int] = 0):
    try:
        out = int(value)
    except (ValueError, TypeError):
        out = default
    return out


def parse_float(value: Union[float, int, str], default: Optional[Union[float, int]] = 0):
    try:
        out = float(value)
    except (ValueError, TypeError):
        out = default
    return out


def parse_date(date_stamp: str, default=None) -> datetime:
    try:
        return iso8601.parse_date(date_stamp)
    except (ValueError, iso8601.ParseError, TypeError):
        return default


def parse_date_to_unix_ts(date_stamp, default=None):
    dt = parse_date(date_stamp)
    if dt:
        return get_unixtimestamp(dt)
    else:
        return default


def get_uts_high_precision() -> float:
    return time.time()


def get_unixtimestamp(eventDate: datetime = None) -> int:
    """
    Converts date and time to unix timestamp
    If date is not provided then it takes current time and convert it to unix timestamp
    :param eventDate: datetime
    :return: int
    """
    if not eventDate or not isinstance(eventDate, datetime):
        eventDate = datetime.utcnow()
    return timegm(eventDate.utctimetuple())


def from_str_to_unixtimestamp(date_str: str, str_format='%Y-%m-%d') -> Optional[int]:
    try:
        date = datetime.strptime(date_str, str_format)
        return get_unixtimestamp(date)
    except (ValueError, TypeError):
        return None


def last_day_of_month(any_day):
    next_month = any_day.replace(day=28) + timedelta(days=4)  # this will never fail
    return next_month - timedelta(days=next_month.day)


def unixtimestamp_to_str(timestamp, str_format='%Y-%m-%d'):
    return datetime.utcfromtimestamp(timestamp).strftime(str_format)


def uts_to_str(timestamp=None, str_format='%Y-%m-%dT%H:%M:%SZ'):
    return unixtimestamp_to_str(timestamp, str_format) if timestamp else ''


def hash_password(password, salt) -> str:
    return hashlib.sha256(("%s%s%s" % (salt, password, salt)).encode()).hexdigest()


def generate_token():
    return "".join([str(randint(100, 999)), str(uuid4()).replace('-', '')])


def filter_fields(data: dict, field_set: tuple) -> dict:
    return {k: v for k, v in data.items() if k in field_set}


def baseN(num: int, base: int, numerals: str = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"):
    return ((num == 0) and numerals[0]) or (
            baseN(num // base, base, numerals).lstrip(numerals[0]) + numerals[num % base])


def date_range(startDate, days):
    counter = 0
    while counter < days:
        yield startDate + timedelta(days=counter)
        counter += 1


def parse_UTFStr(valueStr):
    if not valueStr:
        return None
    try:
        valueStr = valueStr.decode('utf-8')
        return valueStr
    except UnicodeError:
        return valueStr


def random_date_gen(startDate, endDate, amount=100, minHour=8, maxHour=20):
    t1 = get_unixtimestamp(startDate)
    t2 = get_unixtimestamp(endDate)

    i = 0
    while i < amount:
        d = datetime.fromtimestamp(randint(t1, t2))
        if minHour < d.hour < maxHour:
            mins = 0 if d.minute < 30 else 30
            i += 1
            yield datetime(d.year, d.month, d.day, d.hour, mins, 0, tzinfo=d.tzinfo)


def remove_polish_chars(inStr):
    return unicodedata.normalize('NFKD', inStr).replace(u'ł', 'l').replace(u'Ł', 'L').encode('ascii', 'ignore')


def flatten_dict(dd, separator='_', prefix=''):
    return {'%s%s%s' % (prefix, separator, k.upper()) if prefix else k: v
            for kk, vv in dd.items()
            for k, v in flatten_dict(vv, separator, kk).items()
            } if isinstance(dd, dict) else {prefix: dd}


class FunctionResult(NamedTuple):
    status: int
    result: Any


RESULT_OK = 1
RESULT_ERR = 0
