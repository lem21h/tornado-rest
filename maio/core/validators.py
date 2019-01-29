# coding=utf-8
import binascii
import re
from datetime import datetime
from encodings.base64_codec import base64_decode
from typing import NamedTuple, Callable, Dict, Optional, Union, List, Set, Tuple, Any

from maio.core.data import VO
from maio.core.helpers import (RESULT_OK, FunctionResult, RESULT_ERR, remove_tags, parse_phone_number, parse_date, parse_int, parse_uuid, parse_objectId,
                               parse_bool, parse_float)
from maio.core.iso3166 import countries_by_alpha3

_EMAIL_RE = re.compile(r'(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)')


class _FieldValidator(NamedTuple):
    method: Callable
    params: Dict = None

    def call(self, value):
        if self.params:
            return self.method(value, **self.params)
        else:
            return self.method(value)


class ValErrorEntry(NamedTuple):
    code: int
    message: str


def _val_err_dict(err: ValErrorEntry):
    return {'code': err.code, 'message': err.message}


_ERR_JUST_FAIL = 0
_ERR_REQUIRED = 5
_ERR_INVALID_VALUE = 6
_ERR_EXPECTED_LIST = 7

_ERR_STR_NOT_STRING = 10
_ERR_STR_TOO_SHORT = 11
_ERR_STR_TOO_LONG = 12
_ERR_STR_NOT_ENDS_WITH = 13
_ERR_STR_NOT_STARTS_WITH = 14

_ERR_EMAIL_DOMAIN = 20
_ERR_EMAIL_NOT_VALID = 21

_ERR_PHONE_FORMAT = 30

_ERR_DATE_FORMAT = 40
_ERR_DATE_BEFORE = 41
_ERR_DATE_AFTER = 41

_ERR_NUMBER_FORMAT = 50
_ERR_NUMBER_TOO_SMALL = 51
_ERR_NUMBER_TOO_BIG = 52

_ERR_LIST_TOO_SHORT = 60
_ERR_LIST_TOO_BIG = 61
_ERR_LIST_VALUE_ERROR = 62

_ERR_VALUE_IN = 70

_ERR_ADDR_FORMAT = 80
_ERR_ADDR_MISSING_CITY = 81
_ERR_ADDR_MISSING_COUNTRY = 82
_ERR_ADDR_MISSING_STREET = 83
_ERR_ADDR_MISSING_DISTRICT = 84
_ERR_ADDR_COUNTRY = 85

_ERR_IMG_JPEG = 101
_ERR_IMG_GIF = 102
_ERR_IMG_PNG = 103
_ERR_IMG_CONTENT_TOO_SHORT = 104
_ERR_IMG_MISSING_HEADER = 105
_ERR_IMG_CONTENT = 106
_ERR_IMG_TYPE = 107


def _is_required(value: Any) -> FunctionResult:
    if value in (None, ''):
        return FunctionResult(RESULT_ERR, ValErrorEntry(_ERR_REQUIRED, 'Missing required value'))
    else:
        return FunctionResult(RESULT_OK, value)


def _val_string(value: Any, min_len: Optional[int] = None, max_len: Optional[int] = None, ends_with: Optional[str] = None, starts_with: Optional[str] = None,
                strip_html: bool = False) -> FunctionResult:
    if value is None:
        return FunctionResult(RESULT_OK, None)
    if not isinstance(value, str):
        return FunctionResult(RESULT_ERR, ValErrorEntry(_ERR_STR_NOT_STRING, f'Value not a string'))
    if min_len and len(value) < min_len:
        return FunctionResult(RESULT_ERR, ValErrorEntry(_ERR_STR_TOO_SHORT, f'Value is too short. Min length {min_len}'))
    if max_len and len(value) > max_len:
        return FunctionResult(RESULT_ERR, ValErrorEntry(_ERR_STR_TOO_LONG, f'Value is too long. Max length {max_len}'))
    if ends_with and not value.endswith(ends_with):
        return FunctionResult(RESULT_ERR, ValErrorEntry(_ERR_STR_NOT_ENDS_WITH, f'Incorrect value. Value not ends with {ends_with}'))
    if starts_with and not value.startswith(starts_with):
        return FunctionResult(RESULT_ERR, ValErrorEntry(_ERR_STR_NOT_STARTS_WITH, f'Incorrect value. Value not starts with {starts_with}'))
    if strip_html:
        value = remove_tags(value)
    return FunctionResult(RESULT_OK, value)


def _val_email(value: Any, domain: Optional[str] = None) -> FunctionResult:
    if value is None:
        return FunctionResult(RESULT_OK, None)

    result = _EMAIL_RE.match(value)
    value = result.string if result else None
    if value:
        if domain and not value.endswith(domain):
            return FunctionResult(RESULT_ERR, ValErrorEntry(_ERR_EMAIL_DOMAIN, 'Not valid domain'))
        return FunctionResult(RESULT_OK, value)
    else:
        return FunctionResult(RESULT_ERR, ValErrorEntry(_ERR_EMAIL_NOT_VALID, 'Not valid email address'))


def _val_phone(value: Any, country: Optional[str] = None) -> FunctionResult:
    if value is None:
        return FunctionResult(RESULT_OK, None)
    if parse_phone_number(value, country == 'POL') is None:
        return FunctionResult(RESULT_ERR, ValErrorEntry(_ERR_PHONE_FORMAT, 'Invalid phone format'))
    else:
        return FunctionResult(RESULT_OK, value)


def _val_date(value: Any, remove_offset: bool = True, before_date: Optional[datetime] = None, after_date: Optional[datetime] = None) -> FunctionResult:
    if value is None:
        return FunctionResult(RESULT_OK, None)
    v = parse_date(value)
    if v is None:
        return FunctionResult(RESULT_ERR, ValErrorEntry(_ERR_DATE_FORMAT, "Not valid date format"))
    else:
        if remove_offset:
            v = v.replace(tzinfo=None)
        if before_date and before_date < v:
            return FunctionResult(RESULT_ERR, ValErrorEntry(_ERR_DATE_BEFORE, f'Date has to be before {before_date}'))
        if after_date and after_date > v:
            return FunctionResult(RESULT_ERR, ValErrorEntry(_ERR_DATE_AFTER, f'Date has to be after {after_date}'))

        return FunctionResult(RESULT_OK, v)


def _val_numeric(value: Any, parser: Callable[[str, Optional[Union[float, int]]], Optional[Union[float, int]]], min_val: Optional[Union[int, float]] = None,
                 max_val: Optional[Union[int, float]] = None) -> FunctionResult:
    if value is None:
        return FunctionResult(RESULT_OK, None)
    v = parser(value, None)
    if v is None:
        return FunctionResult(RESULT_ERR, ValErrorEntry(_ERR_NUMBER_FORMAT, "Invalid number format"))
    else:
        if min_val is not None and v < min_val:
            return FunctionResult(RESULT_ERR, ValErrorEntry(_ERR_NUMBER_TOO_SMALL, f'Cannot be smaller than {min_val}'))
        if max_val is not None and v > max_val:
            return FunctionResult(RESULT_ERR, ValErrorEntry(_ERR_NUMBER_TOO_BIG, f'Cannot be bigger than {max_val}'))
        return FunctionResult(RESULT_OK, v)


def _val_fn(value: Any, fn: Callable[[str], Optional[Any]]) -> FunctionResult:
    if value is None:
        return FunctionResult(RESULT_OK, None)
    v = fn(value)
    if v is None:
        return FunctionResult(RESULT_ERR, ValErrorEntry(_ERR_INVALID_VALUE, 'Incorrect value'))
    else:
        return FunctionResult(RESULT_OK, v)


def _val_list_of_items(value: Any, parser: Callable[[str], Optional[Union[float, int]]], min_length: int = None, max_length: int = None) -> FunctionResult:
    if value is None:
        return FunctionResult(RESULT_OK, None)

    if not isinstance(value, (list, tuple)):
        return FunctionResult(RESULT_ERR, ValErrorEntry(_ERR_EXPECTED_LIST, 'Expected list'))
    elif min_length and min_length > len(value):
        return FunctionResult(RESULT_ERR, ValErrorEntry(_ERR_LIST_TOO_SHORT, f'List too short. Required at least {min_length} elements'))
    else:
        out = []
        for i, e in enumerate(value):
            e = parser(e, None)
            if e is None:
                return FunctionResult(RESULT_ERR, ValErrorEntry(_ERR_LIST_VALUE_ERROR, f'Invalid value at position {i}'))
            else:
                out.append(e)
        if max_length and len(out) > max_length:
            return FunctionResult(RESULT_OK, ValErrorEntry(_ERR_LIST_TOO_BIG, f'List too long. Expected maximum {max_length} elements'))
        return FunctionResult(RESULT_OK, out)


def _val_value_in(value: Any, available: Optional[Union[List, Tuple]] = None) -> FunctionResult:
    if value is None or available is None:
        return FunctionResult(RESULT_OK, None)
    if value in available:
        return FunctionResult(RESULT_OK, value)
    else:
        s = ', '.join(available)
        return FunctionResult(RESULT_ERR, ValErrorEntry(_ERR_VALUE_IN, f'Incorrect value. Expected {s}'))


ADDR_REQ_CITY = 1
ADDR_REQ_COUNTRY = 2
ADDR_REQ_STREET = 4
ADDR_REQ_DISTRICT = 8


def _val_address(value: Any, required: int = ADDR_REQ_CITY & ADDR_REQ_COUNTRY) -> FunctionResult:
    if value is None or required == 0:
        return FunctionResult(RESULT_OK, None)

    if not isinstance(value, dict):
        return FunctionResult(RESULT_ERR, ValErrorEntry(_ERR_ADDR_FORMAT, 'Invalid format'))
    err = {}
    if required & ADDR_REQ_CITY and value.get('city') is None:
        err['city'] = {'code': _ERR_ADDR_MISSING_CITY, 'message': 'Missing required value'}
    if required & ADDR_REQ_COUNTRY and value.get('country') is None:
        err['country'] = {'code': _ERR_ADDR_MISSING_COUNTRY, 'message': 'Missing required value'}
    if value.get('country') and value.get('country') not in countries_by_alpha3:
        err['country'] = {'code': _ERR_ADDR_COUNTRY, 'message': 'Incorrect value'}
    if required & ADDR_REQ_STREET and value.get('street') is None:
        err['street'] = {'code': _ERR_ADDR_MISSING_STREET, 'message': 'Missing required value'}
    if required & ADDR_REQ_DISTRICT and value.get('district') is None:
        err['district'] = {'code': _ERR_ADDR_MISSING_DISTRICT, 'message': 'Missing required value'}

    if any(err):
        return FunctionResult(RESULT_ERR, err)
    else:
        return FunctionResult(RESULT_OK, value)


_PNG_HEADER = bytes((0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A))
_PNG_TRAILER = bytes((0x49, 0x45, 0x4E, 0x44, 0xAE, 0x42, 0x60, 0x82))


def val_png(contents: bytes) -> FunctionResult:
    if not contents.startswith(_PNG_HEADER):
        return FunctionResult(RESULT_ERR, ValErrorEntry(_ERR_IMG_PNG, 'Invalid PNG file'))
    if not contents.endswith(_PNG_TRAILER):
        return FunctionResult(RESULT_ERR, ValErrorEntry(_ERR_IMG_PNG, 'Invalid PNG file'))
    return FunctionResult(RESULT_OK, None)


_JPEG_HEADER = bytes((0xFF, 0xD8, 0xFF))
_JPEG_TRAILER = bytes((0xFF, 0xD9))


def val_jpg(contents: bytes) -> FunctionResult:
    if not contents.startswith(_JPEG_HEADER):
        return FunctionResult(RESULT_ERR, ValErrorEntry(_ERR_IMG_JPEG, 'Invalid JPEG file'))
    if not contents.endswith(_JPEG_TRAILER):
        return FunctionResult(RESULT_ERR, ValErrorEntry(_ERR_IMG_JPEG, 'Invalid JPEG file'))

    if 0xE0 <= contents[3] <= 0xE8:
        return FunctionResult(RESULT_OK, None)
    else:
        return FunctionResult(RESULT_ERR, ValErrorEntry(_ERR_IMG_JPEG, 'Invalid JPEG file'))


_GIF_HEADER = bytes((0x47, 0x49, 0x46, 0x38))
_GIF_TRAILER = bytes((0x00, 0x3B))


def val_gif(contents: bytes) -> FunctionResult:
    if not contents.startswith(_GIF_HEADER):
        return FunctionResult(RESULT_ERR, ValErrorEntry(_ERR_IMG_GIF, 'Invalid GIF file'))
    if not contents.endswith(_GIF_TRAILER):
        return FunctionResult(RESULT_ERR, ValErrorEntry(_ERR_IMG_GIF, 'Invalid GIF file'))

    if contents[4] not in (0x37, 0x39) or contents[5] != 0x61:
        return FunctionResult(RESULT_ERR, ValErrorEntry(_ERR_IMG_GIF, 'Invalid GIF file'))


class _ImageValidator:
    PNG = 1
    JPEG = 2
    GIF = 4

    _IMG_VAL = {
        b'jpeg': val_jpg,
        b'gif': val_gif,
        b'png': val_png,
    }

    _IMG_TYPES = {
        b'jpeg': JPEG,
        b'jpg': JPEG,
        b'gif': GIF,
        b'png': PNG,
    }

    class Image(NamedTuple):
        img_type: str
        img_contents: bytes

    @classmethod
    def validate(cls, contents: Union[bytes, str], types: int = PNG + JPEG + GIF) -> FunctionResult:
        if not contents:
            return FunctionResult(RESULT_OK, None)
        if isinstance(contents, str):
            contents = bytes(contents, 'ascii')
        header = bytes(memoryview(contents)[0:36])
        if len(header) != 36:
            return FunctionResult(RESULT_ERR, ValErrorEntry(_ERR_IMG_CONTENT_TOO_SHORT, 'Not valid data contents. Content too short'))
        if not header.startswith(b'data:image/'):
            return FunctionResult(RESULT_ERR, ValErrorEntry(_ERR_IMG_MISSING_HEADER, 'Not valid data contents. Expected image data'))
        img = header.split(b';', 1)
        if len(img) != 2:
            return FunctionResult(RESULT_ERR, ValErrorEntry(_ERR_IMG_MISSING_HEADER, 'Not valid data contents. Expected image data'))

        image_type = img[0][11:]
        val = cls._IMG_VAL.get(image_type)
        it = cls._IMG_TYPES.get(image_type)
        if not it or it & types == 0 or not val:
            formats = []
            if types & cls.PNG:
                formats.append('PNG')
            if types & cls.JPEG:
                formats.append('JPEG')
            if types & cls.GIF:
                formats.append('GIF')
            formats = ', '.join(formats)
            return FunctionResult(RESULT_ERR, ValErrorEntry(_ERR_IMG_TYPE, f'Unknown image type. Expected {formats}'))

        if not img[1].startswith(b'base64,'):
            return FunctionResult(RESULT_ERR, ValErrorEntry(_ERR_IMG_CONTENT, 'Unknown image type. Expected base64 contents'))

        content_start = len(img[0]) + 8
        try:
            image_bytes = base64_decode(contents[content_start:])[0]
        except (TypeError, binascii.Error) as ex:
            return FunctionResult(RESULT_ERR, ValErrorEntry(_ERR_IMG_CONTENT, f'Invalid image contents. {ex}'))

        result = val(image_bytes)
        if result.status == RESULT_OK:
            return FunctionResult(RESULT_OK, cls.Image(img_type=image_type.decode('ascii'), img_contents=image_bytes))
        else:
            return result


def _just_fail(value):
    return FunctionResult(RESULT_ERR, ValErrorEntry(0, "Fail"))


def _just_pass(value):
    return FunctionResult(RESULT_OK, None)


class Val:
    @classmethod
    def required(cls):
        return _FieldValidator(_is_required)

    @classmethod
    def email(cls, domain: str = None):
        return _FieldValidator(_val_email, {'domain': domain})

    @classmethod
    def string(cls, min_len=None, max_len=None, ends_with=None, starts_with=None, strip_html=False):
        return _FieldValidator(_val_string, {
            'min_len': min_len,
            'max_len': max_len,
            'ends_with': ends_with,
            'starts_with': starts_with,
            'strip_html': strip_html
        })

    @classmethod
    def uuid(cls):
        return _FieldValidator(_val_fn, {'fn': parse_uuid})

    @classmethod
    def objectId(cls):
        return _FieldValidator(_val_fn, {'fn': parse_objectId})

    @classmethod
    def boolean(cls):
        return _FieldValidator(_val_fn, {'fn': parse_bool})

    @classmethod
    def phone(cls, country: Optional[str] = None):
        return _FieldValidator(_val_phone, {'country': country})

    @classmethod
    def date(cls, remove_offset: bool = True, before_date: datetime = None, after_date: datetime = None):
        return _FieldValidator(_val_date, {
            'remove_offset': remove_offset,
            'before_date': before_date,
            'after_date': after_date,
        })

    @classmethod
    def number(cls, min_val: Optional[Union[int, float]] = None, max_val: Optional[Union[int, float]] = None, integer: bool = False):
        return _FieldValidator(_val_numeric, {
            'parser': parse_int if integer else parse_float,
            'min_val': min_val,
            'max_val': max_val,
        })

    @classmethod
    def list_of_uuid(cls, min_length: int = None, max_length: int = None):
        return _FieldValidator(_val_list_of_items, {
            'parser': parse_uuid,
            'min_length': min_length,
            'max_length': max_length,
        })

    @classmethod
    def list_of_objectId(cls, min_length: int = None, max_length: int = None):
        return _FieldValidator(_val_list_of_items, {
            'parser': parse_objectId,
            'min_length': min_length,
            'max_length': max_length,
        })

    @classmethod
    def list_of_numbers(cls, integer: bool = False, min_length: int = None, max_length: int = None):
        return _FieldValidator(_val_list_of_items, {
            'parser': parse_int if integer else parse_float,
            'min_length': min_length,
            'max_length': max_length,
        })

    @classmethod
    def list_of_dates(cls, min_length: int = None, max_length: int = None):
        return _FieldValidator(_val_list_of_items, {
            'parser': parse_date,
            'min_length': min_length,
            'max_length': max_length,
        })

    @classmethod
    def list_of_strings(cls, available, min_length: int = None, max_length: int = None):
        return _FieldValidator(_val_list_of_items, {
            'parser': lambda value, default: value if value in available else default,
            'min_length': min_length,
            'max_length': max_length,
        })

    @classmethod
    def values_in(cls, available: Union[List, Set, Tuple]):
        return _FieldValidator(_val_value_in, {'available': available})

    @classmethod
    def image(cls, png: bool = True, jpg: bool = True, gif: bool = True):
        accepted_types = 0
        if png:
            accepted_types += _ImageValidator.PNG
        if jpg:
            accepted_types += _ImageValidator.JPEG
        if gif:
            accepted_types += _ImageValidator.GIF
        return _FieldValidator(_ImageValidator.validate, {
            'types': accepted_types
        })

    @classmethod
    def address(cls, req_city: bool = False, req_country: bool = False, req_street: bool = False, req_district: bool = False):
        required = 0
        if req_city:
            required += ADDR_REQ_CITY
        if req_country:
            required += ADDR_REQ_COUNTRY
        if req_street:
            required += ADDR_REQ_STREET
        if req_district:
            required += ADDR_REQ_DISTRICT

        return _FieldValidator(_val_address, {'required': required})


class ValidationResult:
    __slots__ = ('_results', '_errors')

    def __init__(self) -> None:
        super().__init__()

        self._results = {}
        self._errors = {}

    def add_field(self, field, value):
        self._results[field] = value

    def add_field_error(self, field, value, error):
        self.add_field(field, value)
        self._errors[field] = error

    def has_errors(self):
        return any(self._errors)

    @property
    def errors(self):
        return self._errors

    @property
    def result(self):
        return self._results


class SimpleValidator:
    @classmethod
    def validate(cls, data: Union[VO, Dict], validators: Dict) -> ValidationResult:
        if isinstance(data, VO):
            val_res = cls._va(validators, lambda k: getattr(data, k))
            if not val_res.has_errors():
                data.update_object(val_res.result)
            return val_res
        else:
            return cls._va(validators, lambda k: data.get(k))

    @classmethod
    def _va(cls, validators: Dict, getter: Callable[[str], Any]) -> ValidationResult:
        res = ValidationResult()

        for field in validators.keys():
            value = getter(field)
            if field in validators:
                status, result = cls._validate_field(value, validators[field])
                if status == RESULT_OK:
                    res.add_field(field, result)
                else:
                    res.add_field_error(field, value, result)
            else:
                res.add_field(field, value)

        return res

    @classmethod
    def _validate_field(cls, value: Any, validators: Union[List, Tuple]) -> FunctionResult:
        check_status = True
        errors = []
        for i, v in enumerate(validators):
            if isinstance(v, _FieldValidator):
                result = v.call(value)
            elif callable(v):
                result = v(value)
            else:
                raise ValueError(f'Validator at position {i} of "{v}" is unknown')

            if isinstance(result, FunctionResult):
                if result.status != RESULT_OK:
                    check_status = False
                    if isinstance(result.result, ValErrorEntry):
                        errors.append(_val_err_dict(result.result))
                    else:
                        errors.append(result.result)
                else:
                    value = result.result
            else:
                raise ValueError(f'Validator at position {i} of "{v}" has returned unexpected result')
        if check_status:
            return FunctionResult(RESULT_OK, value)
        else:
            return FunctionResult(RESULT_ERR, errors)
