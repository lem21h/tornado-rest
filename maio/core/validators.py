# coding=utf-8
import re
from datetime import datetime
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


def _is_required(value):
    if value in (None, ''):
        return FunctionResult(RESULT_ERR, 'Missing required value')
    else:
        return FunctionResult(RESULT_OK, value)


def _val_string(value, min_len: Optional[int] = None, max_len: Optional[int] = None, ends_with=None, starts_with=None, strip_html=False):
    if value is None:
        return FunctionResult(RESULT_OK, None)
    if not isinstance(value, str):
        return FunctionResult(RESULT_ERR, 'Incorrect value. Not a string')
    if min_len and len(value) < min_len:
        return FunctionResult(RESULT_ERR, 'Incorrect value. Value is too short')
    if max_len and len(value) > max_len:
        return FunctionResult(RESULT_ERR, 'Incorrect value. Value is too long')
    if ends_with and not value.endswith(ends_with):
        return FunctionResult(RESULT_ERR, f'Incorrect value. Value not ends with {ends_with}')
    if starts_with and not value.startswith(starts_with):
        return FunctionResult(RESULT_ERR, f'Incorrect value. Value not starts with {starts_with}')
    if strip_html:
        value = remove_tags(value)
    return FunctionResult(RESULT_OK, value)


def _val_email(value, domain: str = None):
    if value is None:
        return FunctionResult(RESULT_OK, None)

    result = _EMAIL_RE.match(value)
    value = result.string if result else None
    if value:
        if domain and not value.endswith(domain):
            return FunctionResult(RESULT_ERR, 'Domain not match')
        return FunctionResult(RESULT_OK, value)
    else:
        return FunctionResult(RESULT_ERR, 'Not valid email')


def _val_phone(value, country: str = None):
    if value is None:
        return FunctionResult(RESULT_OK, None)
    if parse_phone_number(value, country == 'POL') is None:
        return FunctionResult(RESULT_ERR, 'Invalid format')
    else:
        return FunctionResult(RESULT_OK, value)


def _val_date(value, remove_offset=True, before_date=None, after_date=None):
    if value is None:
        return FunctionResult(RESULT_OK, None)
    v = parse_date(value)
    if v is None:
        return FunctionResult(RESULT_ERR, "Not valid date format")
    else:
        if remove_offset:
            v = v.replace(tzinfo=None)
        if before_date and before_date < v:
            return FunctionResult(RESULT_ERR, f'Date has to be before {before_date}')
        if after_date and after_date > v:
            return FunctionResult(RESULT_ERR, f'Date has to be after {after_date}')

        return FunctionResult(RESULT_OK, v)


def _val_numeric(value, parser, min_val: Optional[Union[int, float]] = None, max_val: Optional[Union[int, float]] = None):
    if value is None:
        return FunctionResult(RESULT_OK, None)
    v = parser(value, None)
    if v is None:
        return FunctionResult(RESULT_ERR, "Incorrect value")
    else:
        if min_val is not None and v < min_val:
            return FunctionResult(RESULT_ERR, f'Incorrect value. Cannot be smaller than {min_val}')
        if max_val is not None and v > max_val:
            return FunctionResult(RESULT_ERR, f'Incorrect value. Cannot be bigger than {max_val}')
        return FunctionResult(RESULT_OK, v)


def _val_fn(value, fn):
    if value is None:
        return FunctionResult(RESULT_OK, None)
    v = fn(value)
    if v is None:
        return FunctionResult(RESULT_ERR, 'Incorrect value')
    else:
        return FunctionResult(RESULT_OK, v)


def _val_list_of_items(value, parser, min_length: int = None, max_length: int = None):
    if value is None:
        return FunctionResult(RESULT_OK, None)

    if not isinstance(value, (list, tuple)):
        return FunctionResult(RESULT_ERR, 'Expected list')
    elif min_length and min_length > len(value):
        return FunctionResult(RESULT_ERR, f'List too short. Required at least {min_length} elements')
    else:
        out = []
        for i, e in enumerate(value):
            e = parser(e, None)
            if e is None:
                return FunctionResult(RESULT_ERR, f'Invalid value at position {i}')
            else:
                out.append(e)
        if max_length and len(out) > max_length:
            return FunctionResult(RESULT_OK, f'List too long. Expected maximum {max_length} elements')
        return FunctionResult(RESULT_OK, out)


def _val_value_in(value, available=None):
    if value is None or available is None:
        return FunctionResult(RESULT_OK, None)
    if value in available:
        return FunctionResult(RESULT_OK, value)
    else:
        s = ', '.join(available)
        return FunctionResult(RESULT_ERR, f'Incorrect value. Expected {s}')


ADDR_REQ_CITY = 1
ADDR_REQ_COUNTRY = 2
ADDR_REQ_STREET = 4
ADDR_REQ_DISTRICT = 8


def _val_address(value, required: int = ADDR_REQ_CITY & ADDR_REQ_COUNTRY):
    if value is None or required == 0:
        return FunctionResult(RESULT_OK, None)

    if not isinstance(value, dict):
        return FunctionResult(RESULT_ERR, 'Invalid format')
    err = {}
    if required & ADDR_REQ_CITY and value.get('city') is None:
        err['city'] = 'Missing value'
    if required & ADDR_REQ_COUNTRY and value.get('country') is None:
        err['country'] = 'Missing value'
    if value.get('country') and value.get('country') not in countries_by_alpha3:
        err['country'] = 'Incorrect value'
    if required & ADDR_REQ_STREET and value.get('street') is None:
        err['street'] = 'Missing value'
    if required & ADDR_REQ_DISTRICT and value.get('district') is None:
        err['district'] = 'Missing value'

    if any(err):
        return FunctionResult(RESULT_ERR, err)
    else:
        return FunctionResult(RESULT_OK, value)


def _just_fail(value):
    return FunctionResult(RESULT_ERR, "Fail")


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
                    errors.append(result.result)
                else:
                    value = result.result
            else:
                raise ValueError(f'Validator at position {i} of "{v}" has returned unexpected result')
        if check_status:
            return FunctionResult(RESULT_OK, value)
        else:
            return FunctionResult(RESULT_ERR, errors)