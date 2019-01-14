# coding=utf-8
from datetime import datetime
from typing import Any, Optional, Union, NamedTuple, Type, Callable, Dict, Awaitable, List, Tuple
from uuid import UUID

from maio.core.helpers import parse_bool, parse_date_to_unix_ts, parse_uuid, parse_int, FunctionResult, parse_float
from maio.core.mongo import MongoAsyncRepository


class AbstractCommand(object):
    @classmethod
    async def execute(cls, *args, **kwargs) -> FunctionResult:
        raise NotImplementedError()


class FieldMapping(NamedTuple):
    field: str
    converter: Callable[[Any], Union[str, Dict]]


class FieldSearch(NamedTuple):
    fields: Tuple
    converter: Callable[[Any], Union[str, Dict]]


class FieldFilter(NamedTuple):
    field_mapping: Union[str, FieldMapping, FieldSearch]
    field_type: Optional[Type] = None
    default: Any = None
    from_query: bool = True


_FIELD_TYPE_MAPPING = {
    bool: parse_bool,
    int: parse_int,
    float: parse_float,
    datetime: parse_date_to_unix_ts,
    UUID: parse_uuid,
}


class ListSort(NamedTuple):
    field: str
    direction: str

    def get_tuple(self):
        return [(self.field, -1 if self.direction == ListBuilder.DESCENDING else 1)]


class ListPagination(NamedTuple):
    limit: int
    offset: int


class ListSerialization(NamedTuple):
    fn_serialize: Callable
    row_serialize: bool
    result_as_map: bool


class ListBuilder:
    ASCENDING = 'asc'
    DESCENDING = 'desc'

    __slots__ = ('_main_clazz', '_filtering', '_pagination', '_sorting', '_serialization', '_projection')

    def __init__(self, clazz) -> None:
        super().__init__()

        self._main_clazz: AbstractListCommand = clazz

        self._filtering: Dict[str, Any] = {}
        self._pagination: ListPagination = None
        self._sorting: ListSort = None
        self._serialization: ListSerialization = None

        self._projection = None

    def with_query(self, query_data):
        self._process_filters(query_data, lambda x, y: x[y][-1].decode('utf-8'), True)

        return self

    def with_filtering(self, filters):
        self._process_filters(filters, lambda x, y: x[y], False)

        return self

    def _process_filters(self, data: Dict[str, Any], mapper: Callable, only_query: bool = False):
        c_filters = self._main_clazz.get_available_filtering()

        if not c_filters or not data:
            return

        if self._filtering is None:
            self._filtering = {}

        for field, v in c_filters.items():
            if (not only_query or v.from_query) and field in data:

                converter = _FIELD_TYPE_MAPPING.get(v.field_type, None) if v.field_type else None
                field_val = mapper(data, field) if converter is None else converter(mapper(data, field), v.default)

                if isinstance(v.field_mapping, str):
                    self._str_filter(v.field_mapping, field_val, None, v.default)
                elif isinstance(v.field_mapping, FieldMapping):
                    self._str_filter(v.field_mapping.field, field_val, v.field_mapping.converter, v.default)
                elif isinstance(v.field_mapping, FieldSearch):
                    self._search_filtering(v.field_mapping.fields, v.field_mapping.converter(field_val) if field_val else v.default)

    def _str_filter(self, db_field, field_val, db_conv, default):
        if field_val is None:
            self._filtering[db_field] = default
        else:
            self._filtering[db_field] = db_conv(field_val) if db_conv and callable(db_conv) else field_val

    def _search_filtering(self, fields, value):
        if value is None:
            return

        or_dict = {}
        for field in fields:
            or_dict[field] = value
        self._filtering['$or'] = [or_dict]

    def with_pagination(self, page: Union[int, str], limit: Union[int, str], per_page: int = 50, max_per_page: int = 100):
        if not page:
            page = 0
        elif isinstance(page, str):
            page = parse_int(page, 1)
        if not limit:
            limit = per_page
        elif isinstance(limit, str):
            limit = parse_int(limit, per_page)

        limit = min(max_per_page, max(1, limit))
        self._pagination = ListPagination(limit, limit * (max(1, page) - 1))
        return self

    def with_sorting(self, field: str, direction: str = ASCENDING):
        default = self._main_clazz.get_default_sorting()

        if not direction or direction not in (self.ASCENDING, self.DESCENDING):
            direction = default[1]

        if not field or field not in self._main_clazz.get_available_sorting():
            field = default[0]

        self._sorting = ListSort(field, direction)
        return self

    def with_projection(self, projection: Dict[str, bool]):
        self._projection = projection
        return self

    def with_serialization(self, fn_serialization: Optional[Callable[[Dict], Any]], row_as_dict: bool = False, as_map: bool = False):
        self._serialization = ListSerialization(fn_serialization, row_as_dict, as_map)

        return self

    @property
    def filtering(self) -> Optional[Dict[str, Any]]:
        return self._filtering

    @property
    def sorting(self) -> Optional[ListSort]:
        return self._sorting

    @property
    def pagination(self) -> Optional[ListPagination]:
        return self._pagination

    @property
    def fn_serialize(self) -> Optional[Callable[[Any], Dict]]:
        return self._serialization.fn_serialize if self._serialization else None

    @property
    def row_as_dict(self) -> bool:
        return self._serialization.row_serialize if self._serialization else False

    @property
    def return_as_map(self) -> bool:
        return self._serialization.result_as_map if self._serialization else False

    @property
    def projection(self) -> Optional[Dict[str, bool]]:
        return self._projection

    def fetch_data(self) -> Awaitable[Union[List, Dict]]:
        return self._main_clazz.execute(self)

    def fetch_with_count(self) -> Awaitable[Tuple]:
        return self._main_clazz.execute_with_count(self)


class AbstractListCommand(object):
    @classmethod
    def get_repo_clazz(cls) -> Type[MongoAsyncRepository]:
        raise NotImplementedError()

    @classmethod
    def get_available_filtering(cls) -> Dict[str, FieldFilter]:
        raise NotImplementedError()

    @classmethod
    def get_default_sorting(cls) -> ListSort:
        raise NotImplementedError()

    @classmethod
    def get_available_sorting(cls) -> Dict[str, str]:
        raise NotImplementedError()

    @classmethod
    def post_process_filtering(cls, filtering):
        return filtering

    @classmethod
    def _get_serializer(cls, fn_serialize: Optional[Callable[[Dict], Any]], row_as_dict: bool) -> Callable[[Dict], Any]:
        if fn_serialize and callable(fn_serialize):
            if row_as_dict:
                def serializer(r):
                    return fn_serialize(r)
            else:
                def serializer(r):
                    return fn_serialize(cls.get_repo_clazz().unserialize(r))
        else:
            if row_as_dict:
                def serializer(r):
                    return r
            else:
                def serializer(r):
                    return cls.get_repo_clazz().unserialize(r)
        return serializer

    @classmethod
    async def execute(cls, builder: ListBuilder) -> Union[List, Dict]:
        params = {}
        if builder.filtering:
            params['filtering'] = cls.post_process_filtering(builder.filtering)
        if builder.sorting:
            params['sort'] = builder.sorting.get_tuple()
        if builder.pagination:
            params['limit'] = builder.pagination.limit
            params['skip'] = builder.pagination.offset
        if builder.projection:
            params['projection'] = builder.projection

        cursor = cls.get_repo_clazz().find(**params)

        serializer = cls._get_serializer(builder.fn_serialize, builder.row_as_dict)
        if builder.return_as_map:
            result = {}
            key_id = cls.get_repo_clazz().getIdName()
            async for row in cursor:
                result[row[key_id]] = serializer(row)
        else:
            result = []
            async for row in cursor:
                result.append(serializer(row))

        return result

    @classmethod
    async def execute_with_count(cls, builder: ListBuilder) -> Tuple[Union[List, Dict], int]:
        result = await cls.execute(builder)
        result_len = len(result)
        if result_len > 0:
            if builder.pagination and (builder.pagination.offset != 0 or result_len >= builder.pagination.limit):
                return result, await cls.get_repo_clazz().count(builder.filtering)
            else:
                return result, result_len
        else:
            return result, 0
