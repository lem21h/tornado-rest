# coding=utf-8
from io import BytesIO, StringIO
from typing import Any, AsyncIterable, Awaitable, Dict, List, Optional, Tuple, Type, TypeVar, Union, ClassVar

from bson import ObjectId
from gridfs import GridFS, GridFSBucket
from motor import MotorClient, MotorDatabase, MotorGridFSBucket
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo.collection import Collection
from pymongo.command_cursor import CommandCursor
from pymongo.cursor import Cursor
from pymongo.database import Database
from pymongo.results import DeleteResult, InsertOneResult, UpdateResult

from maio.core.data import Document, VO
from maio.core.di import DI, ApiService

T = TypeVar('T')


class MongoSort:
    __slots__ = ('_field', '_order')
    _L_ASCENDING = 'asc'

    def __init__(self, sort_by_field, order):
        super(MongoSort, self).__init__()
        self._field = sort_by_field
        self._order = order

    def isSet(self):
        if self._field:
            return True
        else:
            return False

    def getSortTuple(self) -> List[Tuple[str, int]]:
        return [(self._field, ASCENDING if self._order == self._L_ASCENDING else DESCENDING)]

    def getSortObject(self) -> Dict[str, int]:
        return {self._field: ASCENDING if self._order == self._L_ASCENDING else DESCENDING}


class AbstractMapper(object):
    DB_KEY = 'id'

    @classmethod
    def serialize(cls, data: Document, fields: Optional[Tuple] = None) -> Dict[str, Any]:
        raise NotImplementedError()

    @classmethod
    def unserialize(cls, data: Dict[str, Any], clazz: Type[T]) -> T:
        raise NotImplementedError()


class DocumentMongoMapper(AbstractMapper):
    DB_KEY = '_id'
    OBJ_KEY = 'uuid'

    @classmethod
    def serialize(cls, data: Document, fields: Optional[Tuple] = None) -> Dict[str, Any]:
        res = data.to_dict(fields)
        res[cls.DB_KEY] = data.uuid
        del res[cls.OBJ_KEY]
        return res

    @classmethod
    def unserialize(cls, data: Dict[str, Any], clazz: Type[T]) -> T:
        obj = clazz.from_dict(data)
        if cls.DB_KEY in data:
            obj.uuid = data[cls.DB_KEY]
        return obj


class MongoConfig(VO):
    __slots__ = ('uri', 'params', 'database')

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.uri = None
        self.params = None
        self.database = None
        if config:
            self.from_dict(config)


class AbstractMongoConnection(ApiService):
    __slots__ = ('_db_client', '_file_client', '_config')

    def __init__(self, config: MongoConfig) -> None:
        super().__init__()
        self._db_client = None
        self._file_client = None

        self._config = config

    @classmethod
    def getDIKey(cls) -> str:
        return 'mongo.connection'

    def getClient(self, refresh: bool = False):
        raise NotImplementedError()

    def getDatabase(self):
        raise NotImplementedError()

    def getFileBucket(self, bucket_name: str = 'fs'):
        raise NotImplementedError()

    def getGridFs(self, collection: str = 'fs'):
        raise NotImplementedError()

    def verify_bucket(self, bucket):
        raise NotImplementedError()

    @classmethod
    def find_one(cls, clazz: ClassVar, filtering: Dict[str, Any], sort: Optional[List[Tuple[str, int]]] = None, projection: Optional[Dict[str, bool]] = None) \
            -> Optional[Any]:
        raise NotImplementedError()

    @classmethod
    def find(cls, clazz: ClassVar, cursor: Cursor) -> Optional[Any]:
        raise NotImplementedError()


class MongoUtils(object):

    @staticmethod
    def match_in(value_list: List[Any]) -> Dict[str, List]:
        return {'$in': value_list}

    @staticmethod
    def match_date_range(data: Dict[str, Any], field_from: str = None, field_to: str = None) -> Dict[str, Any]:
        date_filtering = {}
        if field_from and isinstance(data.get(field_from), int):
            date_filtering['$gte'] = data[field_from]
        if field_to and isinstance(data.get(field_to), int):
            date_filtering['$lte'] = data[field_to]
        return date_filtering

    @staticmethod
    def match_value_between(min_val: Union[int, float], max_val: Union[int, float]):
        return {'$gte': min_val, '$lte': max_val}

    @staticmethod
    def match_less_than(val: Union[int, float], can_equal: bool = True) -> Dict[str, Union[int, float]]:
        return {'$lte' if can_equal else '$le': val}

    @staticmethod
    def match_greater_than(val: Union[int, float], can_equal: bool = True) -> Dict[str, Union[int, float]]:
        return {'$gte' if can_equal else '$ge': val}

    @classmethod
    def match(cls, val: Any) -> Dict[str, Any]:
        return {'$eq': val}

    @staticmethod
    def match_string(matching: str, options: Optional[str] = 'i', match_from_start=True) -> Dict[str, str]:
        res = {'$regex': f"^{matching}" if match_from_start else matching}
        if options:
            res['$options'] = options
        return res

    @classmethod
    def drop_collections(cls, connection: AbstractMongoConnection, collections: Union[List, Tuple] = None):
        db = connection.getDatabase()
        col_list = collections if collections and any(collections) else db.list_collection_names()
        for collection in col_list:
            db[collection].drop()


class MongoFileStorage(object):
    __bucket__ = None
    __grid_fs__ = None
    __bucketName__: str = 'fs'

    @classmethod
    def connection(cls) -> AbstractMongoConnection:
        return DI.get(AbstractMongoConnection.getDIKey())

    @classmethod
    def bucket(cls) -> GridFSBucket:
        if cls.__bucket__ is None or not cls.connection().verify_bucket(cls.__bucket__):
            cls.__bucket__ = cls.connection().getFileBucket(cls.__bucketName__)
        return cls.__bucket__

    @classmethod
    def grid_fs(cls):
        if not cls.__grid_fs__:
            cls.__grid_fs__ = cls.connection().getGridFs(cls.__bucketName__)
        return cls.__grid_fs__

    @classmethod
    def uploadFile(cls, filename: str, file_data: Union[bytes, str], content_type: str, metadata: dict = None) -> Union[ObjectId, Awaitable[ObjectId]]:
        if content_type:
            if metadata:
                metadata['contentType'] = content_type
            else:
                metadata = {
                    'contentType': content_type
                }
        stream = BytesIO(file_data) if isinstance(file_data, bytes) else StringIO(file_data)
        return cls.bucket().upload_from_stream(filename, source=stream, metadata=metadata)

    @classmethod
    def delete(cls, file_id: ObjectId):
        return cls.bucket().delete(file_id)


class MongoRepository:
    __collection__: str = None
    __serialization__: Type[Document] = None
    __mapper__: AbstractMapper = DocumentMongoMapper

    @classmethod
    def DB_ID(cls):
        return cls.__mapper__.DB_KEY

    @classmethod
    def unserialize(cls, result):
        return cls.__mapper__.unserialize(result, cls.__serialization__) if cls.__serialization__ else result

    @classmethod
    def serialize(cls, obj: Document, fields: Optional[Tuple] = None) -> Dict[str, Any]:
        return cls.__mapper__.serialize(obj, fields)

    @classmethod
    def getSerializationClass(cls) -> Type[Document]:
        return cls.__serialization__

    @classmethod
    def connection(cls) -> AbstractMongoConnection:
        return DI.get(AbstractMongoConnection.getDIKey())

    @classmethod
    def getCollection(cls) -> Collection:
        return cls.connection().getDatabase()[cls.__collection__]

    @classmethod
    def dropCollection(cls) -> None:
        cls.getCollection().drop()

    @classmethod
    def getIdName(cls) -> str:
        return cls.__mapper__.DB_KEY

    @staticmethod
    def _set(data: Dict) -> Dict[str, Dict]:
        return {'$set': data}

    # aggregate

    @classmethod
    def aggregate(cls, pipeline: List, **kwargs) -> CommandCursor:
        return cls.getCollection().aggregate(pipeline, **kwargs)

    # find methods

    @classmethod
    def findOne(cls, filtering: Dict[str, Any], sort: Optional[List[Tuple[str, int]]] = None, projection: Optional[Dict[str, bool]] = None) \
            -> Optional[Union[Awaitable[Dict[str, Any]], Document]]:
        return cls.connection().find_one(cls, filtering, sort=sort, projection=projection)

    @classmethod
    def find(cls, filtering=None, sort: Optional[List[Tuple[str, int]]] = None, limit: Optional[int] = None, skip: Optional[int] = None,
             collation: Optional[str] = None, projection: Optional[Dict[str, bool]] = None) -> Cursor:
        cursor = cls.getCollection().find(filtering, projection=projection)
        if limit:
            cursor.limit(limit)
        if skip:
            cursor.skip(skip)
        if collation:
            cursor.collation({'locale': collation, 'caseLevel': False})
        if sort:
            cursor.sort(sort)
        return cls.connection().find(cls, cursor)

    @classmethod
    def findById(cls, key_id: Any, projection: Optional[Dict[str, bool]] = None) -> Optional[Union[Awaitable[Dict[str, Any]], Dict[str, Any]]]:
        return cls.findOne({cls.getIdName(): key_id}, projection=projection)

    @classmethod
    def findByIds(cls, key_ids: List[Any], filters: Optional[Dict[str, Any]] = None,
                  sort: Optional[List[Tuple[str, int]]] = None, limit: int = 0, offset: int = 0) -> Cursor:
        if filters is None:
            filters = {}
        if key_ids and any(key_ids):
            assert isinstance(key_ids, (list, tuple))
            filters[cls.getIdName()] = MongoUtils.match_in(key_ids)

        return cls.find(filters, sort, limit, offset)

    @classmethod
    def findByIdAndUpdate(cls, key_id: Any, changes: Dict[str, Any], return_first: bool = True) -> Optional[Union[Awaitable[Dict[str, Any]], Dict[str, Any]]]:
        return cls.findOneAndUpdateEx({cls.getIdName(): key_id}, cls._set(changes), return_first)

    @classmethod
    def findByIdAndUpdateEx(cls, key_id: Any, update: Dict[str, Any], return_first: bool = True) -> Optional[Union[Awaitable[Dict[str, Any]], Dict[str, Any]]]:
        return cls.findOneAndUpdateEx({cls.getIdName(): key_id}, update, return_first)

    @classmethod
    def findOneAndUpdate(cls, filtering: Dict[str, Any], set_changes: Dict[str, Any],
                         return_first: bool = True) -> Optional[Union[Awaitable[Dict[str, Any]], Dict[str, Any]]]:
        return cls.findOneAndUpdateEx(filtering, cls._set(set_changes), return_first)

    @classmethod
    def findOneAndUpdateEx(cls, filtering: Dict[str, Any], update: Dict[str, Any],
                           return_first: bool = True, upsert: bool = False) -> Optional[Union[Awaitable[Dict[str, Any]], Dict[str, Any]]]:
        r = ReturnDocument.BEFORE if return_first else ReturnDocument.AFTER
        return cls.getCollection().find_one_and_update(filtering, update, return_document=r, upsert=upsert)

    @classmethod
    def count(cls, filtering: Dict[str, Any]) -> Union[Awaitable[int], int]:
        return cls.getCollection().count_documents(filtering)

    # insert

    @classmethod
    def insert(cls, data: Document, enforce_id: bool = True) -> Union[Awaitable[InsertOneResult], InsertOneResult]:
        m_data = cls.__mapper__.serialize(data)

        if not enforce_id and cls.__mapper__.DB_KEY in m_data:
            m_data.pop(cls.__mapper__.DB_KEY)

        return cls.getCollection().insert_one(m_data)

    @classmethod
    def findOneOrInsert(cls, filtering: Dict[str, Any], update: Dict[str, Any],
                        return_first: bool = False) -> Optional[Union[Awaitable[Dict[str, Any]], Dict[str, Any]]]:
        return cls.findOneAndUpdateEx(filtering, update, return_first, True)

    @classmethod
    def updateOrInsert(cls, filtering: Dict[str, Any], set_changes: Dict[str, Any],
                       set_on_insert: Dict[str, Any]) -> Union[Awaitable[UpdateResult], UpdateResult]:
        return cls.updateOrInsertEx(filtering, cls._set(set_changes), set_on_insert)

    @classmethod
    def updateOrInsertEx(cls, filtering: Dict[str, Any], changes: Dict[str, Any],
                         set_on_insert: Dict[str, Any]) -> Union[Awaitable[UpdateResult], UpdateResult]:
        if set_on_insert:
            changes['$setOnInsert'] = set_on_insert
        return cls.getCollection().update_one(filter=filtering, update=changes, upsert=True)

    # update methods

    @classmethod
    def update(cls, key: Any, set_changes: Dict[str, Any]) -> Union[Awaitable[UpdateResult], UpdateResult]:
        return cls.updateEx(key, cls._set(set_changes))

    @classmethod
    def updateEx(cls, key_id: Any, changes: Dict[str, Any]) -> Union[Awaitable[UpdateResult], UpdateResult]:
        return cls.updateOneEx({cls.getIdName(): key_id}, changes)

    @classmethod
    def updateOne(cls, filtering: Dict[str, Any], set_changes: Dict[str, Any]) -> Union[Awaitable[UpdateResult], UpdateResult]:
        return cls.updateOneEx(filtering, cls._set(set_changes))

    @classmethod
    def updateOneEx(cls, filtering: Dict[str, Any], changes: Dict[str, Any]) -> Union[Awaitable[UpdateResult], UpdateResult]:
        return cls.getCollection().update_one(filtering, changes)

    @classmethod
    def updateMany(cls, filtering: Dict[str, Any], set_changes: Dict[str, Any]) -> Union[Awaitable[UpdateResult], UpdateResult]:
        return cls.updateManyEx(filtering, cls._set(set_changes))

    @classmethod
    def updateManyEx(cls, filtering: Dict[str, Any], changes: Dict[str, Any]) -> Union[Awaitable[UpdateResult], UpdateResult]:
        return cls.getCollection().update_many(filtering, changes)

    # delete

    @classmethod
    def delete(cls, key_id: Any) -> Union[Awaitable[DeleteResult], DeleteResult]:
        return cls.getCollection().delete_one({cls.getIdName(): key_id})

    @classmethod
    def deleteOne(cls, filtering: Dict[str, Any]) -> Union[Awaitable[DeleteResult], DeleteResult]:
        return cls.getCollection().delete_one(filtering)

    @classmethod
    def deleteKeys(cls, key_ids: List[Any]) -> Union[Awaitable[DeleteResult], DeleteResult]:
        return cls.getCollection().delete_many({cls.getIdName(): cls._in(key_ids)})

    @classmethod
    def deleteManyEx(cls, filtering: Dict[str, Any]) -> Union[Awaitable[DeleteResult], DeleteResult]:
        return cls.getCollection().delete_many(filtering)

    @classmethod
    def purge(cls) -> Union[Awaitable[DeleteResult], DeleteResult]:
        return cls.getCollection().delete_many({})


class MongoAsyncConnection(AbstractMongoConnection):
    __slots__ = ('_db_client', '_file_client', '_config', '_io_loop')

    def __init__(self, config: MongoConfig, io_loop=None) -> None:
        super().__init__(config)
        self._io_loop = io_loop

    def getClient(self, refresh: bool = False) -> MotorClient:
        if not self._db_client or refresh:
            if self._db_client:
                self._db_client.close()
            self._db_client = MotorClient(self._config.uri, uuidrepresentation='standard', io_loop=self._io_loop)
        return self._db_client

    def getDatabase(self) -> MotorDatabase:
        return self.getClient()[self._config.database]

    def getFileBucket(self, bucket_name: str = 'fs') -> MotorGridFSBucket:
        return MotorGridFSBucket(self.getDatabase(), collection=bucket_name)

    def getGridFs(self, collection: str = 'fs') -> MotorGridFSBucket:
        if not self._file_client:
            self._file_client = MotorGridFSBucket(self.getDatabase(), collection)

    def verify_bucket(self, bucket: MotorGridFSBucket) -> bool:
        return bucket.get_io_loop() == self._io_loop

    @classmethod
    def find_one(cls, clazz: ClassVar, filtering: Dict[str, Any], sort: Optional[List[Tuple[str, int]]] = None, projection: Optional[Dict[str, bool]] = None) \
            -> Awaitable[Dict[str, Any]]:
        return clazz.getCollection().find_one(filtering, projection=projection, sort=sort)

    @classmethod
    def find(cls, clazz: ClassVar, cursor: Cursor) -> AsyncIterable[Dict[str, Any]]:
        return cursor


class MongoSyncConnection(AbstractMongoConnection):
    def getClient(self, refresh: bool = False) -> MongoClient:
        if not self._db_client or refresh:
            if self._db_client:
                self._file_client = None
                self._db_client.close()
            self._db_client = MongoClient(self._config.uri, uuidrepresentation='standard')
        return self._db_client

    def getDatabase(self) -> Database:
        return self.getClient()[self._config.database]

    def getFileBucket(self, bucket_name: str = 'fs') -> GridFSBucket:
        return GridFSBucket(self.getDatabase(), bucket_name=bucket_name)

    def getGridFs(self, collection: str = 'fs', refresh: bool = False) -> GridFS:
        if not self._file_client or refresh:
            self._file_client = GridFS(self.getDatabase(), collection)
        return self._file_client

    def verify_bucket(self, bucket: GridFSBucket) -> bool:
        return True

    @classmethod
    def find_one(cls, clazz: MongoRepository, filtering: Dict[str, Any], sort: Optional[List[Tuple[str, int]]] = None,
                 projection: Optional[Dict[str, bool]] = None) -> Optional[Document]:
        result = clazz.getCollection().find_one(filtering, sort=sort, projection=projection)
        return clazz.unserialize(result) if result else None

    @classmethod
    def find(cls, clazz: MongoRepository, cursor: Cursor):
        if clazz.getSerializationClass():
            for row in cursor:
                yield clazz.unserialize(row)
        else:
            for row in cursor:
                yield row
