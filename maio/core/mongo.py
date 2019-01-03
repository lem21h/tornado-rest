# coding=utf-8
from io import BytesIO, StringIO
from typing import Any, AsyncIterable, Awaitable, Dict, List, Optional, Tuple, Type, TypeVar, Union

from bson import ObjectId
from gridfs import GridFS, GridFSBucket
from motor import MotorClient, MotorDatabase, MotorGridFSBucket
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo.collection import Collection
from pymongo.command_cursor import CommandCursor
from pymongo.cursor import Cursor
from pymongo.database import Database
from pymongo.results import DeleteResult, InsertOneResult, UpdateResult

from maio.core.data import Document
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
    def serialize(cls, data: Document) -> Dict[str, Any]:
        raise NotImplementedError()

    @classmethod
    def unserialize(cls, data: Dict[str, Any], clazz: Type[Document]) -> T:
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
    def unserialize(cls, data: Dict[str, Any], clazz: Type[Document]) -> Document:
        obj = clazz.from_dict(data)
        if cls.DB_KEY in data:
            obj.uuid = data[cls.DB_KEY]
        return obj

    def update_object(self, changes: dict):
        for key in self.__slots__:
            if key in changes:
                self.__setattr__(key, changes[key])


class MongoConfig:
    __slots__ = ('uri', 'params', 'database')

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.uri = None
        self.params = None
        self.database = None
        if config:
            self.enrich(config)

    def enrich(self, config: Dict[str, Any]):
        self.uri = config.get('uri', 'mongodb://localhost:27017')
        self.params = config.get('params', {})
        self.database = config.get('database', None)
        if not self.database:
            raise ValueError('Missing database setting')

        return self


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


class MongoFileStorage(object):
    __bucket__ = None
    __bucketName__: str = 'fs'

    @classmethod
    def connection(cls) -> AbstractMongoConnection:
        return DI.get(AbstractMongoConnection.getDIKey())

    @classmethod
    def bucket(cls) -> GridFSBucket:
        if not cls.__bucket__:
            cls.__bucket__ = cls.connection().getFileBucket(cls.__bucketName__)
        return cls.__bucket__

    @classmethod
    def uploadFile(cls, filename: str, file_data: Union[bytes, str], content_type: str, metadata: dict = None) -> ObjectId:
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


class _AbstractMongoRepository(object):
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
    def _in(data: List[Any]) -> Dict[str, List[Any]]:
        return {'$in': data}

    @staticmethod
    def _set(data: Dict) -> Dict[str, Dict]:
        return {'$set': data}

    # aggregate

    @classmethod
    def aggregate(cls, pipeline: List, **kwargs) -> CommandCursor:
        return cls.getCollection().aggregate(pipeline, **kwargs)

    # find methods

    @classmethod
    def findOne(cls, filtering: Dict[str, Any], sort: Optional[List[Tuple[str, int]]] = None,
                projection: Optional[Dict[str, bool]] = None) -> Optional[Union[Awaitable[Dict[str, Any]], Dict[str, Any]]]:
        raise NotImplementedError()

    @classmethod
    def find(cls, filtering=None, sort: Optional[List[Tuple[str, int]]] = None,
             limit: Optional[int] = None, skip: Optional[int] = None, collation: str = 'pl',
             projection: Optional[Dict[str, bool]] = None) -> Cursor:
        raise NotImplementedError()

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
            filters[cls.getIdName()] = cls._in(key_ids)

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


class MongoSyncRepository(_AbstractMongoRepository):
    @classmethod
    def findOne(cls, filtering: Dict[str, Any], sort: Optional[List[Tuple[str, int]]] = None,
                projection: Optional[Dict[str, bool]] = None) -> Optional[Document]:
        result = cls.getCollection().find_one(filtering, sort=sort, projection=projection)
        if result:
            return cls.unserialize(result)

    @classmethod
    def find(cls, filtering: Dict[str, Any] = None, sort: Optional[List[Tuple[str, int]]] = None,
             limit: int = 0, skip: int = 0, collation: str = 'pl', projection: Optional[Dict[str, bool]] = None) -> Union[Document, Dict[str, Any]]:
        cursor = cls.getCollection().find(filtering, projection=projection)
        if limit:
            cursor.limit(limit)
        if skip:
            cursor.skip(skip)
        if collation:
            cursor.collation({'locale': collation, 'caseLevel': False})
        if sort:
            cursor.sort(sort)

        if cls.__serialization__:
            for row in cursor:
                yield cls.unserialize(row)
        else:
            for row in cursor:
                yield row


class MongoAsyncRepository(_AbstractMongoRepository):

    @classmethod
    def findOne(cls, filtering: Dict[str, Any], sort: Optional[List[Tuple[str, int]]] = None,
                projection: Optional[Dict[str, bool]] = None) -> Awaitable[Dict[str, Any]]:
        return cls.getCollection().find_one(filtering, projection=projection, sort=sort)

    @classmethod
    def find(cls, filtering: Dict[str, Any] = None, sort: Optional[List[Tuple[str, int]]] = None,
             limit: int = 0, skip: int = 0, collation: str = 'pl', projection: Optional[Dict[str, bool]] = None) -> AsyncIterable[Dict[str, Any]]:
        cursor = cls.getCollection().find(filtering, projection=projection)
        if limit:
            cursor.limit(limit)
        if skip:
            cursor.skip(skip)

        if sort:
            cursor.sort(sort)

        return cursor


class MongoUtils(object):

    @staticmethod
    def in_match(value_list: List[Any]) -> Dict[str, List]:
        return _AbstractMongoRepository._in(value_list)

    @staticmethod
    def match_date_range(data: Dict[str, Any], field_from: str = None, field_to: str = None) -> Dict[str, Any]:
        date_filtering = {}
        if field_from and isinstance(data.get(field_from), int):
            date_filtering['$gte'] = data[field_from]
        if field_to and isinstance(data.get(field_to), int):
            date_filtering['$lte'] = data[field_to]
        return date_filtering

    @staticmethod
    def match_string(matching: str, options: Optional[str] = 'i', match_from_start=True) -> Dict[str, str]:
        res = {'$regex': f"^{matching}" if match_from_start else matching}
        if options:
            res['$options'] = options
        return res

    def __init__(self, connection: AbstractMongoConnection) -> None:
        super().__init__()
        self._connection = connection

    def drop_collections(self, collections: Union[List, Tuple] = None):
        db = self._connection.getDatabase()
        col_list = collections if collections and any(collections) else db.list_collection_names()
        for collection in col_list:
            db[collection].drop()
