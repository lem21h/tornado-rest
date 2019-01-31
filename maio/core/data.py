# coding=utf-8
import json
from datetime import date
from typing import Any, Dict, Optional, Tuple, Union
from uuid import UUID, uuid4

from bson import ObjectId


class VO:
    def to_dict(self, fields: Optional[Tuple] = None) -> Dict:
        if fields:
            return {k: self.__getattribute__(k) for k in self.__slots__ if k in fields}
        else:
            return {k: self.__getattribute__(k) for k in self.__slots__}

    @classmethod
    def create(cls):
        return cls()

    @classmethod
    def from_dict(cls, data: Dict[str, Any] = None):
        obj = cls.create()

        if not data or not isinstance(data, dict):
            return obj

        for key in obj.__slots__:
            obj.__setattr__(key, data.get(key))
        return obj

    @classmethod
    def from_object(cls, obj: object):
        new_obj = cls.create()

        if not obj or not isinstance(obj, object):
            return new_obj

        for key in new_obj.__slots__:
            if key in obj.__slots__:
                new_obj.__setattr__(key, obj.__getattribute__(key))

        return new_obj

    @classmethod
    def get_fields(cls):
        return cls.__slots__

    def update_object(self, changes: Dict):
        for key in self.__slots__:
            if key in changes:
                self.__setattr__(key, changes[key])

    def __repr__(self) -> str:
        return str(self.to_dict())

    def __eq__(self, other: Optional[Union[Dict, object]]):
        # fast escape
        if other is None:
            return False
        elif self is other:
            return True

        if isinstance(other, dict):
            if len(other) != len(self.__slots__):
                return False
            for i in self.__slots__:
                if getattr(self, i) != other.get(i):
                    return False
        elif isinstance(other, self.__class__):
            for i in self.__slots__:
                if getattr(self, i) != getattr(other, i):
                    return False
        else:
            return False
        return True


class Document(VO):
    __slots__ = ('uuid',)

    def __init__(self, uuid: Optional[UUID] = None) -> None:
        super().__init__()
        self.uuid = uuid

    @classmethod
    def create(cls):
        obj = super().create()
        obj.uuid = cls.generate_id()

        return obj

    @staticmethod
    def generate_id() -> UUID:
        return uuid4()


class CustomJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (UUID, ObjectId)):
            return str(obj)
        elif isinstance(obj, date):
            return str(obj.isoformat())
        elif isinstance(obj, set):
            return list(obj)
        elif isinstance(obj, bytes):
            return obj.decode()
        elif isinstance(obj, VO):
            return obj.to_dict()
        return json.JSONEncoder.default(self, obj)
