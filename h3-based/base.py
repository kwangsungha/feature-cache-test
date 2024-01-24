from abc import ABC, abstractmethod
from typing import Any
from geopy import distance
from dataclasses import dataclass

import redis
import zlib
import pickle
from aiocache.serializers import BaseSerializer


@dataclass
class Location:
    lat: float
    lng: float

    def distance(self, other) -> float:
        return distance.distance((self.lat, self.lng), (other.lat, other.lng)).km

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.lat:.6f}, {self.lng:.6f})"


class CompressionSerializer(BaseSerializer):
    DEFAULT_ENCODING = None

    def dumps(self, value: Any) -> Any:
        return zlib.compress(pickle.dumps(value))

    def loads(self, value: Any) -> Any:
        if value is None:
            return None
        return pickle.loads(zlib.decompress(value))


class BaseRedisCache(ABC):
    def __init__(self) -> None:
        self.serializer = CompressionSerializer()
        self.db = redis.Redis(host="localhost", port=6379, db=0)

    @abstractmethod
    def mget(self, loc: Location, keys: set[str]) -> list[tuple[str, Any]]:
        ...

    @abstractmethod
    def mset(self, loc: Location, values: list[Any]) -> None:
        ...

    def __str__(self) -> str:
        return self.__class__.__name__
