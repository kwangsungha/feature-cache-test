import h3
from typing import Any

from base import BaseRedisCache, Location


"""
res: 4, key: 112, max: 58881, avg: 2588.7678571428573, tot: 289942
res: 5, key: 499, max: 16837, avg: 581.0460921843687, tot: 289942
res: 6, key: 1484, max: 4611, avg: 195.3787061994609, tot: 289942
"""


class BasicRedisCache(BaseRedisCache):
    def __init__(self, resolution=5, cache_limit=10000) -> None:
        super().__init__()
        self.resolution = resolution
        self.cache_limit = cache_limit
        self.last_cached = None

    def cache_key(self, loc: Location) -> str:
        cell = h3.geo_to_h3(loc.lat, loc.lng, self.resolution)
        return "basic" + cell

    def mget(self, loc: Location, keys: set[str]) -> list[tuple[str, Any]]:
        name = self.cache_key(loc)
        value = self.db.get(name)
        self.last_cached = value
        if not value:
            return []

        values = self.serializer.loads(value)
        return [(k, v) for k, v in values if k in keys]

    def mset(self, loc: Location, values: list[tuple[str, Any]]) -> None:
        if not values:
            return

        name = self.cache_key(loc)

        value = self.last_cached
        if value is None:
            cached = []
        else:
            cached = self.serializer.loads(value)

        will_be_cached = (values + cached)[: self.cache_limit]

        data = self.serializer.dumps(will_be_cached)
        self.db.set(name, data)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.resolution}, {self.cache_limit})"
