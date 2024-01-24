import h3
from typing import Any
from collections import defaultdict

from base import BaseRedisCache, Location


"""
res: 4, key: 112, max: 58881, avg: 2588.7678571428573, tot: 289942
res: 5, key: 499, max: 16837, avg: 581.0460921843687, tot: 289942
res: 6, key: 1484, max: 4611, avg: 195.3787061994609, tot: 289942
"""


class KRingCache(BaseRedisCache):
    def __init__(self, resolution=6, cache_limit=2000, k=1) -> None:
        super().__init__()
        self.resolution = resolution
        self.cache_limit = cache_limit
        self.k = k

        self.last_cached = None

    def cache_key(self, cell):
        return f"{str(self)}_{cell}"

    def cache_keys(self, loc: Location) -> list[str]:
        cell = h3.geo_to_h3(loc.lat, loc.lng, self.resolution)
        return [self.cache_key(k) for k in h3.k_ring(cell, k=self.k)]

    def mget(self, loc: Location, keys: set[str]) -> list[tuple[str, Any]]:
        names = self.cache_keys(loc)
        values = self.db.mget(names)
        self.last_cached = values

        if not values:
            return []

        ret = []
        for value in values:
            if value is None:
                continue
            data = self.serializer.loads(value)
            for k, v in data:
                if k in keys:
                    ret.append((k, v))

        return ret

    def mset(self, loc: Location, values: list[tuple[str, Any]]) -> None:
        if not values:
            return

        duplicate = defaultdict(set)
        caching = defaultdict(list)
        for key, value in values:
            lat, lng = value["lat"], value["lng"]
            cell = self.cache_key(h3.geo_to_h3(lat, lng, self.resolution))
            caching[cell].append((key, value))
            duplicate[cell].add(key)

        names = self.cache_key(loc)
        cached = self.last_cached

        for cell, payload in zip(names, cached):
            if payload is None:
                continue

            data = self.serializer.loads(payload)
            for key, value in data:
                if key in duplicate[cell]:
                    continue
                if len(duplicate[cell]) > self.cache_limit:
                    continue

                caching[cell].append((key, value))

        compressed = {k: self.serializer.dumps(v) for k, v in caching.items()}
        self.db.mset(compressed)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.resolution}, {self.cache_limit})"
