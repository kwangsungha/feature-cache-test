import json
from collections import defaultdict
import random
from typing import Any
from heapq import heappush, heappop

from base import Location


class Testdata:
    def __init__(self) -> None:
        self.dummy = {}
        with open("dummy.json", encoding="utf-8") as f:
            self.dummy = json.load(f)

        self.kangnam = Location(37.4855495, 127.013712)
        self.boundary_km = 20

        self.restaurants = self.load()
        print(f"There are {len(self.restaurants)} restaurants")

    def load(self) -> dict[str, Any]:
        restaurants = {}
        for line in open("data.csv", encoding="utf-8"):
            data = line.strip().split(",")
            id = data[0]
            loc = Location(float(data[1]), float(data[2]))
            if self.kangnam.distance(loc) > self.boundary_km:
                continue
            restaurants[id] = self.gen(id, loc)

        return restaurants

    def gen(self, id, loc) -> dict[Any, Any]:
        data = self.dummy.copy()
        data["id"] = id
        data["vendor_id"] = id
        data["lat"] = loc.lat
        data["lng"] = loc.lng
        return data

    def gen_test_location(self) -> Location:
        # 10km around kangnam station
        lat = self.kangnam.lat + random.uniform(-0.0113428 * 10, 0.0113428 * 10)
        lng = self.kangnam.lng + random.uniform(-0.008998 * 10, 0.008998 * 10)
        lat = round(lat, 6)
        lng = round(lng, 6)
        return Location(lat, lng)

    def get_nearest_restaurant_keys_from(self, loc: Location, limit=8000) -> set[str]:
        queue = []
        for k, v in self.restaurants.items():
            dist = loc.distance(Location(v["lat"], v["lng"]))
            heappush(queue, (-dist, k))
            if len(queue) > limit:
                heappop(queue)
        return set([k for _, k in queue])

    def fetch(self, keys: set[str]) -> list[tuple[str, Any]]:
        return [(k, self.restaurants[k]) for k in keys]
