from collections import defaultdict
from time import perf_counter
from base import BaseRedisCache, Location
from basic import BasicRedisCache
from data import Testdata
from kringcache import KRingCache


def process(data: Testdata, cache: BaseRedisCache, loc: Location, rids: set[str]):
    start_time = perf_counter()
    values = cache.mget(loc=loc, keys=rids)
    read_time = (perf_counter() - start_time) * 1000

    hit = set(k for k, _ in values)
    remain = rids - hit
    items = data.fetch(remain)

    start_time = perf_counter()
    cache.mset(loc, items)
    write_time = (perf_counter() - start_time) * 1000

    ret = [v for _, v in values] + [v for _, v in items]
    assert len(rids) == len(ret)
    assert rids == set([r["id"] for r in ret])

    return (len(rids), len(hit), len(remain), read_time, write_time)


def main() -> None:
    data = Testdata()

    caches = [
        BasicRedisCache(resolution=5, cache_limit=5000),
        KRingCache(resolution=6, cache_limit=2000, k=1),
        KRingCache(resolution=6, cache_limit=1000, k=1),
    ]

    stats = defaultdict(list)

    for _ in range(10):
        loc = data.gen_test_location()
        rids = data.get_nearest_restaurant_keys_from(loc, limit=8000)
        print(f"{loc}")
        for i, c in enumerate(caches):
            req, hit, miss, read_time, write_time = process(data, c, loc, rids)
            print(
                f"{str(c):<40s}: req: {req}, hit: {hit}, miss: {miss}, ratio: {hit/(req+0.000001):.3f}, read: {read_time: .5f}ms, write: {write_time:.5f}ms"
            )

            stats[i].append((req, hit, miss, read_time, write_time))

        print("")

    for i, stat in stats.items():
        avg_r_time = sum(r for _, _, _, r, _ in stat) / len(stat)
        avg_w_time = sum(w for _, _, _, _, w in stat) / len(stat)
        avg_ratio = sum(h for _, h, _, _, _ in stat) / sum(r for r, _, _, _, _ in stat)

        max_r_time = max(r for _, _, _, r, _ in stat)
        max_w_time = max(w for _, _, _, _, w in stat)

        print(
                f"{str(caches[i]):<40s} - avg_r_time: {avg_r_time:.3f}ms, avg_w_time: {avg_w_time:.3f}ms, avg_hit_ratio: {avg_ratio:.3f}, max_r_time: {max_r_time:.3f}, max_w_time: {max_w_time:.3f}"
        )


if __name__ == "__main__":
    main()
