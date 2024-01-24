import asyncio
import pathlib
from aiomultiprocess import Pool
from time import perf_counter
from random import sample
import snappy
import pickle

import lmdb


class DocDBCache:
    def __init__(self, lmdb_file="./lmdb", read_only=False):
        self.read_only = read_only
        self.db = lmdb.open(
            lmdb_file,
            max_readers=16,
            map_size=1 * 1024 * 1024 * 1024,
            readonly=read_only,
            lock=True,
            readahead=False,
            meminit=False,
            writemap=True,
            map_async=True,
            sync=True,
        )

    async def mget(self, keys):
        with self.db.begin(write=False) as txn:
            cursor = txn.cursor()
            keys = [k.encode() for k in keys]
            return [
                (k.decode(), pickle.loads(snappy.uncompress(v)))
                for k, v in cursor.getmulti(keys)
            ]

    async def mset(self, keyvalues, ttl):
        if self.read_only:
            return

        with self.db.begin(write=True) as txn:
            for key, value in keyvalues:
                value = snappy.compress(pickle.dumps(value))
                txn.put(key.encode(), value)

    def close(self):
        self.db.close()


async def fetch(ids: list[str]):
    return [
        (
            id,
            {
                "_id": id,
                "vendor_id": id,
                "day": "2024-01-08",
                "vendor_nm": "[jy]맛있어요1호점",
                "logo_exist_yn": True,
                "thumbnail_exist_yn": False,
                "new_tag_mark_start_date": "2023-05-31T00:00:00Z",
                "vendor_new_for_uprank_yn": False,
                "vendor_new_yn": False,
                "franchise_id": 80,
                "vendor_type_cd": "food",
                "yostore_target_yn": False,
                "review_cnt": 2,
                "review_avg_cnt": 5,
                "vendor_contract_yn": True,
                "current_extra_discount_yn": False,
                "extra_discount_amt": 3000,
                "discount_rate": 0,
                "payment_method_code_list": ["creditcard", "online"],
                "vendor_open_yn": True,
                "vendor_oe_yn": False,
                "vendor_online_yn": True,
                "pickup_available_time": 1,
                "test_vendor_yn": False,
                "category_list": ["중식", "한식", "테이크아웃", "카페디저트"],
                "one_dish_threshold": 9000,
                "display_enable_yn": True,
                "delivery_discount_yn": True,
                "pickup_discount_yn": False,
                "preorder_discount_yn": False,
                "vendor_open_for_swimlane_yn": True,
                "hygienic_yn": False,
                "company_no": "3330133333",
                "partner_tenant_cd_list": [],
                "hygiene_grade_cd": None,
                "rank_keyword": None,
                "delivery_order_cnt": 0,
                "takeout_order_cnt": 0,
                "delivery_coupon_yn": False,
                "pickup_coupon_yn": False,
                "delivery_max_coupon_price": 0,
                "pickup_max_coupon_price": 0,
                "created_at": "2024-01-08T11:38:11.409Z",
                "modified_at": "2024-01-08T11:38:11.409Z",
                "vd_recent_eta": -1,
                "vd_recent_eta_expires_at": "2024-01-16T05:30:24.038Z",
            },
        )
        for id in ids
    ]


async def test(args):
    idx, keys = args

    if idx == 0:
        cache = DocDBCache(read_only=False)
    else:
        cache = DocDBCache(read_only=True)
    start_time = perf_counter()

    remain = []
    ret = []

    values = await cache.mget(keys)
    read_time = perf_counter() - start_time

    hit = set()
    for k, v in values:
        hit.add(k)
        ret.append(v)

    remain = list(set(keys) - hit)

    items = await fetch(remain)
    await cache.mset(items, ttl=60)

    total_time = perf_counter() - start_time

    return (len(keys), len(ret), len(remain), total_time, read_time)


async def main() -> None:
    pathlib.Path.unlink(pathlib.Path("./lmdb"), missing_ok=True)

    # 300,000 restaurants
    item_keys = [str(i) for i in range(0, 300000)]

    # expected hit ratio = 33%
    test_keys = [str(i) for i in range(0, 300000 * 3)]

    items = await fetch(item_keys)

    cache = DocDBCache()
    await cache.mset(items, 60)
    print(cache.db.stat())

    testcases = [(i, sample(test_keys, 8000)) for i in range(5)]

    async with Pool(4) as pool:
        async for result in pool.map(test, testcases):
            total, hit, miss, tot_time, read_time = result
            print(
                f"total: {total}, hit: {hit}, miss: {miss}, read: {read_time: .5f}s, total: {tot_time:.5f}s"
            )


if __name__ == "__main__":
    asyncio.run(main())
