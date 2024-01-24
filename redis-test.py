from typing import Any
import asyncio
import pathlib
from aiomultiprocess import Pool
from time import perf_counter
from random import sample
import snappy
import pickle

import aiocache
from aiocache.serializers import BaseSerializer


class CompressionSerializer(BaseSerializer):  # type: ignore
    DEFAULT_ENCODING = None

    def dumps(self, value: Any) -> Any:
        return snappy.compress(pickle.dumps(value))

    def loads(self, value: Any) -> Any:
        if value is None:
            return None
        return pickle.loads(snappy.uncompress(value))


class RedisCacheTest:
    def __init__(self) -> None:
        self.db = aiocache.RedisCache(serializer=CompressionSerializer())

    async def mget(self, keys) -> list[tuple[Any, Any]]:
        ret = await self.db.multi_get(keys)
        return [(k, v) for k, v in zip(keys, ret) if v is not None]

    async def mset(self, pairs: list[tuple[Any, Any]]) -> None:
        await self.db.multi_set(pairs)

    def close(self):
        return


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


async def fetch_small(ids: list[str]):
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
            },
        )
        for id in ids
    ]


async def test(args):
    idx, keys = args

    cache = RedisCacheTest()

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

    items = await fetch_small(remain)
    await cache.mset(items)

    total_time = perf_counter() - start_time

    return (len(keys), len(ret), len(remain), total_time, read_time)


async def main() -> None:
    # 300,000 restaurants
    item_keys = [str(i) for i in range(0, 300000)]

    # expected hit ratio = 33%
    test_keys = [str(i) for i in range(0, 300000 * 3)]

    items = await fetch_small(item_keys)

    cache = RedisCacheTest()
    await cache.mset(items)

    testcases = [(i, sample(test_keys, 8000)) for i in range(5)]

    async with Pool(4) as pool:
        async for result in pool.map(test, testcases):
            total, hit, miss, tot_time, read_time = result
            print(
                f"total: {total}, hit: {hit}, miss: {miss}, read: {read_time: .5f}s, total: {tot_time:.5f}s"
            )


if __name__ == "__main__":
    asyncio.run(main())
