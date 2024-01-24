import asyncio
from typing import Any
from time import perf_counter
from random import sample

from aiocache import caches

caches.set_config(
    {
        "default": {
            "cache": "aiocache.SimpleMemoryCache",
            "serializer": {"class": "aiocache.serializers.PickleSerializer"},
        },
    }
)

cache = caches.get("default")


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


async def set_cache(items) -> None:
    await cache.multi_set(items, ttl=60)


async def check_cache(keys) -> tuple[list[str], list[Any]]:
    remain = []
    ret = []

    values = await cache.multi_get(keys)
    for k, v in zip(keys, values):
        if v is None:
            remain.append(k)
        else:
            ret.append(v)

    return remain, ret


async def main() -> None:
    # 300,000 restaurants
    item_keys = [str(i) for i in range(0, 300000)]

    # expected hit ratio = 33%
    test_keys = [str(i) for i in range(0, 300000 * 3)]

    items = await fetch(item_keys)
    await set_cache(items)

    for _ in range(3):
        testcases = sample(test_keys, 8000)

        start_time = perf_counter()

        remain, ret = await check_cache(testcases)
        items = await fetch(remain)
        await set_cache(items)

        end_time = perf_counter()

        print(f"total: {len(testcases)}, hit: {len(ret)}, miss: {len(remain)}")

        execution_time = end_time - start_time
        print("took {0:.8f}s to execute".format(execution_time))

        print("------------------")


if __name__ == "__main__":
    asyncio.run(main())
