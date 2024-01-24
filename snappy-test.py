import pickle
import snappy
import zlib
import orjson as json
import msgspec
from time import perf_counter


class Restaurant(msgspec.Struct):
    id: int

    distance: float = 0.0
    min_delivery_fee: int = 0
    max_delivery_fee: int = 0
    estimated_delivery_time: int = 0
    order_count: int = 0
    review_count: int = 0
    review_avg: float = 0.0

    is_ypx: bool = False
    previously_ordered: bool = False
    mov: int = 0
    is_hygienic: bool = False
    hygiene_grade: int = -1  # good=0, great=1, excellent=2
    min_pickup_time: int = 999

    # channelyo filter
    franchise_id: int | None = None
    partner_tenant_ids: list[str] = []
    is_yostore: bool = False
    is_test: bool = False
    payment_methods: list[str] | None = []

    # feature
    categories: list[str] = []
    score: float = 0.0
    rank_keyword: str | None = None
    taste_list: list[str] = []
    taste_ratios: list[float] = []
    label: dict[str, float] = {}
    search_count: int = 0
    discount_percent: float = 0.0
    has_discount: bool = False  # additional_discount 까지 고려된 값
    max_coupon_price: float = 0.0
    extra_discount_amt: float = 0.0
    aov: int = 0
    cc_categories: list[str] = []
    cc_scores: list[float] = []

    # metadata
    open: bool = False
    open_for_swimlane: bool = False
    is_displayable: bool = False
    new: bool = False
    company_number: str | None = None
    has_thumbnail: bool = False

    vd_recent_eta: int = -1
    vd_aggregated_eta: int = -1


def fetch(id):
    return Restaurant(id=id)
    # return {"_id" : id, "vendor_id" : id, "day" : "2024-01-08", "vendor_nm" : "[jy]맛있어요1호점", "logo_exist_yn" : True, "thumbnail_exist_yn" : False, "new_tag_mark_start_date" : "2023-05-31T00:00:00Z", "vendor_new_for_uprank_yn" : False, "vendor_new_yn" : False, "franchise_id" : 80, "vendor_type_cd" : "food", "yostore_target_yn" : False, "review_cnt" : 2, "review_avg_cnt" : 5, "vendor_contract_yn" : True, "current_extra_discount_yn" : False, "extra_discount_amt" : 3000, "discount_rate" : 0, "payment_method_code_list" : [ "creditcard", "online" ], "vendor_open_yn" : True, "vendor_oe_yn" : False, "vendor_online_yn" : True, "pickup_available_time" : 1, "test_vendor_yn" : False, "category_list" : [ "중식", "한식", "테이크아웃", "카페디저트" ], "one_dish_threshold" : 9000, "display_enable_yn" : True, "delivery_discount_yn" : True, "pickup_discount_yn" : False, "preorder_discount_yn" : False, "vendor_open_for_swimlane_yn" : True, "hygienic_yn" : False, "company_no" : "3330133333", "partner_tenant_cd_list" : [ ], "hygiene_grade_cd" : None, "rank_keyword" : None, "delivery_order_cnt" : 0, "takeout_order_cnt" : 0, "delivery_coupon_yn" : False, "pickup_coupon_yn" : False, "delivery_max_coupon_price" : 0, "pickup_max_coupon_price" : 0, "created_at" : "2024-01-08T11:38:11.409Z", "modified_at" : "2024-01-08T11:38:11.409Z", "vd_recent_eta" : -1, "vd_recent_eta_expires_at" : "2024-01-16T05:30:24.038Z"}


data = fetch(100000)


start_time = perf_counter()

for _ in range(10000):
    p = pickle.dumps(data, protocol=5)
    s = snappy.compress(p)
    pp = snappy.uncompress(s)
    dd = pickle.loads(pp)
    assert dd == data

end_time = perf_counter()
execution_time = end_time - start_time

print(f"pickle: {len(p)}, pickle+snappy: {len(s)}, elapsed: {execution_time:.8f}s")

"""
start_time = perf_counter()

for _ in range(10000):
    j = json.dumps(data)
    s = snappy.compress(j)
    pp = snappy.uncompress(s)
    dd = json.loads(pp)
    assert dd == data

end_time = perf_counter()
execution_time = end_time - start_time

print(f"json: {len(j)}, json+snappy: {len(s)}, elapsed: {execution_time:.8f}s")
"""


start_time = perf_counter()

for _ in range(10000):
    p = pickle.dumps(data, protocol=5)
    s = zlib.compress(p)
    pp = zlib.decompress(s)
    dd = pickle.loads(pp)
    assert dd == data

end_time = perf_counter()
execution_time = end_time - start_time

print(f"pickle: {len(p)}, pickle+zlib: {len(s)}, elapsed: {execution_time:.8f}s")

"""
start_time = perf_counter()

for _ in range(10000):
    j = json.dumps(data)
    s = zlib.compress(j)
    pp = zlib.decompress(s)
    dd = json.loads(pp)
    assert dd == data

end_time = perf_counter()
execution_time = end_time - start_time

print(f"json: {len(j)}, json+zlib: {len(s)}, elapsed: {execution_time:.8f}s")
"""
