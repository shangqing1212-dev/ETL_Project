"""转换层: ODS→DWD/DWS/ADS 纯函数(polars),无 IO(M4)。

口径定义(数据字典): GMV = 已支付口径(status_norm ∈ paid/shipped/completed/refunded);
stat_date = 下单日;金额一律分;状态枚举见 transforms/orders.py。
"""

from etl_sdk.transforms.ads import build_daily_kpi, build_shop_overview
from etl_sdk.transforms.daily import aggregate_product_daily, aggregate_shop_daily
from etl_sdk.transforms.dim_date import generate_date_rows
from etl_sdk.transforms.dims import ShopDiff, aggregate_products, diff_dim_shop
from etl_sdk.transforms.orders import normalize_status, ods_to_dwd_items, ods_to_dwd_orders

__all__ = [
    "ShopDiff",
    "aggregate_product_daily",
    "aggregate_products",
    "aggregate_shop_daily",
    "build_daily_kpi",
    "build_shop_overview",
    "diff_dim_shop",
    "generate_date_rows",
    "normalize_status",
    "ods_to_dwd_items",
    "ods_to_dwd_orders",
]
