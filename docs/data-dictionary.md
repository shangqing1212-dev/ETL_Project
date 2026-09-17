# 数据字典

> 活文档。表结构以 `sql/ddl/` 中的版本化 DDL 为准,此处维护口径与语义说明。
> 全局约定:所有表带 `shop_id` + `platform`;金额一律 BIGINT **单位:分**(ADR-002);时间一律 Asia/Shanghai 本地时间(ADR-001);字符集 utf8mb4。

## 分层语义

| 层 | 更新方式 | 可重放性 |
|---|---|---|
| ODS | upsert(updated_at 只升不降) | 是(可全量重拉) |
| DWD | upsert 重算 | 是(ODS 派生) |
| DWS | insert-overwrite(按 stat_date 窗口) | 是(DWD 派生) |
| ADS | insert-overwrite | 是(DWS 派生) |
| etl_meta | append | 否(运维资产) |

## ODS 层(待 M4 填充完整 DDL)

| 表 | 主键 | 说明 |
|---|---|---|
| ods_orders | (shop_id, order_id) | 订单主表镜像,raw_json 完整原样 |
| ods_order_items | (shop_id, order_id, item_id) | 订单明细镜像 |
| ods_products | (shop_id, product_id) | 商品镜像 |
| ods_shops | (shop_id) | 店铺镜像 |

通用 ETL 列:`etl_batch_id`、`etl_loaded_at`、`is_deleted`(软删)。

## DWD 层

| 表 | 说明 |
|---|---|
| dwd_orders | 订单事实:状态统一枚举(pending/paid/shipped/completed/cancelled/refunded),金额单位为分 |
| dwd_order_items | 订单明细事实 |
| dim_shop | 店铺维(SCD2:valid_from/valid_to/is_current) |
| dim_date | 日期维(含节假日标记) |
| dim_product | 商品维(快照 + 最新态双轨) |

## DWS 层

| 表 | 唯一键 | 指标 |
|---|---|---|
| dws_shop_daily | (stat_date, shop_id) | gmv、订单量、买家数、退款额、客单价 |
| dws_product_daily | (stat_date, shop_id, product_id) | 销量、销售额 |

## ADS 层

| 表 | 说明 |
|---|---|
| ads_shop_overview | 店铺日报宽表(含环比、退款率、动销率),直接喂 Superset |
| ads_daily_kpi | 全局 KPI |

## etl_meta 元数据库

| 表 | 说明 |
|---|---|
| etl_watermark | (table_name, shop_id, platform) → watermark_value/type |
| etl_batch | 批次血缘:batch_id、window、run_type、行数、状态 |
| etl_task_run | 任务运行记录(dag_id/task_id/data_interval/status) |
| etl_load_error | 坏行死信(raw_row JSON + error_type) |
| dq_check_def | 数据质量规则定义(规则即配置) |
| dq_check_result | DQ 执行结果 |
| schema_migrations | sql/ 版本化执行记录 |

## 字段口径约定(示例,随 M4 补全)

| 字段 | 口径 |
|---|---|
| order_amount | 订单原价合计,**分** |
| payment_amount | 实付金额,**分** |
| refund_amount | 退款金额,**分** |
| stat_date | 自然日(DATE),Asia/Shanghai |
| updated_at | 源端最后更新时间,增量抽取依据 |
