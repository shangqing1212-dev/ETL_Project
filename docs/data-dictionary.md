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

## ODS 层(已落地,见 sql/ddl/V2__ods.sql)

| 表 | 主键 | 说明 |
|---|---|---|
| ods_orders | (shop_id, order_id) | 订单主表镜像,raw_json 完整原样 |
| ods_order_items | (shop_id, order_id, item_id) | 订单明细镜像 |
| ods_products | (shop_id, product_id) | 商品镜像 |
| ods_shops | (shop_id) | 店铺镜像 |

通用 ETL 列:`etl_batch_id`、`etl_loaded_at`、`is_deleted`(软删)。

## DWD 层(清洗 + 统一枚举;事实表自然键 upsert 重算,维表代理键,ADR-005)

| 表 | 主键/唯一键 | 说明 |
|---|---|---|
| dwd_orders | PK (shop_id, order_id) | 订单事实:status_raw(平台原始码)+ status_norm(内部枚举);stat_date 为存储生成列 = DATE(COALESCE(created_at, updated_at)) |
| dwd_order_items | PK (shop_id, order_id, item_id) | 订单明细事实;amount_cents = quantity × price_cents(任一缺失则 NULL) |
| dim_shop | PK shop_key(代理键);UK (shop_id, platform, valid_from) | 店铺维 SCD2:注册表(shops.yaml)属性变化开新版本,valid_from/valid_to/is_current |
| dim_date | PK date_key = YYYYMMDD;UK stat_date | 日期维:周/月/季/周末/节假日(节假日表按国务院通知维护,见 transforms/dim_date.py) |
| dim_product | PK product_key;UK (shop_id, product_id) | 商品维最新态轨(名称取 updated_at 最新;is_active = 近 90 天有出现;快照轨接真实平台后按需扩展) |

内部状态枚举 status_norm:`pending / paid / shipped / completed / cancelled / refunded / unknown`。
归一规则:平台码先查 PLATFORM_STATUS_MAP(真实平台接入时登记差异映射),未登记归 unknown;
派生 refunded = 原始码 shipped/completed 且退款>0 且实付>0。

## DWS 层(日粒度汇总;insert-overwrite 窗口语义)

| 表 | 唯一键 | 指标 |
|---|---|---|
| dws_shop_daily | (stat_date, shop_id) | gmv_cents、order_cnt、buyer_cnt、refund_cents、avg_order_cents |
| dws_product_daily | (stat_date, shop_id, product_id) | sold_qty、sales_cents |

**GMV 口径**(dws/ads 一致):status_norm ∈ {paid, shipped, completed, refunded} 且未软删(is_deleted=0)的订单;
gmv = Σ实付额、order_cnt = 订单数、buyer_cnt = 去重买家数、refund = Σ退款额、客单价 = gmv / order_cnt(整数除法)。
商品表只统计 GMV 口径订单的明细;取消/待支付订单不计入任何指标。

## ADS 层(BI 物理宽表,直接喂 Superset)

| 表 | 唯一键 | 说明 |
|---|---|---|
| ads_shop_overview | (stat_date, shop_id) | 店铺日报宽表:GMV 口径指标 + refund_rate(退款/实付)、gmv_dod_pct/order_dod_pct(日环比 %,无前日为 NULL)、active_product_cnt(动销=当日有销售商品数)、product_total_cnt(在库=dim_product 全量行数)、sell_through_rate(动销率) |
| ads_daily_kpi | (stat_date, platform) | 全局 KPI:跨店汇总(单店铺阶段 buyer_cnt 为店铺加和,多店铺接入后改从 dwd 直接聚合) |

## 数仓构建批(scripts/build_dw.py)

- 驱动源:店铺注册表 dags/config/shops.yaml(与 M5 DAG 展开同源)
- 幂等语义:dwd_*/dim_* upsert 重算;dws_*/ads_* insert-overwrite(先删窗口旧值再重插);
  dim_shop SCD2 由注册表 diff 驱动,属性不变重跑为 no-op;无"构建水位",失败重跑自动重算
- 窗口语义:`--days N` 重建 [today-N+1, today];`--full` 全量(2019-01-01 起);
  窗口只覆盖窗口内业务日,窗口外历史行保留(全量重建才刷新)
- 批次记账:etl_batch run_type=transform(table dw.build / dw.build_kpi),失败发 error 告警

## etl_meta 元数据库

| 表 | 说明 |
|---|---|
| etl_watermark | (table_name, shop_id, platform) → watermark_value/type |
| etl_batch | 批次血缘:batch_id、window、run_type、行数、状态 |
| etl_task_run | 任务运行记录(dag_id/task_id/data_interval/status) |
| etl_load_error | 坏行死信(raw_row JSON + error_type + error_msg) |
| dq_check_def | 数据质量规则定义(规则即配置) |
| dq_check_result | DQ 执行结果(actual/expected/passed/detail,按 batch_id 关联批次) |
| schema_migrations | sql/ 版本化执行记录 |

## DQ 规则口径(规则即配置,sql/dq_rules.yaml → dq_check_def)

| 规则 | 检查范围 | 语义 | 典型参数 |
|---|---|---|---|
| null_rate | 窗口级* | 某列空值率 ≤ max_rate | {column, max_rate} |
| unique | 全表 | 列组合无重复组 | {columns: [...]} |
| range | 窗口级* | 数值列 min/max ∈ [min, max] | {column, min, max} |
| freshness | 窗口级* | 窗口内 max(updated_at) 距 now ≤ max_age_minutes(检出源端停滞) | {max_age_minutes} |
| row_count_delta | etl_batch 历史 | 本批行数 vs 近 N 批成功均值,偏差 ≤ max_deviation | {max_deviation, lookback_batches} |
| referential | 全表 | 子表引用列必须全部命中父表(可带 shop 配对) | {child_column, parent_table, parent_column} |

\* 窗口级 = 本批窗口内落库的行(shop_id+platform 过滤,兼容批次重试);
空窗口/无历史等无意义检查记为 passed 并注明"跳过",不误报。

**死信语义**:映射/契约失败的坏行落 etl_load_error,不中断整批;
死信率超阈值(默认 1%,可配)则中止批次(水位不推进 + error 告警)。
error_type:mapper_error(映射异常)/ schema_mismatch(契约拦截)。

## 字段口径约定(示例,随 M4 补全)

| 字段 | 口径 |
|---|---|
| order_amount | 订单原价合计,**分** |
| payment_amount | 实付金额,**分** |
| refund_amount | 退款金额,**分** |
| stat_date | 自然日(DATE),Asia/Shanghai |
| updated_at | 源端最后更新时间,增量抽取依据 |
