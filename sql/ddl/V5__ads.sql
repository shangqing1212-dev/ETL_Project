-- V5: ADS 层(BI 物理宽表;insert-overwrite 语义;直接喂 Superset,与数仓解耦)

USE dw;

CREATE TABLE IF NOT EXISTS ads_shop_overview (
  stat_date          DATE         NOT NULL COMMENT '业务日',
  shop_id            BIGINT       NOT NULL,
  platform           VARCHAR(32)  NOT NULL,
  gmv_cents          BIGINT       NOT NULL DEFAULT 0,
  order_cnt          INT          NOT NULL DEFAULT 0,
  buyer_cnt          INT          NOT NULL DEFAULT 0,
  refund_cents       BIGINT       NOT NULL DEFAULT 0,
  avg_order_cents    BIGINT       NOT NULL DEFAULT 0,
  refund_rate        DECIMAL(9,4) NOT NULL DEFAULT 0 COMMENT '退款率=refund/gmv(gmv=0 时为 0)',
  gmv_dod_pct        DECIMAL(12,4) NULL COMMENT 'GMV 日环比 %(无前日数据为 NULL)',
  order_dod_pct      DECIMAL(12,4) NULL COMMENT '订单量日环比 %',
  active_product_cnt INT          NOT NULL DEFAULT 0 COMMENT '动销商品数(当日有销售的商品)',
  product_total_cnt  INT          NOT NULL DEFAULT 0 COMMENT '在库商品总数(dim_product)',
  sell_through_rate  DECIMAL(9,4) NOT NULL DEFAULT 0 COMMENT '动销率=动销/在库(在库=0 时为 0)',
  etl_loaded_at      DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (stat_date, shop_id)
) ENGINE=InnoDB COMMENT='店铺日报宽表 ADS(Superset: 店铺日报看板)'
;

CREATE TABLE IF NOT EXISTS ads_daily_kpi (
  stat_date       DATE         NOT NULL,
  platform        VARCHAR(32)  NOT NULL COMMENT 'KPI 按平台归组(店铺维度聚合)',
  gmv_cents       BIGINT       NOT NULL DEFAULT 0,
  order_cnt       INT          NOT NULL DEFAULT 0,
  buyer_cnt       INT          NOT NULL DEFAULT 0 COMMENT '跨店去重后买家数',
  refund_cents    BIGINT       NOT NULL DEFAULT 0,
  avg_order_cents BIGINT       NOT NULL DEFAULT 0,
  shop_cnt        INT          NOT NULL DEFAULT 0 COMMENT '活跃店铺数(当日有汇总的店铺)',
  gmv_dod_pct     DECIMAL(12,4) NULL,
  etl_loaded_at   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (stat_date, platform)
) ENGINE=InnoDB COMMENT='全局 KPI ADS(Superset: KPI 看板)'
;
