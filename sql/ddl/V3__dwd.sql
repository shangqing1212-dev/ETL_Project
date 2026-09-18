-- V3: DWD 层(清洗 + 统一状态枚举;事实表自然键 upsert 重算,维表代理键,见 ADR-005)
-- 约定: 每条语句以独立一行的 ";" 结尾;金额一律分(BIGINT);时间本地时区(ADR-001/002)

USE dw;

CREATE TABLE IF NOT EXISTS dwd_orders (
  shop_id              BIGINT       NOT NULL COMMENT '店铺ID(多租户维度)',
  order_id             VARCHAR(64)  NOT NULL COMMENT '平台订单号',
  platform             VARCHAR(32)  NOT NULL COMMENT '平台标识(mock/taobao/...)',
  status_raw           VARCHAR(32)  NULL COMMENT '平台原始状态码',
  status_norm          VARCHAR(16)  NOT NULL COMMENT '内部统一状态: pending/paid/shipped/completed/cancelled/refunded/unknown',
  buyer_nick           VARCHAR(128) NULL,
  order_amount_cents   BIGINT       NULL COMMENT '订单原价(分)',
  payment_amount_cents BIGINT       NULL COMMENT '实付(分)',
  refund_amount_cents  BIGINT       NULL COMMENT '退款(分)',
  is_deleted           TINYINT(1)   NOT NULL DEFAULT 0 COMMENT '源端软删标记',
  created_at           DATETIME(3)  NULL COMMENT '下单时间(stat_date 依据)',
  updated_at           DATETIME(3)  NOT NULL COMMENT '源端更新时间(只升不降)',
  stat_date            DATE GENERATED ALWAYS AS (DATE(COALESCE(created_at, updated_at))) STORED COMMENT '业务日=下单日(生成列)',
  etl_batch_id         CHAR(36)     NULL COMMENT '构建批次 UUID',
  etl_loaded_at        DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (shop_id, order_id),
  KEY idx_dwd_orders_date (shop_id, stat_date),
  KEY idx_dwd_orders_upd (shop_id, updated_at)
) ENGINE=InnoDB COMMENT='订单事实 DWD(自然键主键,upsert 重算)'
;

CREATE TABLE IF NOT EXISTS dwd_order_items (
  shop_id      BIGINT       NOT NULL,
  order_id     VARCHAR(64)  NOT NULL,
  item_id      VARCHAR(64)  NOT NULL COMMENT '平台明细ID',
  platform     VARCHAR(32)  NOT NULL,
  product_id   VARCHAR(64)  NULL,
  product_name VARCHAR(255) NULL,
  quantity     INT          NULL,
  price_cents  BIGINT       NULL COMMENT '单价(分)',
  amount_cents BIGINT       NULL COMMENT '行金额=单价×数量(分)',
  updated_at   DATETIME(3)  NOT NULL COMMENT '沿用所属订单的 updated_at',
  etl_batch_id CHAR(36)     NULL,
  etl_loaded_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (shop_id, order_id, item_id),
  KEY idx_dwd_items_prod (shop_id, product_id)
) ENGINE=InnoDB COMMENT='订单明细事实 DWD'
;

CREATE TABLE IF NOT EXISTS dim_shop (
  shop_key      BIGINT       NOT NULL AUTO_INCREMENT COMMENT 'SCD2 代理键',
  shop_id       BIGINT       NOT NULL COMMENT '业务店铺ID(注册表 shops.yaml)',
  platform      VARCHAR(32)  NOT NULL,
  shop_name     VARCHAR(128) NOT NULL,
  status        VARCHAR(16)  NOT NULL DEFAULT 'active' COMMENT 'active/disabled',
  valid_from    DATE         NOT NULL COMMENT '版本生效日(含)',
  valid_to      DATE         NULL COMMENT '版本失效日(不含);NULL=当前',
  is_current    TINYINT(1)   NOT NULL DEFAULT 1,
  etl_loaded_at DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (shop_key),
  UNIQUE KEY uk_dim_shop_ver (shop_id, platform, valid_from)
) ENGINE=InnoDB COMMENT='店铺维 SCD2(注册表驱动,属性变化即开新版本)'
;

CREATE TABLE IF NOT EXISTS dim_date (
  date_key     INT         NOT NULL COMMENT 'YYYYMMDD 智能代理键',
  stat_date    DATE        NOT NULL,
  year_num     SMALLINT    NOT NULL,
  quarter_num  TINYINT     NOT NULL,
  month_num    TINYINT     NOT NULL,
  day_num      TINYINT     NOT NULL,
  week_of_year TINYINT     NOT NULL,
  day_of_week  TINYINT     NOT NULL COMMENT '1=周一 ... 7=周日',
  is_weekend   TINYINT(1)  NOT NULL DEFAULT 0,
  is_holiday   TINYINT(1)  NOT NULL DEFAULT 0 COMMENT '法定节假日(节假日表按国务院当年通知维护)',
  holiday_name VARCHAR(32) NULL,
  PRIMARY KEY (date_key),
  UNIQUE KEY uk_dim_date (stat_date)
) ENGINE=InnoDB COMMENT='日期维(周末/节假日标记;节假日库随 transforms/dim_date.py 维护)'
;

CREATE TABLE IF NOT EXISTS dim_product (
  product_key   BIGINT       NOT NULL AUTO_INCREMENT COMMENT '商品代理键(预留快照轨)',
  shop_id       BIGINT       NOT NULL,
  product_id    VARCHAR(64)  NOT NULL,
  product_name  VARCHAR(255) NULL COMMENT '最新名称(按 updated_at 取最新)',
  first_seen_at DATETIME(3)  NULL COMMENT '首次出现在明细(ODS updated_at 最小)',
  last_seen_at  DATETIME(3)  NULL COMMENT '最近出现时间',
  is_active     TINYINT(1)   NOT NULL DEFAULT 1 COMMENT '当前在库(近 N 天有销售)',
  etl_loaded_at DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (product_key),
  UNIQUE KEY uk_dim_product (shop_id, product_id)
) ENGINE=InnoDB COMMENT='商品维(最新态轨;快照轨接入真实平台后按需扩展)'
;
