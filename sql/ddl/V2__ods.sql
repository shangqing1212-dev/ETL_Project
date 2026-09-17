-- V2: ODS 层(与 API 实体 1:1 镜像,upsert 型,幂等落地)
-- 约定: 每条语句以独立一行的 ";" 结尾;金额一律分(BIGINT,ADR-002);时间统一本地时区(ADR-001)

CREATE DATABASE IF NOT EXISTS dw DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

USE dw;

CREATE TABLE IF NOT EXISTS ods_orders (
  shop_id              BIGINT       NOT NULL COMMENT '店铺ID(多租户维度)',
  order_id             VARCHAR(64)  NOT NULL COMMENT '平台订单号',
  platform             VARCHAR(32)  NOT NULL COMMENT '平台标识(mock/taobao/...)',
  order_status         VARCHAR(32)  NULL COMMENT '平台原始状态码',
  buyer_nick           VARCHAR(128) NULL,
  order_amount_cents   BIGINT       NULL COMMENT '订单金额(分)',
  payment_amount_cents BIGINT       NULL COMMENT '实付金额(分)',
  refund_amount_cents  BIGINT       NULL COMMENT '退款金额(分)',
  raw_json             JSON         NULL COMMENT '原始报文(抵御 schema 漂移)',
  is_deleted           TINYINT(1)   NOT NULL DEFAULT 0 COMMENT '源端软删标记',
  created_at           DATETIME(3)  NULL,
  updated_at           DATETIME(3)  NOT NULL COMMENT '源端更新时间(增量依据,只升不降)',
  etl_batch_id         CHAR(36)     NULL COMMENT '装载批次 UUID',
  etl_loaded_at        DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (shop_id, order_id),
  KEY idx_ods_orders_upd (shop_id, updated_at)
) ENGINE=InnoDB COMMENT='订单 ODS'
;

CREATE TABLE IF NOT EXISTS ods_order_items (
  shop_id       BIGINT       NOT NULL,
  order_id      VARCHAR(64)  NOT NULL,
  item_id       VARCHAR(64)  NOT NULL COMMENT '平台明细ID',
  platform      VARCHAR(32)  NOT NULL,
  product_id    VARCHAR(64)  NULL,
  product_name  VARCHAR(255) NULL,
  quantity      INT          NULL,
  price_cents   BIGINT       NULL COMMENT '单价(分)',
  raw_json      JSON         NULL,
  updated_at    DATETIME(3)  NOT NULL COMMENT '沿用所属订单的 updated_at',
  etl_batch_id  CHAR(36)     NULL,
  etl_loaded_at DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (shop_id, order_id, item_id),
  KEY idx_ods_items_upd (shop_id, updated_at)
) ENGINE=InnoDB COMMENT='订单明细 ODS'
;
