-- V4: DWS 层(日粒度汇总;insert-overwrite 语义: 删除窗口内旧值再重插,天然幂等)

USE dw;

CREATE TABLE IF NOT EXISTS dws_shop_daily (
  stat_date       DATE        NOT NULL COMMENT '业务日(下单日)',
  shop_id         BIGINT      NOT NULL,
  platform        VARCHAR(32) NOT NULL,
  gmv_cents       BIGINT      NOT NULL DEFAULT 0 COMMENT 'GMV=已支付口径实付额(paid/shipped/completed/refunded)',
  order_cnt       INT         NOT NULL DEFAULT 0 COMMENT '有效订单数(同 GMV 口径)',
  buyer_cnt       INT         NOT NULL DEFAULT 0 COMMENT '去重买家数',
  refund_cents    BIGINT      NOT NULL DEFAULT 0 COMMENT '退款额(同口径)',
  avg_order_cents BIGINT      NOT NULL DEFAULT 0 COMMENT '客单价=gmv/order_cnt(取整)',
  etl_loaded_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (stat_date, shop_id),
  KEY idx_dws_shop (shop_id, stat_date)
) ENGINE=InnoDB COMMENT='店铺日汇总 DWS'
;

CREATE TABLE IF NOT EXISTS dws_product_daily (
  stat_date     DATE        NOT NULL,
  shop_id       BIGINT      NOT NULL,
  product_id    VARCHAR(64) NOT NULL,
  platform      VARCHAR(32) NOT NULL,
  sold_qty      INT         NOT NULL DEFAULT 0 COMMENT '销量(件,GMV 口径订单的明细)',
  sales_cents   BIGINT      NOT NULL DEFAULT 0 COMMENT '销售额(分,GMV 口径)',
  etl_loaded_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (stat_date, shop_id, product_id),
  KEY idx_dws_prod (shop_id, product_id, stat_date)
) ENGINE=InnoDB COMMENT='商品日汇总 DWS'
;
