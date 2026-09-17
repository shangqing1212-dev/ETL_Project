-- V1: etl_meta 元数据库(水位线/批次/运行记录/死信/DQ)
-- 约定: 每条语句以独立一行的 ";" 结尾(供 scripts/migrate.py 拆分),文件内不出现过程体/触发器

CREATE DATABASE IF NOT EXISTS etl_meta DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

USE etl_meta;

CREATE TABLE IF NOT EXISTS etl_watermark (
  table_name      VARCHAR(64)  NOT NULL COMMENT '目标表名',
  shop_id         BIGINT       NOT NULL COMMENT '店铺ID(多租户维度)',
  platform        VARCHAR(32)  NOT NULL COMMENT '平台标识(mock/taobao/...)',
  watermark_value DATETIME(3)  NOT NULL COMMENT '水位值(源端 updated_at)',
  watermark_type  VARCHAR(16)  NOT NULL DEFAULT 'updated_at' COMMENT 'updated_at/full',
  updated_at      DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (table_name, shop_id, platform)
) ENGINE=InnoDB COMMENT='增量抽取水位线'
;

CREATE TABLE IF NOT EXISTS etl_batch (
  batch_id     CHAR(36)    NOT NULL COMMENT '批次 UUID',
  table_name   VARCHAR(64) NOT NULL COMMENT '目标表名',
  shop_id      BIGINT      NOT NULL,
  platform     VARCHAR(32) NOT NULL,
  run_type     VARCHAR(16) NOT NULL COMMENT 'incremental/full/backfill',
  window_start DATETIME(3) NULL,
  window_end   DATETIME(3) NULL,
  status       VARCHAR(16) NOT NULL DEFAULT 'running' COMMENT 'running/success/failed',
  rows_read    BIGINT      NOT NULL DEFAULT 0,
  rows_written BIGINT      NOT NULL DEFAULT 0,
  error_msg    TEXT        NULL,
  started_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  ended_at     DATETIME(3) NULL,
  PRIMARY KEY (batch_id),
  KEY idx_batch_table (table_name, started_at)
) ENGINE=InnoDB COMMENT='抽取批次(血缘与排障中枢)'
;

CREATE TABLE IF NOT EXISTS etl_task_run (
  run_id              CHAR(36)     NOT NULL COMMENT 'SDK 层任务运行 UUID',
  dag_id              VARCHAR(128) NOT NULL,
  task_id             VARCHAR(128) NOT NULL,
  data_interval_start DATETIME(3)  NULL,
  data_interval_end   DATETIME(3)  NULL,
  status              VARCHAR(16)  NOT NULL COMMENT 'running/success/failed',
  error               TEXT         NULL,
  duration_ms         BIGINT       NULL,
  started_at          DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  ended_at            DATETIME(3)  NULL,
  PRIMARY KEY (run_id),
  KEY idx_task_dag (dag_id, task_id, started_at)
) ENGINE=InnoDB COMMENT='SDK 层任务运行记录(独立于 Airflow 内部表)'
;

CREATE TABLE IF NOT EXISTS etl_load_error (
  error_id   BIGINT      NOT NULL AUTO_INCREMENT COMMENT '死信 ID',
  batch_id   CHAR(36)    NULL,
  table_name VARCHAR(64) NOT NULL,
  raw_row    JSON        NULL COMMENT '原始坏行',
  error_type VARCHAR(64) NOT NULL COMMENT 'type_error/pk_missing/schema_mismatch/...',
  error_msg  TEXT        NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (error_id),
  KEY idx_error_table (table_name, created_at)
) ENGINE=InnoDB COMMENT='装载坏行死信表'
;

CREATE TABLE IF NOT EXISTS dq_check_def (
  check_id   BIGINT      NOT NULL AUTO_INCREMENT,
  table_name VARCHAR(64) NOT NULL,
  rule_type  VARCHAR(32) NOT NULL COMMENT 'null_rate/unique/range/freshness/row_count_delta/referential/custom_sql',
  params     JSON        NOT NULL COMMENT '规则参数(JSON)',
  severity   VARCHAR(16) NOT NULL DEFAULT 'warn' COMMENT 'warn/block',
  enabled    TINYINT(1)  NOT NULL DEFAULT 1,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (check_id),
  KEY idx_check_table (table_name, enabled)
) ENGINE=InnoDB COMMENT='数据质量规则定义(规则即配置)'
;

CREATE TABLE IF NOT EXISTS dq_check_result (
  result_id      BIGINT       NOT NULL AUTO_INCREMENT,
  check_id       BIGINT       NOT NULL,
  batch_id       CHAR(36)     NULL,
  stat_date      DATE         NULL,
  actual_value   VARCHAR(255) NULL COMMENT '实际值',
  expected_value VARCHAR(255) NULL COMMENT '期望/阈值',
  passed         TINYINT(1)   NOT NULL,
  detail         TEXT         NULL COMMENT '失败样例/明细',
  evaluated_at   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (result_id),
  KEY idx_result_check (check_id, evaluated_at),
  KEY idx_result_batch (batch_id)
) ENGINE=InnoDB COMMENT='数据质量检查结果'
;
