# 运维手册(Runbook)

> 骨架版本。M6 故障演练后按实际操作回填步骤与截图;M7 生产化后补充生产地址、账号体系、值班安排。

## 0. 环境清单

| 环境 | 位置 | 说明 |
|---|---|---|
| 本地开发 | 开发者 Windows + Docker Desktop | dev compose |
| 生产 | Linux 服务器 | prod compose(待 M7 填写具体主机) |

## 1. 日常巡检(每日)

- [ ] Airflow UI:昨日 DAG 全部 success(重点:etl_orders / dws_daily / dq_monitor)
- [ ] 告警群:无未处理告警
- [ ] `etl_batch` 表:昨日批次状态与延迟
- [ ] MySQL 备份产物已生成(备份脚本日志)

## 2. 告警处置(骨架)

| 告警类型 | 含义 | 处置要点 |
|---|---|---|
| 任务失败 | 抽取/装载/调度异常 | 查 Airflow 任务日志(带 batch_id) |
| DQ block 级失败 | 数据质量问题 | 查 etl_load_error / dq_check_result,决定修复或临时放行 |
| 延迟告警 | 批次未按时完成 | 查水位线推进、限流状态 |

## 3. 数据回填(骨架)

```bash
# 回填某时间段(窗口由 data_interval 给定,不推进水位,幂等可重跑)
bash scripts/backfill.sh <dag_id> <start> <end>
```

## 4. 数据库备份与恢复(待 M7)

- 每日:mysqldump(etl_meta + ADS)
- 每周:全量物理备份(XtraBackup)+ binlog PITR
- 每月:**恢复演练**(步骤待 M7 回填)

## 5. 版本升级(待 M7)

- 构建新 tag 镜像 → 灰度测试 DAG → `docker compose pull && up -d` → 自动 `db migrate` → 失败回滚旧 tag

## 6. 常见故障处置(待 M6 演练后回填)

| 故障 | 症状 | 处置 |
|---|---|---|
| 平台 API 限流加剧 | 任务变慢、429 日志增多 | 检查配额与降速参数 |
| 源端 schema 变更 | DQ 契约失败/坏行激增 | 查 raw_json,更新适配器与 DDL |
| 水位线漂移 | 重复拉取过多 | 查 etl_watermark,必要时手工修正 |
