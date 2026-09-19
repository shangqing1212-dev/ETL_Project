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

## 3. 数据回填(M5)

```bash
# 回填某时间段(两端含;run_type=backfill,不推进水位,幂等可重跑;M6 验收: 与实时跑结果一致)
uv run python scripts/backfill_airflow.py etl_orders --start 2026-09-10 --end 2026-09-18
uv run python scripts/backfill_airflow.py dws_daily  --start 2026-09-01 --end 2026-09-18 --rerun-failed
# 只重跑失败 run: --rerun-failed(reprocess-behavior=failed);先看将执行什么: --dry-run
# 等效底层命令(Airflow 3.3 起 dags backfill 移除,改用 backfill 命令组):
#   airflow backfill create --dag-id <dag_id> --from-date START --to-date END
```

注意:回填期间 etl_orders 的 dag_run.run_type=backfill,SDK 抽取不推进水位(ADR-003);
dws_daily 回填时构建窗口起点=data_interval_start。回填进度用 `airflow backfill` 查看。

## 3.0.1 Airflow 3.3 运维要点(踩坑记录)

- 组件拆分:api-server(原 webserver 已移除)/ scheduler / **dag-processor**(DAG 解析独立组件,缺失时 scheduler 不加载任何 DAG)
- 新 DAG 默认 **paused**,scheduler 不会调度(包括手动触发的 run):上线后记得 unpause
  (airflow dags list 确认 is_paused;unpause 走 API PATCH /api/v2/dags/{id} 或 UI)
- 分容器部署时任务 worker 默认连 `localhost:8080/execution/` 会 refused,必须显式配置
  `AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://<api-server>:8080/execution/`
- API:任务触发走 `/api/v2/dags/{id}/dagRuns`(v1 已下线);登录用 SAM 的 `/auth/token`
- 回填:`airflow dags backfill` 已移除,用 `airflow backfill create --from-date --to-date`(两端含)

## 3.1 Airflow 登录与密码(M5,SAM)

- UI: dev `http://localhost:18080`(prod 经 nginx https)
- 用户: SAM 默认 `admin:admin`;密码首次启动随机生成,位置:
  `airflow_home` 卷内 `simple_auth_manager_passwords.json.generated`(dev:
  `docker compose -f airflow/docker-compose.dev.yaml exec airflow-webserver cat /opt/airflow/simple_auth_manager_passwords.json.generated`)
- 轮换:编辑该 JSON 文件(webserver 会加载),或按 SAM 文档重建;prod 密码文件持久化挂载,轮换后重启 webserver
- 权限:SAM 角色 viewer/user/op/admin(配置 `[core] simple_auth_manager_users`);生产不把 SAM 暴露公网(nginx IP 白名单兜底)

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
