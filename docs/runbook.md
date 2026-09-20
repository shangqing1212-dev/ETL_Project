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

## 4. 数据库备份与恢复(M7)

策略(ADR-006): 每日逻辑备份 + binlog(7 天)+ 生产周备 XtraBackup;每月恢复演练。

```bash
# 每日备份(dev compose;生产由 cron 调度,等价 mysqldump 命令见脚本 docstring)
uv run python scripts/backup_mysql.py --keep 7
uv run python scripts/backup_mysql.py --list

# 每月恢复演练(破坏性: DROP 双库后重灌 + 关键表行数校验)
uv run python scripts/restore_mysql.py --backup backups/etl-YYYYMMDD-HHMMSS.sql.gz
# 演练通过标准: 恢复后 etl_batch/etl_watermark/dwd_orders/dws_shop_daily 行数与备份时点一致;
# 恢复后若由 Airflow 接管,先起 airflow-init 重建 metadata 库授权(见 M5 章节)。
```

binlog 相关: 生产 my.cnf 见 deploy/mysql/my.cnf(log_bin=ROW、7 天保留);PITR 为文档化手工流程,
生产首次使用前须在演练环境验证一次(ADR-006 后果)。

## 4.1 监控(ADR-006: 4 面板)

```bash
# dev 冒烟(输出 Prometheus 文本)
uv run python scripts/etl_metrics_exporter.py --interval 0.01
# 常驻: uv run python scripts/etl_metrics_exporter.py --port 9101
```

面板(Grafana provision 于 deploy/grafana/): DAG 成功率、任务时长 p95、DQ 失败数/死信数、批次/水位延迟。
口径 = etl_meta 单一事实源,排障时可直接 SQL 复现;告警走 AlertManager,Prometheus 不配告警规则(ADR-006)。

## 5. 版本升级(待 M7)

- 构建新 tag 镜像 → 灰度测试 DAG → `docker compose pull && up -d` → 自动 `db migrate` → 失败回滚旧 tag

## 6. 常见故障处置(M6 故障演练验证过)

| 故障 | 症状 | 自动恢复 | 处置 |
|---|---|---|---|
| 平台 5xx | 任务日志 HTTPStatusError | SDK tenacity 指数退避重试 5 次(1s~120s) | 持续失败→查平台侧;批次 failed + error 告警,水位不动,重跑即续 |
| 平台 429 限流 | 日志 RateLimitedError | 读 Retry-After 等待重试;连续 429 自适应降速 20% | 检查配额;调低 ETL_PLATFORM_RATE_LIMIT_* |
| 响应超时/网络抖动 | ReadTimeout/RequestError | 请求级重试(白名单覆盖全部 RequestError) | 网络侧排查;timeout_seconds 按平台 SLA 调 |
| 坏 JSON 页面 | 日志"返回非法 JSON" | 按瞬态重试(DecodingError ∈ RequestError) | 重试耗尽仍失败→批次失败告警,查平台网关 |
| 游标重置 | 翻页重复 | 批内去重 + upsert 幂等吸收,不重不漏 | 无需干预;注意日志 rows_read > 实际行数属正常 |
| 坏行(金额非法/缺字段) | etl_load_error 增长 | 坏行进死信,好行照常装载 | 查 raw_json 定位源端数据问题;死信率超阈值(1%)自动中止 |
| DQ block | 任务失败 + block 告警 | 无(设计为硬失败) | 查 dq_check_result(actual/expected),修数据或调整规则后重跑 |
| 抽取批次失败 | etl_batch status=failed | Airflow retries(3 次) | 水位未动,直接重跑/重触发即安全续跑 |

**回填 30 天参考数据**(2026-09 实测): 30 天 15,021 单 + 15,021 明细,抽取 2m45s;
全量构建(DWD/DWS/ADS)3.8s;回填后与实时链路逐值一致(9-15/9-16 GMV 完全相同)。

**mock 平台分页性能**(M6 修复): 同窗口结果缓存,翻页不再每页重算全窗口 ——
回填大窗口前确认 mock_api 已含该修复(真实平台按平台分页语义,不受影响)。
