# 架构文档

## 1. 总体架构

```
┌─────────────────────┐        ┌──────────────────────────────────┐
│ 模拟/真实电商平台 API │◄──────│          ETL SDK (etl_sdk)        │
│  - OAuth2/签名鉴权    │        │  adapters  平台适配器(可插拔)     │
│  - 分页(页码/游标)    │ httpx  │  extractors 水位线增量+限流+分页  │
│  - 限流              │ +限流  │  loaders   MySQL 分批幂等 upsert  │
│  - 故障注入(模拟器)   │        │  transforms ODS→DWD/DWS (polars) │
└─────────────────────┘        │  dq        数据质量规则引擎        │
                               │  alerts    钉钉/企微/邮件          │
                               │  airflow   EtlTableOperator 插件  │
                               └────┬─────────────────┬───────────┘
                                    │                 │
                          ┌─────────▼──────┐   ┌──────▼──────────────┐
                          │  etl_meta 元库  │   │  MySQL 数仓          │
                          │  水位/批次/DQ   │   │  ODS→DWD→DWS→ADS    │
                          └────────────────┘   └──────┬─────────────┘
                                                      │
┌──────────────────────┐                      ┌──────▼──────────┐
│ Airflow 3.3 编排层    │──────调用 SDK───────►│  Superset BI    │
│ DAG 薄编排/重试/回填  │                      │  3+ 看板        │
└──────────────────────┘                      └─────────────────┘
```

## 2. 模块职责

| 模块 | 职责 | 关键不变量 |
|---|---|---|
| `etl_sdk.adapters` | 平台接入抽象:鉴权、list+detail 两段式、翻页 | 与 Airflow 零依赖;真实平台 = 实现一个类并注册 |
| `etl_sdk.extractors` | 水位线驱动的增量抽取、令牌桶限流、翻页策略 | 两阶段提交;固定窗口,批内不取 "now" |
| `etl_sdk.loaders` | 分批 upsert、批内去重、坏行死信 | 幂等:同窗口重跑结果不变;updated_at 只升不降 |
| `etl_sdk.transforms` | 纯函数,DataFrame 进出 | 无 IO,可单测 |
| `etl_sdk.dq` | 规则引擎 + pandera 契约,结果落库、按 severity 决策 | 规则即配置(YAML/DB),不改代码可加规则 |
| `etl_sdk.alerts` | 钉钉(加签)/企微/邮件三通道,去重分级 | 告警不阻塞任务,失败仅记日志 |
| `etl_sdk.airflow` | EtlTableOperator、EtlMetaHook | 不访问 Airflow metadata DB |
| `mock_api` | 模拟电商 API,确定性数据 + 故障注入 | 同种子同数据,测试可断言精确值 |
| `dags` | 薄 DAG:参数绑定 + 依赖声明 | 解析期零 DB 访问(3.x 硬约束) |

## 3. 技术选型与理由

| 项 | 选择 | 理由 |
|---|---|---|
| Python | 3.12 | Airflow 3.3 官方镜像基础版本 |
| 依赖管理 | uv workspace | 快、锁可靠;Airflow 本体按官方要求用 pip + constraints |
| HTTP | httpx + ThreadPoolExecutor(8) | 同步双 API;百万行/天不需要 asyncio |
| 数据转换 | polars | 回填场景内存/性能更稳;可降级 pandas |
| DB 访问 | SQLAlchemy 2.x Core + PyMySQL | 裸 SQL executemany 批量写,不用 ORM |
| 重试 | tenacity(请求级) + Airflow retries(任务级) | 分工:瞬时错误 vs 进程崩溃 |
| DQ | pandera + 自研规则引擎 | Great Expectations 对百万级过度设计 |
| 配置 | pydantic-settings,层级:平台默认 < 店铺覆盖 < env | 多租户预留 |
| 日志 | structlog(dev console / prod JSON) | 带 batch_id/shop_id/run_id 上下文 |
| 调度 | Apache Airflow 3.3.x | 2026 年生产主线;2.x 已 EOL |

## 4. 数仓分层

| 层 | 语义 | 更新方式 |
|---|---|---|
| ODS | API 实体 1:1 镜像,raw_json 防 schema 漂移 | upsert(updated_at 只升不降) |
| DWD | 清洗、统一枚举、SCD2 维表(ADR-005: 事实表自然键,维表代理键) | upsert 重算 |
| DWS | 日粒度聚合(GMV 口径见数据字典) | insert-overwrite(DELETE 窗口 + 重插) |
| ADS | BI 出口物理宽表(环比/退款率/动销率) | insert-overwrite |
| etl_meta | 水位/批次/任务/DQ/死信/migration | append |

**构建流水线**(scripts/build_dw.py,店铺注册表 dags/config/shops.yaml 驱动):

```
ods_orders/ods_order_items(按 stat_date 窗口读)
  → dwd_orders/dwd_order_items(状态归一 + 行金额派生,upsert)
  → dim_product(全量明细重算最新态)/ dim_shop(注册表 SCD2 diff)/ dim_date(宽区间幂等填充)
  → dws_shop_daily / dws_product_daily(GMV 口径聚合,窗口 delete + insert)
  → ads_shop_overview(环比需前一日 DWS 行)/ ads_daily_kpi(平台级)
```

幂等三态:dwd/dim 重算收敛、dws/ads 覆盖收敛、SCD2 no-op;每步写 etl_batch(transform 类型),失败 error 告警。转换函数全部在 `etl_sdk.transforms`(polars 纯函数,无 IO),单测覆盖口径。

## 5. BI(Superset)

- 部署:superset/docker-compose.superset.yaml(Superset 6.1 自建镜像 + mysqlclient 驱动;dev SQLite 元数据,生产换 PostgreSQL + 开 CSRF/TLS)
- 初始化:scripts/init_superset.py 走 REST API 幂等创建数据源(MySQL 数仓)→ 3 数据集 → 12 图表 → 3 张看板(店铺日报/全局 KPI/订单明细)
- 验证口径:Superset 图表直接查 ADS/DWD 物理表,数字与数仓 SQL 直查一致(物理表解耦 BI 与数仓,口径变更只需重跑构建)

## 6. 关键机制

- **增量水位线**:`window = [wm - 1h overlap, now - 5min)`,批开始固定;全部成功才推进水位;回填不推进水位。详见 ADR-003。
- **幂等**:业务主键唯一约束 + upsert;DWS/ADS 用 insert-overwrite 天然幂等。
- **限流**:令牌桶按 `平台.端点` 配额,429 扣透支并自适应降速。
- **多租户**:所有表带 shop_id;配置层级合并;DAG 动态任务映射按店铺展开。

## 7. 部署形态

- **dev**:docker-compose 挂载源码热改;MySQL 8.4 容器内;SAM ALL_ADMINS。
- **prod**:源码烘焙进镜像固定 tag;SAM 密码文件 + nginx 反代 + IP 白名单;日志轮转;MySQL 建议独立实例 + 每日备份 + binlog PITR。
