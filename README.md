# ETL Project — 企业级电商数据 ETL 平台

从电商/平台开放 API 抽取数据,经数仓分层(ODS → DWD → DWS → ADS)加工后落地 MySQL,并由 Superset 提供 BI 可视化。全程由 Apache Airflow 编排,具备生产级能力:幂等、可重试、可回填、可观测、可测试。

## 架构总览

```
[电商平台 API / 模拟 API] --httpx+限流--> [ETL SDK: Adapter -> Extractor -> Loader -> DQ]
                                                    |                |
                                          etl_meta(水位/批次/DQ)   MySQL 数仓 ODS→DWD→DWS→ADS
                                                    ^                            |
[Airflow 3.3 (调度/重试/回填)] ----调用 SDK----> DAGs                    Superset 看板
        |__ 失败 -> AlertManager -> 钉钉/企微/邮件
```

核心原则:**Airflow 只做编排,ETL 业务逻辑全部沉淀在独立可测试的 SDK 包中。**

## 目录导航

| 目录 | 说明 |
|---|---|
| [sdk/](sdk/) | ETL SDK(etl_sdk):适配器、抽取、装载、DQ、告警、Airflow 插件 |
| [mock_api/](mock_api/) | FastAPI 模拟电商 API(鉴权/分页/限流/故障注入) |
| [dags/](dags/) | Airflow DAG(薄编排层) |
| [sql/](sql/) | 版本化 DDL/DML(数仓五层 + etl_meta) |
| [airflow/](airflow/) | Airflow 镜像与 dev/prod docker-compose |
| [superset/](superset/) | BI 部署与看板 |
| [deploy/](deploy/) | MySQL 配置、备份脚本、日志轮转 |
| [tests/](tests/) | unit / integration / e2e |
| [docs/](docs/) | 架构文档、ADR、数据字典、运维手册 |
| [scripts/](scripts/) | 回填、初始化、E2E 脚本 |

## 快速开始

```bash
# 1. 安装依赖(虚拟环境只服务 SDK/mock_api 开发测试;Airflow 本体在容器内)
uv sync --all-packages

# 2. 起开发环境(需 Docker Desktop: MySQL 8.4 + 模拟 API;Airflow/Superset 在 M5/M4 加入)
docker compose -f airflow/docker-compose.dev.yaml up -d

# 3. 应用数仓 DDL(版本化,可重复执行)
uv run python scripts/migrate.py

# 4. 运行测试
uv run ruff check . && uv run mypy sdk/src mock_api/src
uv run pytest tests/unit -q          # 单元测试(无外部依赖)
uv run pytest tests/integration -q   # 集成测试(testcontainers 起真实 MySQL)
```

> 注意:Airflow 不支持原生 Windows,开发与生产均在 Docker/Linux 环境运行。
> dev 下 MySQL 映射到宿主端口 13306(3306 被本机 MySQL 服务占用),凭据见 `.env.example`。
> Windows 中文用户名机器需设置用户环境变量 `PYTHONUTF8=1`(本机已设置),否则 docker-py 读取 `~/.docker` 元数据会编码报错。

## 当前进度

- [x] M0 环境准备与项目文档(虚拟环境、README、ADR)
- [x] M1 骨架(uv workspace、SDK 骨架、mock_api 最小版、etl_meta DDL、CI)
- [ ] M2 抽取核心(适配器、水位线、限流、断点续传)
- [ ] M3 装载 + DQ + 告警
- [ ] M4 数仓分层 + BI
- [ ] M5 Airflow 集成
- [ ] M6 故障演练与回填压测
- [ ] M7 生产化

详见 [docs/architecture.md](docs/architecture.md) 与 [docs/adr/](docs/adr/)。
