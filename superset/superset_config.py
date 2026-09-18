"""Superset dev 配置(mount 到 /app/pythonpath/)。

生产差异(见 docs/runbook.md M7):
- 元数据库换 PostgreSQL/MySQL,不再用 SQLite;
- 开启 TALISMAN + WTF_CSRF,经 nginx TLS 反代 + IP 白名单对外;
- SECRET_KEY 从密钥管理系统注入。
"""

import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "dev-secret-change-me")

# dev: SQLite 元数据(单机够用,数据落在挂载卷);prod 换外部库
SQLALCHEMY_DATABASE_URI = os.environ.get("SUPERSET_META_DB_URI", "sqlite:////app/superset_home/superset.db")

# dev: 关 CSRF/Talisman,允许 scripts/init_superset.py 走 HTTP API 建看板
WTF_CSRF_ENABLED = False
TALISMAN_ENABLED = False

LANGUAGES = {"zh": {"flag": "cn", "name": "Chinese"}}
