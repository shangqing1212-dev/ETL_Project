# Superset 6.1.0 官方 lean 镜像不含任何数据库驱动。
# 数仓驱动选 PyMySQL(纯 Python wheel,免编译;与 SDK 同驱动):
#   - mysqlclient(MySQLdb)新版无 manylinux wheel,官方镜像 venv 又缺 pip/编译器,源码构建脆弱
#   - SQLAlchemy URI 用 mysql+pymysql:// 即可;cryptography 满足 MySQL 8 caching_sha2_password
#   - Superset 的 MySQLEngineSpec.get_datatype 硬编码 import MySQLdb 读列类型常量,
#     故垫一层 MySQLdb shim 指向 pymysql(常量同为 MySQL 协议字段类型,数值一致)
# 注意: 官方镜像自带虚拟环境 /app/.venv 且无 pip,需用系统 pip + --target 装进 venv 的 site-packages。
# 生产可改用 postgres 元数据 + 独立维护的驱动层;dev 用 SQLite 元数据(见 compose 注释)。
FROM apache/superset:6.1.0

USER root
RUN /usr/local/bin/pip install --no-cache-dir \
        --target /app/.venv/lib/python3.10/site-packages \
        pymysql cryptography \
    && mkdir -p /app/.venv/lib/python3.10/site-packages/MySQLdb \
    && printf 'from pymysql import *  # noqa: F401,F403\nfrom pymysql import constants  # noqa: F401\n' \
        > /app/.venv/lib/python3.10/site-packages/MySQLdb/__init__.py
USER superset
