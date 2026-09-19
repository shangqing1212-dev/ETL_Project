# Airflow 自定义镜像: 官方 3.3.1 基础 + ETL SDK(业务逻辑全部沉淀在 SDK,Airflow 只做编排)。
# - 官方镜像的 pip wrapper 禁止 root 安装(见官方 docker-stack 文档),SDK 以 airflow 用户 --user 装进 ~/.local
# - SDK 依赖(pymysql/polars/httpx/...)一并装入;metadata 用 MySQL(mysql+pymysql://),与 SDK 同驱动
# 用法(compose build context = 项目根):
#   docker build -f airflow/Dockerfile -t etl-airflow:3.3.1 .
FROM apache/airflow:3.3.1-python3.12

USER root
COPY sdk /tmp/etl-sdk
USER airflow
RUN pip install --no-cache-dir --user /tmp/etl-sdk
USER root
RUN rm -rf /tmp/etl-sdk
USER airflow
