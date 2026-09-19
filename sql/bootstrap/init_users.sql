-- MySQL 容器首次启动时执行(仅首次,data 目录为空时):
-- 创建 ETL 应用账号并授权。生产环境凭据走独立运维流程,此文件仅服务 dev。
-- 存量实例(首启已执行过)如需 airflow 库,直接跑下面的 CREATE/GRANT 语句,
-- 或由 airflow-init 容器用 root 兜底执行(见 airflow/docker-compose.dev.yaml)。
CREATE USER IF NOT EXISTS 'etl'@'%' IDENTIFIED BY 'etl_pass';
GRANT ALL PRIVILEGES ON etl_meta.* TO 'etl'@'%';
GRANT ALL PRIVILEGES ON dw.* TO 'etl'@'%';
CREATE DATABASE IF NOT EXISTS airflow DEFAULT CHARACTER SET utf8mb4;
GRANT ALL PRIVILEGES ON airflow.* TO 'etl'@'%';
FLUSH PRIVILEGES;
