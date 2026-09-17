-- MySQL 容器首次启动时执行(仅首次,data 目录为空时):
-- 创建 ETL 应用账号并授权。生产环境凭据走独立运维流程,此文件仅服务 dev。
CREATE USER IF NOT EXISTS 'etl'@'%' IDENTIFIED BY 'etl_pass';
GRANT ALL PRIVILEGES ON etl_meta.* TO 'etl'@'%';
GRANT ALL PRIVILEGES ON dw.* TO 'etl'@'%';
FLUSH PRIVILEGES;
