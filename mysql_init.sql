CREATE DATABASE IF NOT EXISTS openmetadata_db;

CREATE DATABASE IF NOT EXISTS airflow_db;

GRANT ALL PRIVILEGES ON openmetadata_db.* TO 'openmetadata'@'%';
GRANT ALL PRIVILEGES ON airflow_db.* TO 'openmetadata'@'%';

FLUSH PRIVILEGES;