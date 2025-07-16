Так же вставляем в проект файл .env с содержанием:

# Airflow Core
AIRFLOW_UID=1000
AIRFLOW_GID=0
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
AIRFLOW__WEBSERVER__SECRET_KEY=12345678

# DWH Connection (PostgreSQL)
DWH_HOST=host.docker.internal
DWH_NAME=dwh
DWH_USER=ds
DWH_PASSWORD=13245678
DWH_PORT=5432

# Дополнительные настройки
LOG_LEVEL=INFO
