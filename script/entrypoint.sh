#!/bin/bash
set -e

echo ">>> Checking airflow.db"
if [ ! -f "/opt/airflow/airflow.db" ]; then
  echo ">>> Initializing Airflow DB and creating user..."
  airflow db init
  airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin
fi

echo ">>> Running db upgrade..."
airflow db upgrade

echo ">>> Starting Airflow with command: $@"
exec /entrypoint "$@"