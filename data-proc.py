import uuid
import datetime
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)

# Конфигурация для Yandex.Cloud
CLOUD_ZONE = 'ru-central1-a'
SSH_KEY = 'ssh-ed25546 DSDFGDFGDI1NTE5AAAAIL7QzQcp0xQqFK6vEAo+hrKFwEWDYi9+ypctkf1LxcyE vasil@LES_PC'
SUBNET_ID = 'sdfdfshgfhdfgheklm'
SERVICE_ACCOUNT = 'jsdjdfdslqek3211q'
HIVE_METASTORE_HOST = '10.128.0.18'
S3_STORAGE_BUCKET = 'etl-hse-dataproc'

# Определение DAG-а
with DAG(
    dag_id='hourly_data_job',
    start_date=datetime.datetime.now(),
    schedule_interval='@hourly',
    catchup=False,
    max_active_runs=1,
    tags=['yandexcloud', 'spark', 'automation']
) as dag:

    # Шаг 1 — Подготовка кластера Data Proc
    init_cluster = DataprocCreateClusterOperator(
        task_id='init_dp_cluster',
        cluster_name=f'dp-job-{uuid.uuid4()}',
        cluster_description='Кластер для запуска Spark-процесса',
        zone=CLOUD_ZONE,
        subnet_id=SUBNET_ID,
        ssh_public_keys=SSH_KEY,
        service_account_id=SERVICE_ACCOUNT,
        s3_bucket=S3_STORAGE_BUCKET,
        cluster_image_version='2.1',
        masternode_resource_preset='s2.small',
        masternode_disk_size=32,
        masternode_disk_type='network-hdd',
        computenode_resource_preset='s2.small',
        computenode_disk_size=32,
        computenode_disk_type='network-hdd',
        computenode_count=1,
        computenode_max_hosts_count=3,
        datanode_count=0,
        services=['SPARK', 'YARN'],
        properties={
            'spark:spark.hive.metastore.uris': f'thrift://{HIVE_METASTORE_HOST}:9083'
        },
    )

    # Шаг 2 — Запуск PySpark скрипта
    run_processing_job = DataprocCreatePysparkJobOperator(
        task_id='run_spark_processing',
        main_python_file_uri=f's3a://{S3_STORAGE_BUCKET}/scripts/clean.py'
    )

    # Шаг 3 — Очистка ресурсов
    terminate_cluster = DataprocDeleteClusterOperator(
        task_id='shutdown_dp_cluster',
        trigger_rule=TriggerRule.ALL_DONE
    )

    # Задание порядка выполнения
    init_cluster >> run_processing_job >> terminate_cluster
