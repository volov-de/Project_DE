import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Параметры подключения
K8S_SPARK_NAMESPACE = "de-project"
K8S_CONNECTION_ID = "kubernetes_karpov"
GREENPLUM_ID = "greenplume_karpov"
SUBMIT_NAME = "job_submit"
GP_SCHEMA = "vj_volov"

# Билдер для запуска Spark
def _build_submit_operator(task_id: str, application_file: str, link_dag):
    return SparkKubernetesOperator(
        task_id=task_id,
        namespace=K8S_SPARK_NAMESPACE,
        application_file=application_file,
        kubernetes_conn_id=K8S_CONNECTION_ID,
        do_xcom_push=True,
        dag=link_dag,
    )

# Билдер для запуска Sensor
def _build_sensor(task_id: str, application_name: str, link_dag):
    return SparkKubernetesSensor(
        task_id=task_id,
        namespace=K8S_SPARK_NAMESPACE,
        application_name=application_name,
        kubernetes_conn_id=K8S_CONNECTION_ID,
        attach_log=True,
        dag=link_dag,
    )

default_args = {"owner": "vj-volov",}

# Описание DAG
with DAG(
    dag_id="startde-project-vj-volov-dag",
    default_args=default_args,
    schedule_interval=None,
    start_date=pendulum.datetime(2024, 9, 10, tz="UTC"),
    tags=["example", "volov"],
    catchup=False,
) as dag:

    start = EmptyOperator(task_id="start", dag=dag)
    end = EmptyOperator(task_id="end", dag=dag)

    # Запуск Spark job по yaml-манифесту
    submit_task = _build_submit_operator(
        task_id=SUBMIT_NAME,
        application_file="spark_submit.yaml",
        link_dag=dag,
    )

    sensor_task = _build_sensor(
        task_id='job_sensor',
        application_name=f"{{{{task_instance.xcom_pull(task_ids='{SUBMIT_NAME}')['metadata']['name']}}}}",
        link_dag=dag
    )

# Шаг создания внешней таблицы Greenplum через SQLExecuteQueryOperator
items_datamart = SQLExecuteQueryOperator(
    task_id="items_datamart",
    conn_id=GREENPLUM_ID,
    sql=f"""

DROP EXTERNAL TABLE IF EXISTS "ai-iskakova".seller_items CASCADE;
CREATE EXTERNAL TABLE "ai-iskakova".seller_items(


        CREATE SCHEMA IF NOT EXISTS {GP_SCHEMA};

        DROP EXTERNAL TABLE IF EXISTS {GP_SCHEMA}.seller_items;

        CREATE EXTERNAL TABLE {GP_SCHEMA}.seller_items (
          sku_id BIGINT,
          title TEXT,
          category TEXT,
          brand TEXT,
          seller TEXT,
          group_type TEXT,
          country TEXT,
          availability_items_count BIGINT,
          ordered_items_count BIGINT,
          warehouses_count BIGINT,
          item_price BIGINT,
          goods_sold_count BIGINT,
          item_rate FLOAT8,
          days_on_sell BIGINT,
          avg_percent_to_sold BIGINT,
          returned_items_count INTEGER,
          potential_revenue BIGINT,
          total_revenue BIGINT,
          avg_daily_sales FLOAT8,
          days_to_sold FLOAT8,
          item_rate_percent FLOAT8
        )
        LOCATION ('pxf://startde-project/vj-volov/seller_items?PROFILE=s3:parquet&SERVER=default')
        ON ALL
        FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')
        ENCODING 'UTF8';
    """,
)


    start >> submit_task >> sensor_task >> items_datamart >> end