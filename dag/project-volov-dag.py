import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Параметры подключения
K8S_SPARK_NAMESPACE = "de-project"
K8S_CONNECTION_ID = "kubernetes"
GREENPLUM_ID = "greenplume"
SUBMIT_NAME = "job_submit"
GP_SCHEMA = "vj-volov"

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
    dag_id="project-volov-dag",
    default_args=default_args,
    schedule_interval=None,
    start_date=pendulum.datetime(2025, 8, 8, tz="UTC"),
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
            CREATE SCHEMA IF NOT EXISTS "{GP_SCHEMA}";
            DROP EXTERNAL TABLE IF EXISTS "{GP_SCHEMA}".seller_items CASCADE;
            CREATE EXTERNAL TABLE "{GP_SCHEMA}".seller_items (
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
            LOCATION ('pxf://project/volov/seller_items?PROFILE=s3:parquet&SERVER=default')
            ON ALL
            FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')
            ENCODING 'UTF8';
        """,
        split_statements=True,
    )

    # Шаг создания представления unreliable_sellers_view
    create_unreliable_sellers_report_view = SQLExecuteQueryOperator(
        task_id="create_unreliable_sellers_report_view",
        conn_id=GREENPLUM_ID,
        sql=f"""
            DROP VIEW IF EXISTS "{GP_SCHEMA}".unreliable_sellers_view;
            CREATE VIEW "{GP_SCHEMA}".unreliable_sellers_view AS
            SELECT 
                seller,
                SUM(availability_items_count) AS total_overload_items_count,
                BOOL_OR(
                    days_on_sell > 100 
                    AND availability_items_count > ordered_items_count
                ) AS is_unreliable
            FROM "{GP_SCHEMA}".seller_items
            GROUP BY seller;
        """,
        split_statements=True,
    )

    # Шаг создания представления item_brands_view
    create_brands_report_view = SQLExecuteQueryOperator(
        task_id="create_brands_report_view",
        conn_id=GREENPLUM_ID,
        sql=f"""
            DROP VIEW IF EXISTS "{GP_SCHEMA}".item_brands_view;
            CREATE VIEW "{GP_SCHEMA}".item_brands_view AS
            SELECT 
                brand,
                group_type,
                country,
                SUM(potential_revenue) AS potential_revenue,
                SUM(total_revenue) AS total_revenue,
                COUNT(*) AS items_count
            FROM "{GP_SCHEMA}".seller_items
            GROUP BY brand, group_type, country;
        """,
        split_statements=True,
    )

    start >> submit_task >> sensor_task >> items_datamart
    items_datamart >> create_unreliable_sellers_report_view
    items_datamart >> create_brands_report_view
    [create_unreliable_sellers_report_view, create_brands_report_view] >> end