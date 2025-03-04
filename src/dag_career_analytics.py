from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from etl_code import etl_career_analytics
from metrics_sql import load_metrics_tables


dag = DAG(
    "career_analytics_pipeline",
    description='ETL pipeline for professional career analytics',
    start_date=datetime(2025, 3, 4),
    schedule_interval="@daily",
    catchup=False
)

etl_task = PythonOperator(
    task_id="ETL_job",
    python_callable=etl_career_analytics,
    dag=dag
)

metrics_load_task =  PythonOperator(
    task_id="Metrics_load_job",
    python_callable=load_metrics_tables,
    dag=dag
)


etl_task >> metrics_load_task