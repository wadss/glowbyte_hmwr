import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from etl.extract.extractor_from_gp_to_minio import extract_and_load_to_minio
from etl.transfrom_and_load.trsnfrm_ld_to_ck_order_dt import trsnfrm_ld_to_ck_order_dt
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


default_args = {
    'owner': 'wadss_team',
    'depends_on_past': False,
    'email': ['wadss07@mail.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=1),
}
UPLOAD_DATE = datetime.date(2025, 9, 6)


with DAG(
    dag_id='gp_etl_dag_by_order_dt',
    default_args=default_args,
    description='ETL процесс из Greenplum в Clickhouse',
    schedule_interval='0 2 * * *',
    start_date=datetime.datetime(2025, 9, 1),
    catchup=False,
    tags=['dag_by_order_dt'],
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract_and_load_to_minio,
        op_kwargs={'export_date': UPLOAD_DATE,
                   'query': "SELECT * FROM public.orders WHERE date_trunc('day', order_dt) = '2025-09-06'",
                   'data_type': 'raw'
                }
    )

    transform_and_load_task = PythonOperator(
        task_id='transform_and_load_task',
        python_callable=trsnfrm_ld_to_ck_order_dt,
        op_kwargs={'export_date': UPLOAD_DATE, 'data_type': 'raw'}
    )

    trigger_next = TriggerDagRunOperator(
        task_id='trigger_gp_etl_dag_by_update_dt',
        trigger_dag_id='gp_etl_dag_by_update_dt'
    )

    extract_task >> transform_and_load_task >> trigger_next