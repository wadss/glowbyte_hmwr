import io
from minio import Minio
from airflow.hooks.base import BaseHook
from airflow.hooks.filesystem import FSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

PG_HOOK = PostgresHook(postgres_conn_id='greenplum')
CONN_MINIO = BaseHook.get_connection('minio')
PATH = FSHook(fs_conn_id='gp_path')


def extract_and_load_to_minio(data_type, extract_column, export_date, **kwargs):
    buffer = io.BytesIO()
    query = f"SELECT * FROM public.orders WHERE date_trunc('day', {extract_column}) = '{export_date}'::date"
    client = Minio(f'{CONN_MINIO.host}:{CONN_MINIO.port}', access_key=CONN_MINIO.login, secret_key=CONN_MINIO.password, secure=False)
    bucket = PATH.get_path()
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
    output_file = f'{bucket}/{data_type}/{export_date.year}/{export_date.month}/{export_date}.parquet'

    try:
        df = PG_HOOK.get_pandas_df(sql=query)
        df.to_parquet(buffer, engine='pyarrow', index=False, compression='snappy')
        buffer.seek(0)
        client.put_object(bucket, output_file, buffer, length=len(buffer.getvalue()), content_type='application/octet-stream')
        print('Файл в объектное хранилище успешно записан')

    except ValueError as e:
        print(f'Ошибка записи файла в объектное хранилище: {e}')

        

    
    



