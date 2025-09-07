from minio import Minio
import clickhouse_connect
import pandas as pd
from io import BytesIO
from airflow.hooks.base import BaseHook
from airflow.hooks.filesystem import FSHook
from airflow.providers.odbc.hooks.odbc import OdbcHook
from ..utils.versionizer import versionizer


CONN_MINIO = BaseHook.get_connection('minio')
PATH = FSHook(fs_conn_id='gp_path')
CH = OdbcHook.get_connection('clickhouse')


def trsnfrm_ld_to_ck_order_dt(data_type, export_date, **kwargs):
    bucket = PATH.get_path()
    obj_name = f'{PATH.get_path()}/{data_type}/{export_date.year}/{export_date.month}/{export_date}.parquet'
    client = Minio(f'{CONN_MINIO.host}:{CONN_MINIO.port}', access_key=CONN_MINIO.login, secret_key=CONN_MINIO.password, secure=False)
    ch_client = clickhouse_connect.get_client(host=CH.host, port=CH.port, username=CH.login, password=CH.password)
    try:
        response = client.get_object(bucket, obj_name)
        data = pd.read_parquet(BytesIO(response.read()))
        data = versionizer(data)
        ch_client.insert_df('orders_story', data)
        print('Данные в Clickhouse успешно загружены!')
    except Exception as e:
        print(f'Ошибка: {e}')
    finally:
        response.close()
        response.release_conn()




