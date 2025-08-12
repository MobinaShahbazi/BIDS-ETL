from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from bids import BIDSLayout
import pandas as pd
import os
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

def load_files_to_elasticsearch():
    dataset_dir = "/opt/airflow/bids-data"
    index_name = "file_6040"

    layout = BIDSLayout(dataset_dir, validate=False)

    df = layout.to_df()
    df = df[~df['path'].str.contains(r'\.git', case=False, regex=True)]

    actions = []
    for _, row in df.iterrows():
        doc = row.dropna().to_dict()

        file_path = row['path']
        try:
            file_size = os.path.getsize(file_path)
            doc['filesize'] = file_size
        except OSError:
            doc['filesize'] = None

        action = {
            "_index": index_name,
            "_source": doc
        }
        actions.append(action)

    es = Elasticsearch(
        hosts=[{
            'host': 'elasticsearch',
            'port': 9200,
            'scheme': 'http'
        }],
        basic_auth=('elastic', 'changeme'),
        verify_certs=False
    )

    if not es.ping():
        raise Exception("Elasticsearch connection failed.")

    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)

    es.indices.create(index=index_name)
    bulk(es, actions)


with DAG(
    dag_id="bids_files_6040",
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description="Upload dataset files metadata to Elasticsearch",
) as dag:

    upload_task = PythonOperator(
        task_id="load_bids_files_to_es",
        python_callable=load_files_to_elasticsearch,
    )

    upload_task
