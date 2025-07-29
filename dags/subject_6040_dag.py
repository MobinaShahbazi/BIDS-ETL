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

def upload_to_elasticsearch():
    
    dataset_dir = "/opt/airflow/bids-data"
    tsv_path = os.path.join(dataset_dir, "Demographic_Information.tsv")

    layout = BIDSLayout(dataset_dir, validate=False)

    participants_df = pd.read_csv(tsv_path, sep='\t')
    participants_df['Anonymized ID'] = participants_df['Anonymized ID'].str.replace('sub-', '')
    participants_df['Gender'] = participants_df['Gender'].replace({'1': 'man', '0': 'woman'})

    actions = []
    index_name = "subject_6040"

    subjects = sorted(layout.get_subjects() + ['003'])

    for subj in subjects:
        files = layout.get(subject=subj)
        modalities = sorted({f.entities.get('datatype') for f in files if f.entities.get('datatype')})

        participant_info = participants_df[participants_df['Anonymized ID'] == subj]

        if participant_info.empty:
            continue

        info_dict = participant_info.iloc[0].to_dict()
        info_dict['modalities'] = modalities
        info_dict['subject'] = subj

        for field in ['Height', 'Weight', 'Age']:
            try:
                info_dict[field] = int(info_dict.get(field))
            except (ValueError, TypeError):
                info_dict[field] = None

        action = {
            "_index": index_name,
            "_source": info_dict
        }
        actions.append(action)

    # اتصال به Elasticsearch
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

    # حذف ایندکس در صورت وجود
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)

    es.indices.create(index=index_name)
    bulk(es, actions)


with DAG(
    dag_id="bids_subjects_6040",
    default_args=default_args,
    schedule_interval='@daily',  
    catchup=False,
    description="Extract data from BIDS dataset and upload to Elasticsearch",
) as dag:

    upload_task = PythonOperator(
        task_id="upload_bids_data_to_es",
        python_callable=upload_to_elasticsearch,
    )

    upload_task
