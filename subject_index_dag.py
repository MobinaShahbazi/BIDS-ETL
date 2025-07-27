from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from bids import BIDSLayout
import pandas as pd
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def index_subjects_to_elasticsearch():
    # Load BIDS layout
    layout = BIDSLayout("/opt/airflow/bids-data", validate=False)

    # Load participants.tsv
    participants_path = "/opt/airflow/bids-data/Demographic_Information.tsv"
    participants_df = pd.read_csv(participants_path, sep='\t')
    participants_df['Anonymized ID'] = participants_df['Anonymized ID'].str.replace('sub-', '')
    participants_df['Gender'] = participants_df['Gender'].replace({'1': 'man', '0': 'woman'})

    actions = []
    index_name = "subject_v2"

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

    # Connect to Elasticsearch
    es = Elasticsearch(
        hosts=[{'host': 'elasticsearch', 'port': 9200, 'scheme': 'http'}],
        basic_auth=('elastic', 'changeme')
    )

    if not es.ping():
        raise ConnectionError("Elasticsearch connection failed.")

    # Recreate index
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)

    es.indices.create(index=index_name)

    # Bulk insert
    bulk(es, actions)

    print(f"{len(actions)} documents indexed into {index_name}.")


with DAG(
    dag_id='index_bids_subjects',
    default_args=default_args,
    description='Index BIDS subjects into Elasticsearch daily',
    schedule_interval='0 0 * * *',  # every day at 00:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bids', 'elasticsearch'],
) as dag:

    index_task = PythonOperator(
        task_id='index_subjects',
        python_callable=index_subjects_to_elasticsearch
    )

    index_task
