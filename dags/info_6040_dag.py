from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

import os
import json
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

def load_info_to_elasticsearch():
    base_dir = "/opt/airflow/bids-data"
    modalities = ["anat", "func", "dwi"]

    es = Elasticsearch(
        hosts=[{
            'host': 'elasticsearch',
            'port': 9200,
            'scheme': 'http'
        }],
        basic_auth=('elastic', 'changeme'),
        verify_certs=False
    )

    index_name = "info_anat_func_dwi_6040"
    index_name2 = "info_beh_6040"

    actions = []
    actions2 = []

    dashboard_fields = [
        "MagneticFieldStrength",
        "Manufacturer",
        "ManufacturersModelName",
        "BodyPartExamined",
        "SliceThickness",
        "SpacingBetweenSlices",
        "SAR",
        "EchoTime",
        "RepetitionTime",
        "FlipAngle",
        "TaskName",
        "ImageType",
        "SoftwareVersions",
        "Dcm2bidsVersion"
    ]

    # -------------------------------
    
    for subj_folder in os.listdir(base_dir):
        if subj_folder.startswith("sub-"):
            subject = subj_folder.replace("sub-", "")
            subj_path = os.path.join(base_dir, subj_folder)

            for modality in modalities:
                modality_path = os.path.join(subj_path, modality)

                if not os.path.isdir(modality_path):
                    continue

                for file in os.listdir(modality_path):
                    if file.endswith(".json"):
                        file_path = os.path.join(modality_path, file)

                        try:
                            with open(file_path, 'r', encoding='utf-8') as f:
                                data = json.load(f)

                            filtered_data = {key: data.get(key, None) for key in dashboard_fields}

                            doc = {
                                "subject": subject,
                                "modality": modality,
                                **filtered_data
                            }

                            actions.append({
                                "_index": index_name,
                                "_source": doc
                            })

                        except Exception as e:
                            print(f"error reading {file_path}: {e}")

    # -------------------------------

    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.endswith("_beh.tsv"):
                parts = file.split('_')
                subject = parts[0].replace("sub-", "")
                task = parts[1].replace("task-", "")
                run = parts[2].replace("run-", "")

                file_path = os.path.join(root, file)
                df = pd.read_csv(file_path, sep="\t")

                for _, row in df.iterrows():
                    doc = row.to_dict()
                    doc["subject"] = subject
                    doc["task"] = task
                    doc["run"] = run

                    actions2.append({
                        "_index": index_name2,
                        "_source": doc
                    })

    # -------------------------------
    if not es.ping():
        raise Exception("Elasticsearch connection failed.")

    # -------------------------------
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
    es.indices.create(index=index_name)
    bulk(es, actions)

    # -------------------------------
    if es.indices.exists(index=index_name2):
        es.indices.delete(index=index_name2)
    es.indices.create(index=index_name2)
    bulk(es, actions2)


with DAG(
    dag_id="bids_info_6040",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Extract anat/func/dwi JSON metadata and beh TSV data from dataset and load into Elasticsearch",
) as dag:

    upload_task = PythonOperator(
        task_id="load_bids_info_to_es",
        python_callable=load_info_to_elasticsearch,
    )

    upload_task
