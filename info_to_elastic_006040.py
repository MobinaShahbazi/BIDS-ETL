import os
import json
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

base_dir = r"E:\term8\5. Bachelor Project\search\BIDS\datasets\ds006040-test"
modalities = ["anat", "func", "dwi"]

es = Elasticsearch(
    hosts=[{
        'host': 'localhost',
        'port': 9200,
        'scheme': 'http'
    }],
    basic_auth=('elastic', 'changeme') 
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
                        # print(doc)

                        actions.append({
                            "_index": index_name,
                            "_source": doc
                        })

                    except Exception as e:
                        print(f"error reading {file_path}: {e}")


for root, dirs, files in os.walk(base_dir):
    for file in files:
        if file.endswith("_beh.tsv"):

            # extract info from file name
            parts = file.split('_')
            subject = parts[0].replace("sub-", "")
            task = parts[1].replace("task-", "")
            run = parts[2].replace("run-", "")
            
            file_path = os.path.join(root, file)

            # read TSV
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


# Check connection
if not es.ping():
    print("Elasticsearch connection failed.")
    exit()
else:
    print("Elasticsearch connected successfully.")
    
# 1 ///////////////////////////////////////////////

# Delete index if exists 
if es.indices.exists(index=index_name):
    es.indices.delete(index=index_name)
    print(f"index {index_name} removed.")

# Create index 
print(f"index {index_name} created.")

# Bulk insert into Elasticsearch
bulk(es, actions)
print(f"all ({len(actions)}) data added to index {index_name}.")

# 2 ///////////////////////////////////////////////

# Delete index if exists 
if es.indices.exists(index=index_name2):
    es.indices.delete(index=index_name2)
    print(f"index {index_name2} removed.")

# Create index 
es.indices.create(index=index_name2)
print(f"index {index_name2} created.")

# Bulk insert into Elasticsearch
bulk(es, actions2)
print(f"all ({len(actions2)}) data added to index {index_name2}.")