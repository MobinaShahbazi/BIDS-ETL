from bids import BIDSLayout
import pandas as pd
import os
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# Load layout
layout = BIDSLayout(r"E:\term8\5. Bachelor Project\search\BIDS\datasets\ds006012", validate=False)

# Load participants.tsv
participants_df = pd.read_csv(r"E:\term8\5. Bachelor Project\search\BIDS\datasets\ds006012\participants.tsv", sep='\t')
participants_df['participant_id'] = participants_df['participant_id'].str.replace('sub-', '')

# Elasticsearch connection with authentication
es = Elasticsearch(
    hosts=[{
        'host': 'localhost',
        'port': 9200,
        'scheme': 'http'
    }],
    basic_auth=('elastic', 'changeme')  # user: elastic, pass: changeme
)

# Check connection
if not es.ping():
    print("❌ اتصال به Elasticsearch برقرار نشد.")
    exit()
else:
    print("✅ اتصال به Elasticsearch برقرار شد.")

index_name = "subject_v0"

# Optional: Delete index if exists (for clean run)
if es.indices.exists(index=index_name):
    es.indices.delete(index=index_name)
    print(f"📁 ایندکس {index_name} حذف شد.")

# Create index (optional settings/mappings can be added)
es.indices.create(index=index_name)
print(f"📁 ایندکس {index_name} ساخته شد.")

# Prepare bulk data
actions = []

subjects = layout.get_subjects()
for subj in subjects:
    files = layout.get(subject=subj, return_type='file')

    # Extract modalities based on directory names in file paths
    modalities = set()
    for f in files:
        parts = f.split(os.sep)
        for part in parts:
            if part in ['anat', 'meg', 'func', 'dwi', 'fmap', 'eeg', 'ieeg']:
                modalities.add(part)

    # Get info from participants.tsv
    participant_info = participants_df[participants_df['participant_id'] == subj]
    age = participant_info.iloc[0].get('age') if not participant_info.empty else None
    handedness = participant_info.iloc[0].get('hand') if not participant_info.empty else None

    doc = {
        "subject": subj,
        "age": age,
        "handedness": handedness,
        "sessions": len(layout.get_sessions(subject=subj)),
        "modalities": list(modalities)
    }

    action = {
        "_index": index_name,
        "_source": doc
    }
    actions.append(action)

# Bulk insert into Elasticsearch
bulk(es, actions)
print(f"📤 همه داده‌ها ({len(actions)}) به ایندکس {index_name} اضافه شدند.")