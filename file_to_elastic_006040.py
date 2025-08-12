from bids import BIDSLayout
import pandas as pd
import os
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# Load layout
layout = BIDSLayout(r"E:\term8\5. Bachelor Project\search\BIDS\datasets\ds006040", validate=False)

# Prepare bulk data
actions = []
index_name = "file_6040"

files = layout.get()
df = layout.to_df()
df = df[~df['path'].str.contains(r'\.git', case=False, regex=True)]

for _, row in df.iterrows():
    doc = row.dropna().to_dict()
    
    file_path = row['path']
    try:
        file_size = os.path.getsize(file_path)  # byte
        doc['filesize'] = file_size
    except OSError:
        doc['filesize'] = None 

    action = {
        "_index": index_name,
        "_source": doc
    }
    actions.append(action)


# Elasticsearch connection 
es = Elasticsearch(
    hosts=[{
        'host': 'localhost',
        'port': 9200,
        'scheme': 'http'
    }],
    basic_auth=('elastic', 'changeme')  
)

# Check connection
if not es.ping():
    print("Elasticsearch connection failed.")
    exit()
else:
    print("Elasticsearch connected successfully.")

# Delete index if exists 
if es.indices.exists(index=index_name):
    es.indices.delete(index=index_name)
    print(f"index {index_name} removed.")

# Create index
print(f"index {index_name} created.")

# Bulk insert into Elasticsearch
bulk(es, actions)
print(f"all ({len(actions)}) data added to index {index_name}.")