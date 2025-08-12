from bids import BIDSLayout
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# Load layout
layout = BIDSLayout(r"E:\term8\5. Bachelor Project\search\BIDS\datasets\ds006040-test", validate=False)

# Load participants.tsv
participants_df = pd.read_csv(r"E:\term8\5. Bachelor Project\search\BIDS\datasets\ds006040-test\Demographic_Information.tsv", sep='\t')
participants_df['Anonymized ID'] = participants_df['Anonymized ID'].str.replace('sub-', '')
participants_df['Gender'] = participants_df['Gender'].replace({'1': 'man', '0': 'woman'})

# Prepare bulk data
actions = []
index_name = "subject_6040"

subjects = sorted(layout.get_subjects() + ['003'])
print(subjects)
for subj in subjects:
    files = layout.get(subject=subj)
    modalities = sorted({f.datatype for f in files if f.datatype})

    participant_info = participants_df[participants_df['Anonymized ID'] == subj]

    info_dict = participant_info.iloc[0].to_dict()
    info_dict['modalities'] = modalities
    info_dict['subject'] = subj 
    for field in ['Height', 'Weight', 'Age']:
        value = info_dict.get(field)
        try:
            info_dict[field] = int(value)
        except (ValueError, TypeError):
            info_dict[field] = None

    
    doc = info_dict
    # print(doc)

    action = {
        "_index": index_name,
        "_source": doc
    }
    actions.append(action)
    print(doc)



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
es.indices.create(index=index_name)
print(f"index {index_name} created.")

# Bulk insert into Elasticsearch
bulk(es, actions)
print(f"all ({len(actions)}) data added to index {index_name}.")