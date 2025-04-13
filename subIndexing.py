from bids import BIDSLayout
import pandas as pd
import os

# Load layout
layout = BIDSLayout(r"E:\term8\5. Bachelor Project\search\BIDS\datasets\ds006012", validate=False)

# Load participants.tsv
participants_df = pd.read_csv(r"E:\term8\5. Bachelor Project\search\BIDS\datasets\ds006012\participants.tsv", sep='\t')
participants_df['participant_id'] = participants_df['participant_id'].str.replace('sub-', '')

subjects = layout.get_subjects()

for subj in subjects:
    files = layout.get(subject=subj, return_type='file')

    # Extract modalities based on directory names in file paths
    modalities = set()
    for f in files:
        parts = f.split(os.sep)
        for part in parts:
            if part in ['anat', 'meg', 'func', 'dwi', 'fmap']:
                modalities.add(part)

    # Get info from participants.tsv
    participant_info = participants_df[participants_df['participant_id'] == subj]
    age = participant_info.iloc[0].get('age') if not participant_info.empty else None
    sex = participant_info.iloc[0].get('hand') if not participant_info.empty else None

    doc = {
        "subject": subj,
        "age": age,
        "sex": sex,
        "sessions": len(layout.get_sessions(subject=subj)),
        "modalities": list(modalities)
    }

    print(doc)
    # es.index(index="bids-subjects", document=doc)
