import zipfile
import os
import csv

def process_zip_and_append_dict(data_dict, zip_path):
    # Static addresses
    destination_dir = r"E:\term8\5. Bachelor Project\servises\bids-data"
    tsv_path = r"E:\term8\5. Bachelor Project\servises\bids-data\Demographic_Information.tsv"

    # Ensure destination directory exists
    os.makedirs(destination_dir, exist_ok=True)

    # Extract zip file
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(destination_dir)

    # Append dictionary to TSV file
    # If file doesn't exist, write header first
    file_exists = os.path.isfile(tsv_path)
    with open(tsv_path, 'a', newline='', encoding='utf-8') as tsvfile:
        writer = csv.DictWriter(tsvfile, fieldnames=data_dict.keys(), delimiter='\t')
        if not file_exists:
            writer.writeheader()
        writer.writerow(data_dict)
      

        
process_zip_and_append_dict(
    {"ID": 'sub-029',
     'Age': 30,
     'Gender': 1,
     'Height': 300,
     'Weight': 300,
     'Native Korean': 0
     },
    "E:/term8/5. Bachelor Project/search/BIDS/datasets/samples/sub-029.zip"
)

