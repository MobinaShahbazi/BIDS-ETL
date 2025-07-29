from flask import Flask, request, render_template, redirect, url_for, flash
import os
import csv
import zipfile
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.secret_key = 'your_secret_key'
UPLOAD_FOLDER = 'uploads'
BIDS_FOLDER = r"E:\term8\5. Bachelor Project\servises\bids-data"
TSV_PATH = os.path.join(BIDS_FOLDER, 'Demographic_Information.tsv')

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(BIDS_FOLDER, exist_ok=True)

def process_zip_and_append_dict(data_dict, zip_path):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(BIDS_FOLDER)

    file_exists = os.path.isfile(TSV_PATH)
    with open(TSV_PATH, 'a', newline='', encoding='utf-8') as tsvfile:
        writer = csv.DictWriter(tsvfile, fieldnames=data_dict.keys(), delimiter='\t')
        if not file_exists:
            writer.writeheader()
        writer.writerow(data_dict)

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        
        form_data = {
            'ID': request.form['id'],
            'Age': request.form['age'],
            'Gender': request.form['gender'],
            'Height': request.form['height'],
            'Weight': request.form['weight'],
            'Native Korean': request.form.get('native_korean', 0)
        }

        zip_file = request.files['zip_file']
        if zip_file.filename == '':
            flash('Please upload a zip file.')
            return redirect(request.url)

        zip_path = os.path.join(UPLOAD_FOLDER, secure_filename(zip_file.filename))
        zip_file.save(zip_path)

        process_zip_and_append_dict(form_data, zip_path)
        flash('Subject added successfully.')
        return redirect(url_for('index'))

    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)