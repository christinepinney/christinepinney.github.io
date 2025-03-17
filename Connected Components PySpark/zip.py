import zipfile

def zip_file(file_to_zip, zip_file_name):
    """Zips p2.py into p2.zip for S3 bucket."""

    with zipfile.ZipFile(zip_file_name, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(file_to_zip)

if __name__ == "__main__":
    file_to_zip = "p2.py"
    zip_file_name = "p2.zip"
    zip_file(file_to_zip, zip_file_name)