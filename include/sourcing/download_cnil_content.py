def download_cnil_content():
    from include.sourcing.classes.gcs_processor import GCSProcessor
    from datetime import date
    import pandas as pd
    import os
    from include.sourcing.classes.download_catalog_content import GdprSanctionsScrapper

    today = date.today()

    gcs_bucket_name = 'cnil_csv'
    credentials_path = '/usr/local/airflow/include/sourcing/cred/service_account_local_py.json'
    blob_catalog = f"{today}/raw/source_cnil_catalog_compare_{today}.csv"

    instance1 = GCSProcessor(bucket_name=gcs_bucket_name, credentials_path=credentials_path)
    files = instance1.download_files_from_catalog(catalog_path=blob_catalog)
    zip_file = instance1.create_zip_from_files(files)

    scrapper = GdprSanctionsScrapper()
    zip_file = scrapper.add_to_zip(zip_file)

    file_paths = [zip_file]
    dest_folder = 'raw'
    dest_blobs = ['raw_datasets_compare.zip']
    instance1.upload_local_to_gcs(file_paths=file_paths, dest_folder=dest_folder, dest_blobs=dest_blobs, date=today)