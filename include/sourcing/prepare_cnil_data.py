def prepare_cnil_data():
    from include.sourcing.classes.gcs_processor import GCSProcessor
    from datetime import date
    import pandas as pd
    from include.sourcing.classes.prep_data import PrepDataCnilBQ

    today = date.today()

    gcs_bucket_name = 'cnil_csv'
    credentials_path = '/usr/local/airflow/include/sourcing/cred/service_account_local_py.json'

    gcs_proc = GCSProcessor(bucket_name=gcs_bucket_name, credentials_path=credentials_path)
    blob_name_zip = f'{today}/raw/raw_datasets_compare.zip'
    zip_file = gcs_proc.get_zip_file_object(blob_name_zip)

    prep_data = PrepDataCnilBQ(zip_file)
    dfs = prep_data.process_dfs(zip_file)
    zip_io = prep_data.save_dfs_to_zip(dfs)
    errors_log = prep_data.errors_backlog

    file_paths = [zip_io]
    dest_folder = 'prep'
    dest_blobs = ['prep_gbq_datasets_compare.zip']
    gcs_proc.upload_local_to_gcs(file_paths=file_paths, dest_folder=dest_folder, dest_blobs=dest_blobs, date=today)