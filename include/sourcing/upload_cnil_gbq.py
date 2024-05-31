def upload_cnil_gbq():
    from include.sourcing.classes.gcs_processor import GCSProcessor
    from include.sourcing.classes.to_gbq import GBQProcessor
    from datetime import date

    today = date.today()

    gcs_bucket_name = 'cnil_csv'
    credentials_path = '/usr/local/airflow/include/sourcing/cred/service_account_local_py.json'
    folder = f"{today}/prep"

    instance1 = GCSProcessor(bucket_name=gcs_bucket_name, credentials_path=credentials_path)
    list_blob = instance1.list_blobs(folder)

    for i, blob in enumerate(list_blob):
      if "compare" in blob.name:
          index_to_up = i
            
    zip_file = instance1.get_zip_file_object(list_blob[index_to_up].name)

    project_id = 'cnil-392113'
    dataset_name = 'raw_data'
    processor_bq = GBQProcessor(credentials_path = credentials_path, project_id= project_id, dataset_id=dataset_name)
    processor_bq.create_dataset()
    processor_bq.upload_zipio_to_bq(zip_file_io=zip_file)