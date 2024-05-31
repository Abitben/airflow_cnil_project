def compare_and_upload_catalogs():
    from include.sourcing.classes.catalog import GetCnilCatalog
    from include.sourcing.classes.to_gbq import GBQProcessor
    from include.sourcing.classes.gcs_processor import GCSProcessor
    from unidecode import unidecode
    import pandas as pd
    from include.sourcing.classes.catalog import CustomCatalog
    from datetime import date

    # get the catalog from data.gouv.fr
    url = 'https://www.data.gouv.fr/api/1/organizations/534fff61a3a7292c64a77d59/catalog'
    headers = {'accept': 'application/json'}
    url_add = 'https://www.data.gouv.fr/fr/organizations/cnil/datasets.csv'
    instance1 = GetCnilCatalog(url = url, headers = headers, url_additional_info=url_add)
    data = instance1.fetch_data_from_api()
    data = data['@graph']
    table_name = 'title'
    download_url = 'downloadURL'
    table_id = 'identifier' 
    file_format= 'format'
    last_update= 'modified' 
    accessURL = '@id'
    df_catalog = instance1.response_to_dataframe(data=data, table_name=table_name, download_url=download_url, table_id=table_id, file_format=file_format, last_update=last_update, accessURL=accessURL)
    df_dataset = instance1.load_additional_info()
    df_catalog = instance1.identify_datasets_info()
    df_catalog = instance1.merge_additional_info()
    

    # get the catalog from bigquery
    credentials_path = '/usr/local/airflow/include/sourcing/cred/service_account_local_py.json'
    project_id = 'cnil-392113'
    dataset_name = 'raw_data'
    query = 'SELECT * FROM cnil-392113.catalog_data.cnil_catalog_bq'
    processor_bq = GBQProcessor(credentials_path = credentials_path, project_id= project_id, dataset_id=dataset_name)
    df_bq = processor_bq.bq_to_df(query)
    df_bq_last_updated = df_bq.groupby(['bq_dataset', 'bq_table'], as_index=False)['bq_modified'].max()


    # compare the two catalogs
    today = date.today()
    df_bq = df_bq_last_updated
    df_raw = df_catalog

    # prepare the table_name_match column
    def table_name_match(row):
        return unidecode(row['table_name']).lower().replace(' ', '_').replace('-', '_').replace(".", "_").replace("(", "_").replace(")", "").replace(",", "")

    df_raw['table_name_match'] = df_raw.apply(table_name_match, axis=1)

    # merge the two catalogs
    df_merge = pd.merge(df_raw, df_bq, left_on='table_name_match', right_on='bq_table', how='outer')
    df_merge['bq_modified'] = pd.to_datetime(df_merge['bq_modified']).dt.tz_localize(None)
    df_merge['last_update'] = pd.to_datetime(df_merge['last_update'])

    # compare differences of time between the two catalogs
    df_merge['diff_date'] = df_merge['last_update'] - df_merge['bq_modified']
    df_merge['diff_date'] = df_merge['diff_date'].dt.days

    # if the difference is positive, it means that the bq table is outdated and a new update is needed
    df_filtered = df_merge[~df_merge['data_format'].isna()]
    df_compare = df_filtered[(df_filtered['diff_date'] > 0) | (df_filtered['bq_dataset'].isna())]


    # upload the catalogs to GCS
    today = date.today()
    bucket_name = 'cnil_csv'
    credential_path = '/usr/local/airflow/include/sourcing/cred/service_account_local_py.json'

    init2 = GCSProcessor(bucket_name = bucket_name, credentials_path= credential_path)
    init2.create_bucket()

    file_paths = [df_compare, df_merge]
    dest_folder = 'raw'
    dest_blobs = [f'source_cnil_catalog_compare_{today}.csv',f'source_cnil_catalog_merge_{today}.csv' ]
    init2.upload_local_to_gcs(file_paths=file_paths, dest_folder=dest_folder, dest_blobs=dest_blobs, date=today)
