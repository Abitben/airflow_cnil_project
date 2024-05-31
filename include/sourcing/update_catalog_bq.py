def update_catalog_bq():
  import pandas as pd
  from include.sourcing.classes.catalog import CustomCatalog
  from include.sourcing.classes.to_gbq import GBQProcessor

  credential_path = '/usr/local/airflow/include/sourcing/cred/service_account_local_py.json'
  dataset_name = 'raw_data'
  project_id = 'cnil-392113'
  instance8 = CustomCatalog(credentials_path=credential_path, project_id=project_id, dataset_name=dataset_name)
  df_last = instance8.bq_dataset_catalog()

  query = 'SELECT * FROM cnil-392113.catalog_data.cnil_catalog_bq'

  processor_bq = GBQProcessor(credentials_path = credential_path, project_id= project_id, dataset_id=dataset_name)
  df = processor_bq.bq_to_df(query)

  df_all = pd.concat([df, df_last])
  df_all = df_all.drop_duplicates().sort_values('bq_modified', ascending=False)
  processor_bq.df_to_bq(df_all, 'cnil-392113.catalog_data.cnil_catalog_bq')




