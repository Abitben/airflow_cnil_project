from .connectors import GoogleConnector
from google.cloud import bigquery
import pandas_gbq
from io import StringIO
from colorama import Fore, Style
import requests
from io import BytesIO
import io
import pandas as pd
import zipfile
import re
from unidecode import unidecode

class GBQProcessor(GoogleConnector):
    """
    A class used to process data from Google Cloud Storage (GCS) and BigQuery (BQ)

    ...

    Attributes
    ----------
    credentials : google.auth.credentials.Credentials
        Google Cloud credentials from a service account file
    bq_client : google.cloud.bigquery.client.Client
        A BigQuery client object
    storage_client : google.cloud.storage.client.Client
        A Cloud Storage client object
    project_id : str
        Google Cloud project ID
    dataset_name : str
        BigQuery dataset name
    bucket_name : str
        Cloud Storage bucket name

    Methods
    -------
    create_dataset():
        Creates a new BigQuery dataset
    list_blobs():
        Lists all blobs in the Cloud Storage bucket
    upload_to_bq(blobs):
        Uploads blobs data to BigQuery
    """

    def __init__(self, credentials_path, project_id, dataset_id, bucket_name=None):
        """
        Constructs all the necessary attributes for the GCS_BQ_Processor object.

        Parameters
        ----------
            credentials_path : str
                Path to the service account file
            project_id : str
                Google Cloud project ID
            dataset_name : str
                BigQuery dataset name
            bucket_name : str
                Cloud Storage bucket name
        """
        super().__init__(credentials_path, project_id=project_id)
        self.project_id = project_id
        self.dataset_id = dataset_id
    

    def create_dataset(self):
        """
        Creates a new BigQuery dataset
        """
        dataset_id = f"{self.project_id}.{self.dataset_id}"
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "EU"
        self.bq_client.create_dataset(dataset, timeout=30, exists_ok=True)
        print(f"{Fore.GREEN}Created dataset (or already exists) {self.bq_client.project}.{dataset.dataset_id}{Style.RESET_ALL}")

    def upload_to_bq(self, blobs, prefix=None):
        """
        Uploads blobs data to BigQuery

        Parameters
        ----------
        blobs : list
            a list of blobs to be uploaded
        """
        for blob in blobs:
            blob_data = blob.download_as_text()
            df = pd.read_csv(StringIO(blob_data))
            print(f"{Fore.BLUE}{blob.name}{Style.RESET_ALL}")
            blob_name = blob.name.replace(f"raw_csv/", "").replace(".csv", "")
            table_name = self.project_id + '.' + "raw_data" + "." + blob_name
            pandas_gbq.to_gbq(df, table_name, project_id=self.project_id, if_exists='replace', api_method= "load_csv")
            print(f"{Fore.GREEN}{blob.name} is uploaded to {table_name}{Style.RESET_ALL}")

    def upload_zip_to_bq(self, zip_file):
        """
        Uploads files from a ZIP archive to Google BigQuery.

        This method extracts files from the provided ZIP archive, processes their filenames to generate
        appropriate BigQuery table names, and uploads the data to the specified BigQuery dataset.

        Args:
            zip_file: A ZIP file object containing the files to be uploaded.

        The method performs the following steps:
        1. Lists all files in the ZIP archive, filtering out directories.
        2. Iterates through each file:
            - If the file is in a subdirectory, processes the filename to extract the base name and date information.
            - Cleans the filename to remove accents and converts it to lowercase.
            - Constructs the BigQuery table name using the project ID, dataset ID, and cleaned filename.
        3. Attempts to read each file as a CSV with a semicolon (;) delimiter:
            - If reading fails, attempts to read the file with a comma (,) delimiter.
            - Uploads the DataFrame to BigQuery, replacing any existing table with the same name.
        4. Handles and logs any errors encountered during file reading or uploading.

        Notes:
            - Assumes filenames may contain date information in the format '_YYYY_MM_DD'.
            - Assumes file extensions may be '.csv' or '.xlsx'.
            - Uses the 'unidecode' library to remove accents from filenames.

        Example:
            zip_file = ZipFile('data.zip', 'r')
            self.upload_zip_to_bq(zip_file)
        """
        file_list = [file for file in zip_file.namelist()]
        filtered_list = list(filter(lambda x: not x.endswith('/'), file_list))
        for filename in filtered_list:
            if "/" in filename: 
                print("---------------------")
                print(filename)
                filename_bq = filename.split('/')[1]
                print(filename_bq)
                pattern = "_(?=\d{4}_\d{2}_\d{2})"
                split_name = re.split(pattern, filename_bq)
                filename_bq = unidecode(split_name[0]).lower()
                date_ext = split_name[1]
                date_ext = date_ext.replace('.csv', '')
                if "_csv" in date_ext:
                    ext = "csv"
                    date_date = date_ext.split('_')[0] + '-' + date_ext.split('_')[1] + '-' + date_ext.split('_')[2]
                    print(date_date)
                    print(ext)
                elif "_xlsx" in date_ext:
                    ext = "xlsx"
                    date_date = date_ext.split('_')[0] + '-' + date_ext.split('_')[1] + '-' + date_ext.split('_')[2]
                    print(date_date)
                    print(ext)
                else:
                    ext = "csv"
                    date_date = date_ext
                    print(date_date)
                    print(ext)
                print(filename_bq)
            else:
                filename_bq = filename.replace('.csv', '')
                filename_bq = unidecode(filename_bq).lower()
            table_name = self.project_id + '.' + self.dataset_id + "." + filename_bq
            print('this is the table name: ', table_name)
            print("---------------------")
                
            with zip_file.open(filename) as myfile:
                try:
                    df = pd.read_csv(myfile, sep=";")
                    print(df.head())
                    job_config = bigquery.LoadJobConfig(
                        # Specify a (partial) schema. All columns are always written to the
                        # Optionally, set the write disposition. BigQuery appends loaded rows
                        # to an existing table by default, but with WRITE_TRUNCATE write
                        # disposition it replaces the table with the loaded data.
                        write_disposition="WRITE_TRUNCATE",
                        )
                    print('we are here')
                    job = self.bq_client.load_table_from_dataframe(df, table_name, job_config=job_config)  # Make an API request.
                    job.result()
                    print('temoin 2')
                    print(f"{Fore.GREEN}{filename} is uploaded to {table_name}{Style.RESET_ALL}")
                    table = self.bq_client.get_table(table_name)  # Make an API request.
                    print(
                        "Loaded {} rows and {} columns to {}".format(
                            table.num_rows, len(table.schema), table_name
                        )
                    )
                    print(f"{Fore.GREEN}{filename} is uploaded to {table_name}{Style.RESET_ALL}")
                except Exception as e:
                    try:
                        print(f"{Fore.RED}Error: {e}{Style.RESET_ALL}")
                        print(f"{Fore.RED}Failed to upload {filename} to {table_name}{Style.RESET_ALL}", type(e).__name__)
                        myfile.seek(0)
                        df = pd.read_csv(myfile, sep=";")
                        df = df.astype(str)
                        job_config = bigquery.LoadJobConfig(
                            # Specify a (partial) schema. All columns are always written to the
                            # Optionally, set the write disposition. BigQuery appends loaded rows
                            # to an existing table by default, but with WRITE_TRUNCATE write
                            # disposition it replaces the table with the loaded data.
                            write_disposition="WRITE_TRUNCATE",
                            )
                        print('we are here')
                        job = self.bq_client.load_table_from_dataframe(df, table_name, job_config=job_config)  # Make an API request.
                        job.result()
                        print('temoin 2')
                        print(f"{Fore.GREEN}{filename} is uploaded to {table_name}{Style.RESET_ALL}")
                        table = self.bq_client.get_table(table_name)  # Make an API request.
                        print(
                            "Loaded {} rows and {} columns to {}".format(
                                table.num_rows, len(table.schema), table_name
                            )
                        )
                        print(f"{Fore.GREEN}{filename} is uploaded to {table_name}{Style.RESET_ALL}")
                        continue
                    except Exception as e:
                        print(f"{Fore.RED}Error: {e}{Style.RESET_ALL}")
                        print(f"{Fore.RED}Failed to upload {filename} to {table_name}{Style.RESET_ALL}", type(e).__name__)
    
    def upload_zipio_to_bq(self, zip_file_io):
        with zipfile.ZipFile(zip_file_io, 'r') as zip_file:
            self.upload_zip_to_bq(zip_file)


    def df_to_bq(self, df, table_name):
        """
        Uploads a DataFrame to BigQuery

        Parameters
        ----------
        df : pandas.DataFrame
            a DataFrame to be uploaded
        table_name : str
            a name of the table in BigQuery
        """
        print(f"{Fore.GREEN}DataFrame is uploaded to {table_name}{Style.RESET_ALL}")


        job_config = bigquery.LoadJobConfig(
                        # Specify a (partial) schema. All columns are always written to the
                        # Optionally, set the write disposition. BigQuery appends loaded rows
                        # to an existing table by default, but with WRITE_TRUNCATE write
                        # disposition it replaces the table with the loaded data.
                        write_disposition="WRITE_TRUNCATE",
                        )
        job = self.bq_client.load_table_from_dataframe(df, table_name, job_config=job_config)  # Make an API request.
        job.result()
        print(f"{Fore.GREEN} df is uploaded to {table_name}{Style.RESET_ALL}")
        table = self.bq_client.get_table(table_name)  # Make an API request.
        print(
                        "Loaded {} rows and {} columns to {}".format(
                            table.num_rows, len(table.schema), table_name
                        )
                    )
        print(f"{Fore.GREEN}DataFrame is uploaded to {table_name}{Style.RESET_ALL}")
    
    def bq_to_df(self, query):
        """
        Downloads a DataFrame from BigQuery

        Parameters
        ----------
        query : str
            a SQL query to be executed
        """

        # Execute the query and get the results as a query job
        query_job = self.bq_client.query(query)

        # Read the results into a pandas DataFrame
        df = query_job.to_dataframe()

        return df