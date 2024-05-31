import pandas as pd
from .connectors import GoogleConnector
import zipfile
import os
import io
from colorama import Fore, Style
import requests
from datetime import datetime
from io import BytesIO
import gzip
from datetime import date

class GCSProcessor(GoogleConnector):
    """
    A class for processing data on Google Cloud Storage (GCS).

    Args:
        credentials_path (str): Path to the Google Cloud credentials file.
        bucket_name (str): Name of the GCS bucket.
        output_folder_name (str, optional): Name of the output folder inside the bucket. Defaults to None.
        project_id (str, optional): Google Cloud project ID. Defaults to None.
    """

    def __init__(self, credentials_path, bucket_name, output_folder_name=None, project_id=None):
        """
        Initializes a GCSProcessor object.

        Args:
            credentials_path (str): Path to the Google Cloud credentials file.
            bucket_name (str): Name of the GCS bucket.
            output_folder_name (str, optional): Name of the output folder inside the bucket. Defaults to None.
            project_id (str, optional): Google Cloud project ID. Defaults to None.
        """
        super().__init__(credentials_path, project_id)
        self.bucket_name = bucket_name
        self.output_folder_name = output_folder_name

    def create_bucket(self):
        """
        Creates a new bucket in GCS. If the bucket already exists, it prints a message and does nothing.
        """
        try:
            self.storage_client.get_bucket(self.bucket_name).exists()
            print("Bucket already exists.")
        except:
            bucket = self.storage_client.bucket(self.bucket_name)
            # set the storage class to COLDLINE, which is the cheapest one
            bucket.storage_class = "COLDLINE"
            # set the location to europe-west1, which is in the EU
            new_bucket = self.storage_client.create_bucket(bucket, location="europe-west1")
            print('A new bucket created at {}'.format(new_bucket.name))

    def list_blobs(self, prefix=None):
        """
        Lists blobs in the GCS bucket with an optional prefix.

        Args:
            prefix (str, optional): Prefix to filter blobs. Defaults to None.

        Returns:
            list: List of blobs in the GCS bucket.
        """
        bucket = self.storage_client.get_bucket(self.bucket_name)
        print(bucket.name)
        blobs = list(self.storage_client.list_blobs(self.bucket_name, prefix=prefix))
        return blobs

    def upload_local_to_gcs(self, file_paths, dest_folder, dest_blobs=None, date=None):
        """
        Uploads multiple local files to GCS.

        Args:
            file_paths (list of str): Paths of the local files to be uploaded.
            dest_folder (str): Name of the folder inside the bucket where the data will be uploaded in GCS.
            dest_blob (str or list of str, optional): Destination blob name(s). Defaults to None.
            date (str or None, optional): Date string. Defaults to None.
        """
        dest_folder = dest_folder + "/"

        if date is None:
            date = ''
        else:
            date = str(date) + '/'

        if dest_blobs is None:
            for file_path in file_paths:
                print(file_path)
                if type(file_path) == str:
                    file_name = os.path.basename(file_path)
                    destination_blob_name = date + dest_folder + file_name
                    bucket = self.storage_client.bucket(self.bucket_name)
                    blob = bucket.blob(destination_blob_name)
                    blob.upload_from_filename(file_path, timeout=300)
                    print(
                        f"{Fore.GREEN} file {file_name} uploaded to GCS successfully to {destination_blob_name}.{Style.RESET_ALL}")
                elif type(file_path) == io.BytesIO or type(file_path) == io.StringIO:
                    raise ValueError(
                        "BytesIO or StringIO cannot be uploaded without a destination_blob_name. Try using the dest_blob parameter or save it as a CSV to your local system.")
                elif isinstance(file_path, pd.DataFrame):
                    raise ValueError(
                        "DataFrame cannot be uploaded without a destination_blob_name. Try using the dest_blob parameter or save it as a CSV to your local system.")
        else:
            for file_path, destination_blob_name in zip(file_paths, dest_blobs):
                if type(file_path) == str:
                    file_name = os.path.basename(file_path)
                    destination_blob_name = date + dest_folder + destination_blob_name
                    bucket = self.storage_client.bucket(self.bucket_name)
                    blob = bucket.blob(destination_blob_name)
                    blob.upload_from_filename(file_path, timeout=300)
                    print(
                        f"{Fore.GREEN} file {file_name} uploaded to GCS successfully to {destination_blob_name}.{Style.RESET_ALL}")
                elif isinstance(file_path, pd.DataFrame):
                    data = file_path
                    destination_blob_name = date + dest_folder + destination_blob_name
                    csv_data = data.to_csv(index=False)
                    bucket = self.storage_client.bucket(self.bucket_name)
                    blob_output = bucket.blob(destination_blob_name)
                    blob_output.upload_from_string(
                        csv_data, content_type='text/csv')
                    print(
                        f"{Fore.GREEN}{destination_blob_name} is uploaded to {destination_blob_name}.{Style.RESET_ALL}")
                elif isinstance(file_path, zipfile.ZipFile):
                    destination_blob_name = date + dest_folder + destination_blob_name
                    bucket = self.storage_client.bucket(self.bucket_name)
                    blob = bucket.blob(destination_blob_name)
                    io_object = io.BytesIO(file_path)
                    blob.upload_from_file(
                        io_object, content_type='application/zip')
                    print(
                        f"{Fore.GREEN} file {destination_blob_name} uploaded to GCS successfully to {destination_blob_name}.{Style.RESET_ALL}")
                elif type(file_path) == io.BytesIO or type(file_path) == io.StringIO:
                    destination_blob_name = date + dest_folder + destination_blob_name
                    bucket = self.storage_client.bucket(self.bucket_name)
                    blob = bucket.blob(destination_blob_name)
                    blob.upload_from_file(
                        file_path, content_type='application/zip')
                    print(
                        f"{Fore.GREEN} file {destination_blob_name} uploaded to GCS successfully to {destination_blob_name}.{Style.RESET_ALL}")

    def dl_and_up_from_URLs(self, urls, dest_folder, dest_blobs=None, date=None):
        """
        Downloads data from multiple URLs and uploads them to GCS.

        Args:
            urls (list of str): URLs of the data to be downloaded.
            dest_folder (str): Name of the folder inside the bucket where the data will be uploaded in GCS.
            dest_blob (str or list of str, optional): Destination blob name(s). Defaults to None.
            date (str or None, optional): Date string. Defaults to None.
        """
        if date is None:
            date = ''
        else:
            date = str(date) + '/'

        dest_folder = dest_folder + "/"
        if dest_blobs is None:
            for url in urls:
                response = requests.get(url)
                if response.status_code == 200:
                    file_stream = BytesIO(response.content)
                    file_name = os.path.basename(url)
                    destination_blob_name = date + dest_folder + file_name
                    bucket = self.storage_client.bucket(self.bucket_name)
                    blob = bucket.blob(destination_blob_name)
                    blob.upload_from_file(file_stream, content_type='application/zip')
                    print(f"{Fore.GREEN}Raw file {file_name} downloaded and uploaded to GCS successfully to {destination_blob_name}.{Style.RESET_ALL}")
                else:
                    print(f"{Fore.RED}Request for {url} failed with error {response.status_code}.{Style.RESET_ALL}")
        else:
            for url, destination_blob_name in zip(urls, dest_blobs):
                response = requests.get(url)
                if response.status_code == 200:
                    file_stream = BytesIO(response.content)
                    destination_blob_name = date + dest_folder + destination_blob_name
                    bucket = self.storage_client.bucket(self.bucket_name)
                    blob = bucket.blob(destination_blob_name)
                    blob.upload_from_file(file_stream, content_type='application/zip')
                    print(f"{Fore.GREEN}Raw file {destination_blob_name} downloaded and uploaded to GCS successfully to {destination_blob_name}.{Style.RESET_ALL}")
                else:
                    print(f"{Fore.RED}Request for {url} failed with error {response.status_code}.{Style.RESET_ALL}")

    def extract_and_upload_selection(self, blobs, folder_name=None, date=None):
        """
        Extracts data from a compressed file and uploads it to GCS.

        Args:
            blobs (list): List of blobs to be extracted and uploaded to GCS.
            folder_name (str, optional): Name of the folder inside the bucket where the data will be uploaded in GCS. Defaults to None.
            date (str or None, optional): Date string. Defaults to None.
        """
        if date is None:
            date = ''
        else:
            date = str(date) + '/'

        if folder_name is not None:
            folder_name = date + folder_name + "/"

        bucket = self.storage_client.get_bucket(self.bucket_name)
        for blob in blobs:
            print(f"{Fore.CYAN}Start extracting and uploading to GCS: {date}/{blob.name}{Style.RESET_ALL}")
            zip_data = blob.download_as_bytes()

            if blob.name.endswith('.zip'):
                with zipfile.ZipFile(io.BytesIO(zip_data)) as zip_ref:
                # Extract all files to the folder in GCS
                    for file_name in zip_ref.namelist():
                        # Construct the destination blob name
                        destination_blob_name = os.path.join(folder_name, file_name)
                        # Extract the file from the zip and upload it to GCS
                        extracted_data = zip_ref.read(file_name)
                        blob = bucket.blob(destination_blob_name)
                        blob.upload_from_string(extracted_data)

            elif blob.name.endswith('.gz'):
                with gzip.GzipFile(fileobj=io.BytesIO(zip_data)) as zip_ref:
                    for file_name in zip_ref.namelist():
                        # Construct the destination blob name
                        destination_blob_name = os.path.join(folder_name, file_name)
                        # Extract the file from the zip and upload it to GCS
                        extracted_data = zip_ref.read(file_name)
                        blob = bucket.blob(destination_blob_name)
                        blob.upload_from_string(extracted_data)
            
            elif blob.name.endswith('.csv'):
                print('No need to extract, it is already a CSV file')

    def get_file_string_io(self, blob_name):
        """
        Retrieves file content from GCS blob as BytesIO object.

        Args:
            blob_name (str): Name of the GCS blob.

        Returns:
            BytesIO: BytesIO object containing file content.
        """
        bucket = self.storage_client.get_bucket(self.bucket_name)
        blob = bucket.blob(blob_name)
        return io.BytesIO(blob.download_as_string())

    def get_zip_file_object(self, blob_name):
        """
        Retrieves zip file content from GCS blob.

        Args:
            blob_name (str): Name of the GCS blob.

        Returns:
            zipfile.ZipFile: ZipFile object containing zip file content.
        """
        bucket = self.storage_client.get_bucket(self.bucket_name)
        blob = bucket.blob(blob_name)
        zip_file = io.BytesIO(blob.download_as_bytes())
        return zip_file

    def download_files_from_catalog(self, catalog_path, filter_on=None):
        """
        Downloads files listed in a catalog CSV file and returns their content.

        Args:
            catalog_path (str): Path to the catalog CSV file.

        Returns:
            list: List of tuples containing file paths and their content.
        """
        csv_catalog = self.get_file_string_io(catalog_path)
        df_catalog = pd.read_csv(csv_catalog)

        if filter_on is not None:
            df_catalog = df_catalog[df_catalog['data_format'] == filter_on]

        files = []
        for index, row in df_catalog.iterrows():
            table_name = row.table_name

            if row.dataset_name is not None:
                dest_folder = row.dataset_name
            else:
                dest_folder = 'unknown'

            if row.last_update is not None:
                last_date = row.last_update
                last_date = self.extract_date(last_date)
            else:
                current_datetime = datetime.now()
                last_date = current_datetime.date()
            
            if pd.notna(row.data_format):
                data_format = "." + row.data_format
            else:
                data_format = ''

            file_path = f'{dest_folder}/{table_name}_{last_date}{data_format}'

            if pd.notna(row.download_URL):
                url = row.download_URL
                response = requests.get(url)
                if response.status_code == 200:
                    print('Current file downloading:', file_path)
                    files.append((file_path, response.content))
                else:
                    print(f"Failed to download file from {url}")
            else:
                print(f"File {table_name} does not have a download URL.")
        return files
    
    def create_zip_from_files(self, files):
        """
        Creates a zip file from a list of file content.

        Args:
            files (list): List of tuples containing file paths and their content.

        Returns:
            BytesIO: BytesIO object containing the created zip file.
        """
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', compression=zipfile.ZIP_DEFLATED) as zip_file:
            for file_path, content in files:
                print('Current file:', file_path)
                directory, filename = os.path.split(file_path)
                zip_file.writestr(file_path, content)
        zip_buffer.seek(0)
        return zip_buffer

    def extract_date(self, date_str):
        """
        Extracts and converts date strings to datetime objects.

        Args:
            date_str (str): Date string in the format '%Y-%m-%dT%H:%M:%S.%f' or '%Y-%m-%dT%H:%M:%S'.

        Returns:
            datetime.date: Date extracted from the date string.
        """
        try:
            timestamp_obj = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S.%f')
        except:
            timestamp_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S')
        return timestamp_obj.date()
