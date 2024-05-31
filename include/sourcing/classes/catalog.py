from .connectors import GoogleConnector
import requests
import pandas as pd
from unidecode import unidecode
import os
from datetime import date
import urllib3
import re
import zipfile

class GetSourceCatalog:
    """
    A class for fetching and processing catalog data from a given API.
    """
    def __init__(self, url, headers):
        """
        Initialize the GetSourceCatalog object with the API URL and headers.

        Parameters:
        - url (str): The URL of the API.
        - headers (dict): Headers to be used in the API request.
        """
        # urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.api_url = url
        self.headers = headers

    def fetch_data_from_api(self):
        """
        Fetch data from the API.

        Returns:
        - list: List of catalog data from the API.
        """
        response = requests.get(self.api_url, headers=self.headers)
        if response.status_code == 200:
            print('Request is a success:', response.status_code)
            data = response.json()
            return data
        else:
            print('Request failed with this error:', response.status_code)
            return []
    
    def response_to_dataframe(self, data, table_name, download_url, 
                              table_id=None, file_format=None, last_update=None, 
                              dataset_id=None, dataset_name=None,
                              frequency=None, accessURL=None):
        """
        Process API response data into a DataFrame.

        Parameters:
        - data (list): List of catalog data.
        - table_name (str): Key for table name in each catalog entry.
        - download_url (str): Key for download URL in each catalog entry.
        - table_id (str): Key for table ID in each catalog entry.
        - file_format (str): Key for file format in each catalog entry.
        - last_update (str): Key for last update information in each catalog entry.
        - dataset_id (str): Key for dataset ID in each catalog entry.
        - dataset_name (str): Key for dataset name in each catalog entry.
        - frequency (str): Key for frequency information in each catalog entry.
        - accessURL (str): Key for access URL in each catalog entry.

        Returns:
        - pd.DataFrame: Processed catalog data in a DataFrame.
        """
        processed_data = []
        for table in data:
            content = {
                'table_id': table.get(table_id),
                'table_name': table.get(table_name),
                'download_URL': table.get(download_url),
                'accessURL': table.get(accessURL),
                'data_format': table.get(file_format),
                'last_update': table.get(last_update),
                'dataset_id': table.get(dataset_id),
                'dataset_name': table.get(dataset_name),
                'frequency': table.get(frequency)
            }
            processed_data.append(content)
          
        self.df_catalog = pd.DataFrame(processed_data).sort_values(by=['last_update', 'table_name', 'download_URL'], ascending=False)
        return self.df_catalog
    
    def save_to_csv(self, filename):
        """
        Save catalog data to a CSV file.

        Parameters:
        - filename (str): The name of the CSV file.
        """
        os.makedirs('data', exist_ok=True)
        os.makedirs('data/catalog', exist_ok=True)
        path = 'data/catalog/' + filename + '_' + str(date.today()) + '.csv'
        self.df_catalog.to_csv(path, index=False)
        print("CSV file has been loaded to this path", path)


class CustomCatalog(GoogleConnector):

    def __init__(self, credentials_path, project_id=None, dataset_name=None):
        super().__init__(credentials_path, project_id)
        self.project_id = project_id
        self.dataset_name = dataset_name
        

    def create_catalog_gcs(self, zip_file):
        """
        Creates a catalog of files contained in a zip archive for storage on Google Cloud Storage.

        This function extracts the list of files from a zip archive, filters out directories,
        and creates a catalog in the form of a DataFrame. Each entry in the catalog contains
        information about the file such as the name, update date, source format, and destination table
        name in BigQuery.

        Args:
            zip_file (ZipFile): The zip archive containing the files to catalog.

        Returns:
            pd.DataFrame: A DataFrame containing the catalog of files with the following columns:
                - 'filename': The name of the file (without extension for CSV files).
                - 'updated_at': The update date of the file.
                - 'source_format': The format of the file (csv or xlsx).
                - 'bq_dest_table': The name of the destination table in BigQuery.

        Note:
            - File names are converted to lowercase and accents are removed.
            - Files with names containing dates in the format YYYY_MM_DD are processed
            to extract and format the date and file extension.
            - If the file does not contain a path (no "/"), it is assumed to be a CSV file.

        Example:
            zip_file = ZipFile('path/to/your/zipfile.zip', 'r')
            catalog_df = create_catalog_gcs(zip_file)
        """
        file_list = [file for file in zip_file.namelist()]
        catalog = []
        filtered_list = list(filter(lambda x: not x.endswith('/'), file_list))
        for filename in filtered_list:
            if "/" in filename: 
                print(filename)
                filename_bq = filename.split('/')[1]
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

            table_name = self.dataset_name + "." + filename_bq
            dict_table = {
                'filename': filename_bq,
                'updated_at': date_date,
                'source_format': ext,
                'bq_dest_table': table_name
                }
            catalog.append(dict_table)
        
        df = pd.DataFrame(catalog)
        return df
    
    def create_catalog_gcs__io(self, zip_file_io):
        with zipfile.ZipFile(zip_file_io, 'r') as zip_file:
            return self.create_catalog_gcs(zip_file)
    
    def bq_catalog_all_datasets(self):
        print('Getting BigQuery modified dates...')

        dataset_list = list(self.bq_client.list_datasets())
        dataset_ids = []
        table_ids = []
        modified_dates = []
        for dataset_item in dataset_list:
            dataset = self.bq_client.get_dataset(dataset_item.reference)
            tables_list = list(self.bq_client.list_tables(dataset))

            for table_item in tables_list:
                table = self.bq_client.get_table(table_item.reference)
                dataset_ids.append(dataset.dataset_id)
                table_ids.append(table.table_id.lower())
                modified_dates.append(table.modified)
        data = {
            'bq_dataset': dataset_ids,
            'bq_table': table_ids,
            'bq_modified': modified_dates
        }
        print('Done.')
        self.df_bq = pd.DataFrame(data)
        self.df_bq['bq_modified'] = self.df_bq['bq_modified'].dt.tz_localize(None)
        self.df_bq['bq_table'] = self.df_bq['bq_table'].str.replace(r'_\d{8}', '', regex=True)
    
    def bq_dataset_catalog(self):
        print('Getting BigQuery modified dates...')

        table_ids = []
        modified_dates = []
        tables_list = list(self.bq_client.list_tables(self.dataset_name))

        for table_item in tables_list:
            table = self.bq_client.get_table(table_item.reference)
            table_ids.append(table.table_id.lower())
            modified_dates.append(table.modified)

        data = {
            'bq_dataset': self.dataset_name,
            'bq_table': table_ids,
            'bq_modified': modified_dates
            }
        print('Done.')

        df = pd.DataFrame(data)
        return df

            
class GetCnilCatalog(GetSourceCatalog):
    """
    A class for fetching and processing catalog data from CNIL with additional information.
    """
    def __init__(self, url, headers, url_additional_info):
        """
        Initialize the GetCnilCatalog object.

        Parameters:
        - url (str): The URL of the CNIL API.
        - headers (dict): Headers to be used in the API request.
        - url_additional_info (str): URL of the additional information CSV file.
        """
        super().__init__(url, headers)
        self.additional_info = url_additional_info

    def load_additional_info(self):
        """
        Load additional information from a CSV file.

        Returns:
        - pd.DataFrame: Additional information loaded from the CSV file.
        """
        try:
            self.df_dataset = pd.read_csv(self.additional_info, sep=';')
            return self.df_dataset
        except Exception as e:
            print(f"Error when loading CSV file : {e}")
            return None
        
    def identify_datasets_info(self): 
        """
        Identify dataset information and add it to the catalog DataFrame.
        """
        def find_dataset_id(row):
            for dataset_id in self.df_dataset.id:
                if dataset_id in row['accessURL']:
                    return dataset_id

        self.df_catalog['dataset_id'] = self.df_catalog.apply(lambda row: find_dataset_id(row), axis=1)
        return self.df_catalog

    def merge_additional_info(self):
        """
        Merge additional information into the catalog DataFrame.
        """
        self.df_catalog = self.df_catalog.merge(self.df_dataset[['id', 'slug', 'frequency']], left_on='dataset_id', right_on='id', how='left')
        self.df_catalog['dataset_name'] = self.df_catalog['slug']
        self.df_catalog = self.df_catalog.drop(columns=['id', 'frequency_x', 'slug'])
        self.df_catalog.rename(columns={'frequency_y': 'frequency'}, inplace=True)
        self.df_catalog.dropna(subset=['table_id'], inplace=True)
        self.df_catalog = self.df_catalog.reset_index()
        return self.df_catalog
