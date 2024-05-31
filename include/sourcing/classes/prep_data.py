import pandas as pd
from pandas.errors import ParserError
import zipfile
import io
import os
import re
from unidecode import unidecode
from colorama import Fore, Style
import os

class PrepFilesBQ:
    """
    Class to preprocess files before loading them into BigQuery.
    The main difficulty is to handle the different formats of the files.

    Attributes:
    - paths: List of file paths to be processed.
    - zip_file: Optional. Zip file containing files to be processed.
    """

    def __init__(self, paths=None):
        """
        Initialize PrepFilesBQ object.

        Args:
        - paths: Optinnal. List of file paths.
        - zip_file: Optional. Zip file containing files.
        """
        self.paths = paths
        self.errors_backlog = []

    def process_all_files(self):
        """
        Processes all files in the paths attribute.
        """
        for path in self.paths:
            print("---------------------------------------------------")
            print(Fore.GREEN + path + Style.RESET_ALL)
            df = self.open_df(path)
            if df is not None:
                df = self.transposed(df)
                df = self.drop_empty_columns(df)
                df = self.columns_formatter(df)
                df = self.check_column_clean(df)
                df = self.rename_duplicate_columns(df)
                self.return_csv(df, path)
                print(Fore.GREEN + f"{path} processed successfully!" + Style.RESET_ALL)
                print("---------------------------------------------------")
            else:
                print(Fore.RED + f"{path} not processed!" + Style.RESET_ALL)
                print("---------------------------------------------------")

    def process_df(self, df):
        if df is not None:
            self.dic_errors = {
                            'path': df,
                            'error': []
                        }
            print("---------------------------------------------------")
            df = self.transposed(df)
            df = self.drop_empty_columns(df)
            df = self.columns_formatter(df)
            df = self.check_column_clean(df)
            df = self.rename_duplicate_columns(df)
            return df

    def process_dfs(self, zip_file_io, filter_on=None):
        """
        Processes a zip file containing multiple files.

        Args:
        - zip_file: Zip file object.

        Returns:
        - BytesIO: BytesIO object containing the processed zip file.
        """

        # Get list of files in zip
        # Namelist will returns a list of all the files and directories in the archive
        with zipfile.ZipFile(zip_file_io, 'r') as zip_file:
            file_list = [file for file in zip_file.namelist()]

            if filter_on is not None:
                filtered_list = list(filter(lambda x: not x.endswith('/'), file_list))
                filtered_list = list(filter(lambda x: filter_on in x, filtered_list))
            else:
                filtered_list = list(filter(lambda x: not x.endswith('/'), file_list))

            self.filtered_list = filtered_list

        # Filter out directories

            dfs = []
            for path in filtered_list:
                print(Fore.GREEN + 'current:', path + Style.RESET_ALL)
                with zip_file.open(path) as file:
                    self.dic_errors = {
                            'path': path,
                            'error': []
                        }
                    print("---------------------------------------------------")
                    print(Fore.GREEN + path + Style.RESET_ALL)
                    if path == '.DS_Store':
                        continue
                    df = self.open_df(path, file)
                    if df is not None:
                        df = self.transposed(df)
                        df = self.drop_empty_columns(df)
                        df = self.columns_formatter(df)
                        df = self.check_column_clean(df)
                        df = self.rename_duplicate_columns(df)

                        dict_df = {
                            'path': self.replace_char_in_df_names(path),
                            'df': df
                        }
                        dfs.append(dict_df)

                        print(Fore.GREEN + f"{path} processed successfully!" + Style.RESET_ALL)
                        print("---------------------------------------------------")
                        self.errors_backlog.append(self.dic_errors)
                        
                    else:
                        print(Fore.RED + f"{path} not processed!" + Style.RESET_ALL)
                        self.files_not_processed.append(path)
                        print("---------------------------------------------------")
        
        self.error_backlog_dataframe()
        print('For information on errors, please check the errors_backlog attribute.')
        return dfs
    
    def error_backlog_dataframe(self):
        """
        Returns the errors_backlog attribute.
        """
        
        df = pd.DataFrame(self.errors_backlog)
        df = df.explode('error')
        if not os.path.exists('data/prep_data'):
            os.makedirs('data/prep_data')
        df.to_csv('data/prep_data/errors_backlog.csv', index=False)
        self.errors_backlog = df
        return df
    
    def replace_char_in_df_names(self, path):
        """
        Replaces characters in filenames associated with df.

        Args:
        - path: a filename associated with a df.

        Returns:
        - new_filename: a new filename adapted for bigquery.
        """
        new_filename = path.replace(' ', '_').replace('-', '_').replace(".", "_").replace("(", "_").replace(")", "").replace(",", "")
        new_filename = new_filename + ".csv"
        return new_filename
    
    def save_dfs_to_zip(self, dfs):
        """
        Save a DataFrame to a CSV file within a zip archive.

        Parameters:
            dfs (pandas.DataFrame): The DataFrame to be saved.

        Returns:
            None
        """
        # Create a zip file
        zip_output = io.BytesIO()
        with zipfile.ZipFile(zip_output, 'w') as zipf:
                for file in dfs:
                    df = file['df']
                    filename = file['path']
                    # Write DataFrame to a CSV file in memory
                    csv_buffer = df.to_csv(index=False, sep=';')

                    # Add the CSV file to the zip archive
                    zipf.writestr(filename, csv_buffer)

        zip_output.seek(0)
        return zip_output
    
    def open_df(self, path, file=None):
        """
        Opens and reads a file.

        Args:
        - path: File path.
        - file: Optional. File object or path.

        Returns:
        - DataFrame: DataFrame containing file data.
        """
        if file is None:
            file = path

        print(file)
        print(path)

        df = None

        if path.endswith('.csv'):
            print('.csv found')
            df = self.open_csv_file(file=file)
        elif path.endswith('.xlsx'):
            print('.xsxl found')
            df = self.open_excel_file(file=file)
        elif "." not in path:
            try:
                print('try to read as csv')
                df = self.open_csv_file(file=file)
            except ParserError as e:
                self.dic_errors['error'].append(f'{type(e).__name__} and {e}')
                # print(f"{Fore.RED}Exception type: {type(e).__name__}{Style.RESET_ALL}")
                # print(f"{Fore.RED}Exception: {e}{Style.RESET_ALL}")
                # print('try to read as excel')
                df = self.open_excel_file(file=file)
                print(df.shape)
            except UnicodeError as e:
                self.dic_errors['error'].append(f'{type(e).__name__} and {e}')
                # print(f"{Fore.RED}Exception type: {type(e).__name__}{Style.RESET_ALL}")
                # print(f"{Fore.RED}Exception: {e}{Style.RESET_ALL}")
                # print('try to read as excel')
                df = self.open_excel_file(file=file)
                print(df.shape)
            except Exception as e:
                self.dic_errors['error'].append(f'{type(e).__name__} and {e}')
                # print(f"{Fore.RED}Exception type: {type(e).__name__}{Style.RESET_ALL}")
                # print(f"{Fore.RED}Exception: {e}{Style.RESET_ALL}")
                # print('cant read as df')
                df = None

        print('opened df, return from open_df')
        return df
    
    def open_csv_file(self, file):
        """
        Opens and reads a CSV file.


        Args:
        - path: File path.
        - file: File object or path.

        Returns:
        - DataFrame: DataFrame containing CSV data.
        """
        print('file:', file)


        try:
            df = pd.read_csv(file)
        except UnicodeDecodeError as e:
            if 'zipfile.ZipExtFile' in str(type(file)):
                    file.seek(0)
            self.dic_errors['error'].append(f'{type(e).__name__} and {e}')
            print(f"{Fore.RED}Exception type (first attempt): {type(e).__name__}{Style.RESET_ALL}")
            print(f"{Fore.RED}Exception: {e}{Style.RESET_ALL}")
            df = pd.read_csv(file, sep=';', encoding='latin1')
        except ParserError as e:
            if 'zipfile.ZipExtFile' in str(type(file)):
                    file.seek(0)
            self.dic_errors['error'].append(f'{type(e).__name__} and {e}')
            print(f"{Fore.RED}Exception type (first attempt): {type(e).__name__}{Style.RESET_ALL}")
            print(e)
            print('trying to open csv with sep = ";"')
            try:
                df = pd.read_csv(file, sep=';') 
            except Exception as e:
                self.dic_errors['error'].append(f'{type(e).__name__} and {e}')
                print(f"{Fore.RED}Exception type (second attempt): {type(e).__name__}{Style.RESET_ALL}")
                print(f"{Fore.RED}Exception: {e}{Style.RESET_ALL}")
                print('Unable to open CSV file')
                df = None

        if df is not None:
            # print(df.shape)
            df = self.correct_shape(file, df)
            return df
        else:
            return df

    def open_excel_file(self, file):
        """
        Opens and reads an Excel file.

        Args:
        - file: File object or path.

        Returns:
        - DataFrame: DataFrame containing Excel data.
        """
        df = pd.read_excel(file)
        print(df.shape)
        df = self.correct_shape(file, df)
        return df

    def verify_error_onbadlines(self, path, df):
        """
        Verifies and handles errors caused by bad lines in the CSV file.
        If more than 1% of rows are skipped, the file is considered bad. It will be skipped.

        Args:
        - path: File path.
        - df: DataFrame containing the CSV data.

        Returns:
        - DataFrame: Processed DataFrame.
        """
        with open(path) as f:
            len_csv = sum(1 for line in f)

        print(len_csv)
        print(df.shape)
        number_of_skipped_rows = len_csv - df.shape[0]
        print('number_of_skipped_rows:' , number_of_skipped_rows)
        errors_imp = number_of_skipped_rows/len_csv * 100
        if errors_imp > 1:
            string = 'More than 1 percent of rows skipped, file is not good'
            self.dic_errors['error'].append(string)
            print(string)
            df = None
        else:
            print('More less 1 percent of rows skipped, file is okay')
        
        return df

    def correct_shape(self, file, df):
        """
        Corrects the shape of the DataFrame if needed.
        A csv file can be read with ',' separator and give a wrong shape.
        This methods can correct the shape of the DataFrame with the good separator or by skipping the first row.

        Args:
        - file: File object or path.
        - df: DataFrame to be corrected.

        Returns:
        - DataFrame: Corrected DataFrame.
        """
        try:
            # Handling if CSV is read with ';' separator
            if df.shape[1] == 1:
                self.dic_errors['error'].append('columns shape is 1, csv read with ;')
                # print('columns shape is 1, csv read with ;')
                # print(type(file))
                if 'zipfile.ZipExtFile' in str(type(file)):
                    file.seek(0)
                df = pd.read_csv(file, sep=';')
                if df.shape[1] == 1:
                    self.dic_errors['error'].append('columns shape is 1, csv read with ;, try to skip first row')
                    print('try to find headers in 2nd row')
                    if 'zipfile.ZipExtFile' in str(type(file)):
                        file.seek(0)
                    df = pd.read_csv(file, sep=';', skiprows=1)
                else:
                    df = df
            # Handling if column names are unnamed
            elif 'unnamed' in str(df.columns[1]).lower():
                self.dic_errors['error'].append('unnamed columns')
                # print('try to find headers in 2nd row')
                df.columns = df.iloc[0]
                df = df[1:]
        except ParserError as e:
            self.dic_errors['error'].append(f'{type(e).__name__} and {e}')
            # print(f"{Fore.RED}Exception type: {type(e).__name__}{Style.RESET_ALL}")
            # print(f"{Fore.RED}Exception: {e}{Style.RESET_ALL}, return None")
            # print('cant correct shape')
            df = None
        except Exception as e:
            self.dic_errors['error'].append(f'{type(e).__name__} and {e}')
            # print(f"{Fore.RED}Exception type: {type(e).__name__}{Style.RESET_ALL}")
            # print(f"{Fore.RED}Exception: {e}{Style.RESET_ALL}, return None")
            df = None
    
        return df

    def transposed(self, df):
        """
        Transposes the DataFrame if the number of columns exceeds the number of rows.
        A DataFrame with more columns than rows is transposed.

        Args:
        - df: DataFrame to be transposed.

        Returns:
        - DataFrame: Transposed DataFrame.
        """
        if df.shape[1] > df.shape[0]:
            self.dic_errors['error'].append('More columns than rows, need to transpose')
            # print('More columns than rows, need to transpose')
            df = df.transpose()
            df.columns = df.iloc[0]
            df = df[1:]  
            column1 = df.columns.name
            df = df.reset_index()
            df = df.rename(columns={df.columns[0]: column1})
        else:
            print('More rows than columns, no need to transpose')

        return df

    
    
    def columns_formatter(self, df):
        """
        Formats column names of the DataFrame to be clean and readable by BigQuery.

        Args:
        - df: DataFrame.

        Returns:
        - DataFrame: DataFrame with formatted column names.
        """
        new_columns = []
        for index, column in enumerate(df.columns):
            try:
                column = str(column)
                column = re.sub(r'[!\"$()\*\.,\/;?\@\[\]\\^`{}\~]', '', column)
                column = column.strip().lower().replace(" ", "_")
                column = unidecode(column)
                column = column.replace("\n", "_")
                column = column.replace("'", "")
                column = column.replace("-", "")
                column = column.replace("&", 'and')
                column = column.replace('<', "").replace('>', "")
                column = column[:200]
                new_columns.append(column)
            except AttributeError:
                new_columns.append(column)
                continue
    
        df.columns = new_columns
        return df
    
    def check_column_clean(self, df):
        """
        Checks if column names are clean and reformats them if needed.

        Args:
        - df: DataFrame.

        Returns:
        - DataFrame: DataFrame with cleaned column names.
        """
        for index, column in enumerate(df.columns):
            try: 
                pattern = r"^[a-zA-Z0-9_]+$"
                is_cleaned = re.match(pattern, column)
                if is_cleaned and len(column) <= 200:
                    pass
                else:
                    self.dic_errors['error'].append(f"Column name {column} is not clean.")
                    print("The column_formatter method didn't work perfectly. Executing again the method...")
                    self.columns_formatter(df)
                    break 
            except TypeError:
                continue

        print("Re-execution completed.")
        print("The column_formatter method worked perfectly.")
        return df
    
    def rename_duplicate_columns(self, df):
        """
        Renames duplicate columns in the DataFrame.

        Args:
        - df: DataFrame.

        Returns:
        - DataFrame: DataFrame with renamed duplicate columns.
        """
        column_count = {}
        new_columns = []

        for column in df.columns:
            if column in column_count:
                column_count[column] += 1
                new_column = f"{column}_{column_count[column]}"
            else:
                column_count[column] = 1
                new_column = column

            new_columns.append(new_column)
        df.columns = new_columns
        return df

    def drop_empty_columns(self, df):
        """
        Drops empty columns from the DataFrame.

        Args:
        - df: DataFrame.

        Returns:
        - DataFrame: DataFrame with empty columns dropped.
        """
        if df is not None:
            self.dic_errors['error'].append('Drop empty columns')
            df = df.dropna(axis=1, how='all')
            return df
        else:
            return df
    
    def return_csv(self, df, path):
        """
        Saves DataFrame to CSV file.

        Args:
        - df: DataFrame.
        - path: File path to save CSV file.
        """
        path_split = path.split('/')
        dataset = path_split[2]
        if '.' in path_split[3]:
            table_ext = path_split[3].split('.')
            table = table_ext[0]
            extension = table_ext[1]
        else:
            table = path_split[3]
            extension = ""
        table = table.replace(" ", "_")
        table = table.replace("-", "_")
        table = table.replace("(", "_").replace(")", "_")
        os.makedirs(f'data/prep_datasets/{path_split[2]}', exist_ok=True)
        df.to_csv(f'data/prep_datasets/{dataset}/{table}_{extension}.csv', index=False, sep=";")
    

class PrepDataCnilBQ(PrepFilesBQ):

    def __init__(self, paths):
        super().__init__(paths)
        self.files_not_processed = []
        self.errors_backlog = []

    def transposed(self, df):
        if df.shape[1] > df.shape[0]:
            self.dic_errors['error'].append('More columns than rows, need to transpose')
            # print('More columns than rows, need to transpose')
            df = df.transpose()
            df.columns = df.iloc[0]
            df = df[1:]  
            column1 = df.columns.name
            df = df.reset_index()
            df = df.rename(columns={df.columns[0]: column1})
        elif df.shape[1] <= df.shape[0] and df.columns[0].lower() == 'année' and str(df.columns[1]).isdigit() and len(str(df.columns[1])) == 4:
            self.dic_errors['error'].append('Values for years in first column, need to transpose')
            # print('Values for years in first column, need to transpose')
            df = df.transpose()
            df.columns = df.iloc[0]
            df = df[1:] 
            column1 = df.columns.name
            df = df.reset_index()
            df = df.rename(columns={df.columns[0]: column1})
        else:
            print('More rows than columns, no need to transpose')
        
        return df
    
    def columns_formatter(self, df):
        new_columns = []
        for index, column in enumerate(df.columns):
            try:
                column = str(column)
                column = re.sub(r'[!\"$()\*\.,\/;?\@\[\]\\^`{}\~]', '', column)
                column = column.strip().lower().replace(" ", "_")
                column = unidecode(column)
                column = column.replace("\n", "_")
                column = column.replace("'", "")
                column = column.replace("-", "")
                column = column.replace("&", 'and')
                column = column.replace('https:edpbeuropaeuaboutedpbboardmembers_fr', "").replace('https:wwwafapdporglafapdpmembres', "")
                column = column.replace('<', "").replace('>', "")
                column = column[:200]
                new_columns.append(column)
            except AttributeError:
                new_columns.append(column)
                continue
    
        df.columns = new_columns
        return df
