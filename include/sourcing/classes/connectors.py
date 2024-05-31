from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound


class GoogleConnector:
    """
    This class establishes a connection to Google Cloud Platform services such as BigQuery and Cloud Storage,
    using credentials from a service account key file.

    Args:
        credentials_path (str): The path to the JSON service account key file for authentication.
        project_id (str): The Google Cloud Platform project ID to connect to.

    Attributes:
        credentials: The credentials extracted from the service account key file.
        bq_client: The BigQuery client used to interact with BigQuery in the specified project.
        storage_client: The Cloud Storage client used to interact with storage buckets in the project.

    """

    def __init__(self, credentials_path, project_id):
        """
        Initializes a new instance of GoogleConnector with the specified credentials and project ID.

        Args:
            credentials_path (str): The path to the JSON service account key file for authentication.
            project_id (str): The Google Cloud Platform project ID to connect to.
        """
        self.credentials = service_account.Credentials.from_service_account_file(credentials_path)
        self.bq_client = bigquery.Client(credentials=self.credentials, project=project_id)
        self.storage_client = storage.Client(credentials=self.credentials)
