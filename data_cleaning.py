import csv
import requests
import boto3
import psycopg2

class DataCleaning:

    def __init__(self, data_extractor, database_connector):
        """Initializes the DataCleaning class.

        Args:
            data_extractor: The DataExtractor class object.
            database_connector: The DatabaseConnector class object.
        """

        self.data_extractor = data_extractor
        self.database_connector = database_connector

    def clean_data_from_csv(self, file_path):
        """Cleans data from a CSV file.

        Args:
            file_path: The path to the CSV file.

        Returns:
            The cleaned data.
        """

        data = self.data_extractor.extract_data_from_csv(file_path)

        # Clean the data.
        for row in data:
            for key, value in row.items():
                if value == '':
                    row[key] = None

        return data

    def clean_data_from_api(self, api_url):
        """Cleans data from an API.

        Args:
            api_url: The URL of the API.

        Returns:
            The cleaned data.
        """

        data = self.data_extractor.extract_data_from_api(api_url)

        # Clean the data.
        for row in data:
            for key, value in row.items():
                if value == '':
                    row[key] = None

        return data

    def clean_data_from_s3_bucket(self, bucket_name, file_key):
        """Cleans data from an S3 bucket.

        Args:
            bucket_name: The name of the S3 bucket.
            file_key: The key of the file in the S3 bucket.

        Returns:
            The cleaned data.
        """

        data = self.data_extractor.extract_data_from_s3_bucket(bucket_name, file_key)

        # Clean the data.
        for row in data:
            for key, value in row.items():
                if value == '':
                    row[key] = None

        return data

    def upload_cleaned_data_to_database(self, data, table_name):
        """Uploads cleaned data to the database.

        Args:
            data: The cleaned data.
            table_name: The name of the table in the database.
        """

        database_connector = self.database_connector
        connection = database_connector.connect()
        database_connector.upload_data(connection, data, table_name)
        connection.close()
