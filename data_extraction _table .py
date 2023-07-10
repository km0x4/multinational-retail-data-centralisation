import csv
import requests
import boto3
import yaml
import pandas as pd
from sqlalchemy import create_engine


class DataExtractor:
    def __init__(self, db_creds_file):
        self.db_creds_file = db_creds_file

    def extract_from_csv(self, filename):
        data = []
        with open(filename, 'r') as file:
            csv_reader = csv.reader(file)
            header = next(csv_reader)
            for row in csv_reader:
                data.append(dict(zip(header, row)))
        return data

    def extract_from_api(self, url):
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print("Failed to retrieve data from the API.")
            return None

    def extract_from_s3(self, bucket_name, file_key):
        s3 = boto3.resource('s3')
        obj = s3.Object(bucket_name, file_key)
        data = obj.get()['Body'].read().decode('utf-8')
        return data

    def extract_from_rds(self, table_name):
        with open(self.db_creds_file, 'r') as file:
            db_creds = yaml.safe_load(file)

        db_url = f"postgresql+psycopg2://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}"
        engine = create_engine(db_url)
        with engine.connect() as connection:
            query = f"SELECT * FROM {table_name}"
            data = pd.read_sql(query, connection)
        return data

    def read_rds_table(self, connector, table_name):
        data = connector.extract_data(table_name)
        if data:
            df = pd.DataFrame(data)
            return df
        else:
            print(f"Failed to extract data from the table '{table_name}'.")
            return None


class DataCleaning:
    def __init__(self):
        pass

    def clean_user_data(self, df):
        # Perform data cleaning operations on the user data DataFrame
        # Handle NULL values, errors with dates, incorrectly typed values, etc.
        # Return the cleaned DataFrame


# Example usage
db_creds_file = 'db_creds.yaml'
connector = DatabaseConnector(db_creds_file)
extractor = DataExtractor(db_creds_file)
cleaner = DataCleaning()

# List all tables in the database
table_names = connector.list_db_tables()
print(table_names)

# Read RDS table into a pandas DataFrame
table_name = 'users'
df = extractor.read_rds_table(connector, table_name)
if df is not None:
    print(df.head())
else:
    print(f"Failed to read the table '{table_name}'.")

# Clean user data
cleaned_df = cleaner.clean_user_data(df)
if cleaned_df is not None:
    print(cleaned_df.head())
else:
    print("Failed to clean the user data.")
