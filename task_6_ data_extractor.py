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

    def extract_from_s3(self, s3_address):
        s3_bucket = s3_address.split("//")[1].split("/")[0]
        s3_key = s3_address.split("//")[1].split("/", 1)[1]
        
        s3 = boto3.client('s3')
        try:
            response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
            data = pd.read_csv(response['Body'])
            return data
        except Exception as error:
            print("Error while extracting data from S3:", error)
            return None

    def extract_from_rds(self, table_name):
        with open(self.db_creds_file, 'r') as file:
            db_creds = yaml.safe_load(file)

        db_url = f"postgresql+psycopg2://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}"
        engine = create_engine(db_url)
        with engine.connect() as connection:
            query = f"SELECT * FROM {table_name}"
            data = pd.read_sql(query, connection)
        return data


class DataCleaning:
    def __init__(self):
        pass

    def clean_user_data(self, df):
        # Perform data cleaning operations on the user data DataFrame
        # Handle NULL values, errors with dates, incorrectly typed values, etc.
        # Return the cleaned DataFrame

    def convert_product_weights(self, df):
        def convert_weight(weight):
            weight = str(weight).strip()
            if weight.endswith('ml'):
                weight = weight[:-2]
                weight = float(weight) / 1000
            elif weight.endswith('g'):
                weight = weight[:-1]
                weight = float(weight) / 1000
            return weight

        df['weight'] = df['weight'].apply(convert_weight)
        return df

    def clean_products_data(self, df):
        # Clean the products DataFrame of any additional erroneous values
        # Handle NULL values, errors with formatting, erroneous values, etc.
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

# Retrieve data from S3
s3_address = 's3://data-handling-public/products.csv'
s3_data = extractor.extract_from_s3(s3_address)
if s3_data is not None:
    print(s3_data.head())
else:
    print("Failed to retrieve data from S3.")

# Clean user data
cleaned_user_data = cleaner.clean_user_data(df)
if cleaned_user_data is not None:
    print(cleaned_user_data.head())
else:
    print("Failed to clean the user data.")

# Convert product weights
converted_weights = cleaner.convert_product_weights(s3_data)
if converted_weights is not None:
    print(converted_weights.head())
else:
    print("Failed to convert product weights.")

# Clean product data
cleaned_products_data = cleaner.clean_products_data(converted_weights)
if cleaned_products_data is not None:
    print(cleaned_products_data.head())
else:
    print("Failed to clean the products data.")

# Upload cleaned products data to the database
products_table_name = 'dim_products'
connector.upload_to_db(cleaned_products_data, products_table_name)

# Extract data from the database
extracted_data = connector.extract_data(products_table_name)
if extracted_data:
    for row in extracted_data:
        print(row)

# Close the database connection
connector.close_connection(connection)
