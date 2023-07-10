import psycopg2
import psycopg2.extras
import yaml
from sqlalchemy import create_engine
import pandas as pd


class DatabaseConnector:
    def __init__(self, db_creds_file):
        self.db_creds_file = db_creds_file

    def read_db_creds(self):
        with open(self.db_creds_file, 'r') as file:
            db_creds = yaml.safe_load(file)
        return db_creds

    def init_db_engine(self):
        db_creds = self.read_db_creds()
        db_url = f"postgresql+psycopg2://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}"
        engine = create_engine(db_url)
        return engine

    def list_db_tables(self):
        engine = self.init_db_engine()
        table_names = engine.table_names()
        return table_names

    def connect(self):
        try:
            db_creds = self.read_db_creds()

            connection = psycopg2.connect(
                host=db_creds['RDS_HOST'],
                port=db_creds['RDS_PORT'],
                database=db_creds['RDS_DATABASE'],
                user=db_creds['RDS_USER'],
                password=db_creds['RDS_PASSWORD']
            )
            print("Connected to the database successfully.")
            return connection
        except psycopg2.Error as error:
            print("Error while connecting to the database:", error)
            return None

    def upload_data(self, connection, table_name, data):
        try:
            cursor = connection.cursor()
            columns = data[0].keys()
            values = [tuple(row.values()) for row in data]
            insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"
            psycopg2.extras.execute_values(cursor, insert_query, values)
            connection.commit()
            print("Data uploaded successfully.")
        except psycopg2.Error as error:
            print("Error while uploading data to the database:", error)
            connection.rollback()

    def close_connection(self, connection):
        if connection:
            connection.close()
            print("Database connection closed.")

    def extract_data(self, table_name):
        try:
            db_creds = self.read_db_creds()

            connection = psycopg2.connect(
                host=db_creds['RDS_HOST'],
                port=db_creds['RDS_PORT'],
                database=db_creds['RDS_DATABASE'],
                user=db_creds['RDS_USER'],
                password=db_creds['RDS_PASSWORD']
            )

            cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            query = f"SELECT * FROM {table_name}"
            cursor.execute(query)
            data = cursor.fetchall()
            cursor.close()
            self.close_connection(connection)
            return data
        except psycopg2.Error as error:
            print("Error while extracting data from the database:", error)
            return None

    def upload_to_db(self, dataframe, table_name):
        try:
            engine = self.init_db_engine()
            dataframe.to_sql(table_name, engine, if_exists='append', index=False)
            print("Data uploaded to the database successfully.")
        except Exception as error:
            print("Error while uploading data to the database:", error)


# Example usage
db_creds_file = 'db_creds.yaml'
connector = DatabaseConnector(db_creds_file)

# List all tables in the database
table_names = connector.list_db_tables()
print(table_names)

# Connect to the database
connection = connector.connect()

# Upload data to the database
data = [
    {'name': 'John Doe', 'age': 30, 'city': 'New York'},
    {'name': 'Jane Smith', 'age': 25, 'city': 'Los Angeles'},
    {'name': 'Tom Johnson', 'age': 35, 'city': 'Chicago'}
]
table_name = 'users'
connector.upload_data(connection, table_name, data)

# Upload pandas DataFrame to the database
df = pd.DataFrame(data)
connector.upload_to_db(df, table_name)

# Extract data from the database
extracted_data = connector.extract_data(table_name)
if extracted_data:
    for row in extracted_data:
        print(row)

# Close the database connection
connector.close_connection(connection)
