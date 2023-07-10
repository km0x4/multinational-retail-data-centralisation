import json
import boto3

def extract_json_data(file_url):
        """Extracts the data from the JSON file.

        Args:
            file_url: The URL of the JSON file.

        Returns:
            A list of dictionaries, where each dictionary represents a row in the JSON file.
        """

        # Download the JSON file from S3.
        client = boto3.client('s3')
        file_object = client.get_object(Bucket='data-handling-public', Key='date_details.json')

        # Load the JSON file into a list of dictionaries.
        data = json.loads(file_object['Body'].read())

        return data

def clean_date_details(data):
        """Cleans the date details data.

        Args:
            data: The list of dictionaries of the date details data.

        Returns:
            A list of dictionaries of the date details data after cleaning.
        """

        # Check for NULL values.
        for row in data:
            for key, value in row.items():
                if value is None:
                    row[key] = ''

        # Check for errors with dates.
        for row in data:
            if 'order_date' in row:
                try:
                    datetime.datetime.strptime(row['order_date'], '%Y-%m-%d')
                except ValueError:
                    row['order_date'] = ''

        # Check for incorrectly typed values.
        for row in data:
            for key, value in row.items():
                try:
                    int(value)
                except ValueError:
                    try:
                        float(value)
                    except ValueError:
                        row[key] = ''

        return data

def upload_to_db(data, table_name):
        """Uploads the data to the database.

        Args:
            data: The list of dictionaries of the data to be uploaded.
            table_name: The name of the table in the database.
        """

        # Connect to the database.
        connection = psycopg2.connect(host='localhost', user='postgres', password='password', database='data_handling')

        # Insert the data into the database.
        cursor = connection.cursor()
        cursor.executemany('INSERT INTO {} (column1, column2, ...) VALUES (value1, value2, ...)'.format(table_name), data)
        connection.commit()
        cursor.close()

        # Close the connection to the database.
        connection.close()

if __name__ == '__main__':
        data = extract_json_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
        data = clean_date_details(data)
        upload_to_db(data, 'dim_date_times')
