import csv
import requests
import boto3


class DataExtractor:
    def __init__(self):
        pass

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


# Example usage
extractor = DataExtractor()

# Extract data from CSV file
csv_data = extractor.extract_from_csv('data.csv')
print(csv_data)

# Extract data from API
api_url = 'https://api.example.com/data'
api_data = extractor.extract_from_api(api_url)
print(api_data)

# Extract data from S3 bucket
s3_bucket = 'my-bucket'
s3_file_key = 'data.txt'
s3_data = extractor.extract_from_s3(s3_bucket, s3_file_key)
print(s3_data)
