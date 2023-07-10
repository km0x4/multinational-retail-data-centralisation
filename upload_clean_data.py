# Upload cleaned data to the "dim_users" table
table_name = "dim_users"
connector.upload_to_db(cleaned_data, table_name)