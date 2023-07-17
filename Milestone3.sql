--cast the coloumns of the dim_user_table to the correct data types 
ALTER TABLE dim_user_table
ALTER COLUMN first_name VARCHAR(255) NOT NULL,
ALTER COLUMN last_name   VARCHAR(255) NOT NULL,
ALTER COLUMN date_of_birth DATE NOT NULL,
ALTER COLUMN country_code VARCHAR(2) NOT NULL,
ALTER COLUMN user_uuid    UUID NOT NULL,
ALTER COLUMN join_date    DATE NOT NULL;


--- Update the dim store table 
ALTER TABLE store_details_table
ADD COLUMN latitude_merged FLOAT;

UPDATE store_details_table
SET latitude_merged = latitude
WHERE latitude IS NOT NULL;

UPDATE store_details_table
SET latitude_merged = longitude
WHERE longitude IS NOT NULL AND latitude IS NULL;

ALTER TABLE store_details_table
DROP COLUMN latitude,
DROP COLUMN longitude;

ALTER TABLE store_details_table
ALTER COLUMN latitude_merged FLOAT NOT NULL;

ALTER TABLE store_details_table
ALTER COLUMN locality VARCHAR(255) NOT NULL;

ALTER TABLE store_details_table
ALTER COLUMN store_code VARCHAR(255) NOT NULL;

ALTER TABLE store_details_table
ALTER COLUMN staff_numbers SMALLINT NOT NULL;

ALTER TABLE store_details_table
ALTER COLUMN opening_date DATE NOT NULL;

ALTER TABLE store_details_table
ALTER COLUMN store_type VARCHAR(255) NULL;

ALTER TABLE store_details_table
ALTER COLUMN country_code VARCHAR(255) NOT NULL;

ALTER TABLE store_details_table
ALTER COLUMN continent VARCHAR(255) NOT NULL;

UPDATE store_details_table
SET latitude_merged = 'N/A'
WHERE latitude_merged IS NULL;

--make changes to the dim_products for the delivery team 
ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(255) NULL;

UPDATE dim_products
SET weight_class =
CASE
WHEN weight < 2 THEN 'Light'
WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
ELSE 'Truck_Required'
END;

--update the dim_products tables with the required data types
ALTER TABLE dim_products
ALTER COLUMN product_price FLOAT,
ALTER COLUMN weight FLOAT,
ALTER COLUMN EAN VARCHAR(255),
ALTER COLUMN product_code VARCHAR(255),
ALTER COLUMN date_added DATE,
ALTER COLUMN uuid UUID,
ALTER COLUMN still_available BOOL,
ALTER COLUMN weight_class VARCHAR(255);

UPDATE dim_products
SET still_available = 1
WHERE still_available = 'Yes';

UPDATE dim_products
SET still_available = 0
WHERE still_available = 'No';

-- Update with the correct time(here I remember it's to do with date time extract )
ALTER TABLE dim_date_times 
ALTER COLUMN  month  varchain (255) 
ALTER COLUMN year VARCHAR(255),
ALTER COLUMN day VARCHAR(255),
ALTER COLUMN time_period VARCHAR(255),
ALTER COLUMN date_uuid UUID;

--Update the dim_card_details (CARD NUMBER CAN HAVE 16 SO VARCHAR IS 16 AND EXPIRE DATE CAN BE 5 )
ALTER TABLE dim_card_tables
ALTER TABLE dim_card_details
ALTER COLUMN card_number VARCHAR(16),
ALTER COLUMN expiry_date VARCHAR(5),
ALTER COLUMN date_payment_confirmed DATE;

-- Create PK in dimension table 
ALTER TABLE dim_customers
ADD CONSTRAINT pk_dim_customers PRIMARY KEY (customer_id);

ALTER TABLE dim_products
ADD CONSTRAINT pk_dim_products PRIMARY KEY (product_id);

ALTER TABLE dim_date_times
ADD CONSTRAINT pk_dim_date_times PRIMARY KEY (date_uuid);

ALTER TABLE dim_card_details
ADD CONSTRAINT pk_dim_card_details PRIMARY KEY (card_number);
