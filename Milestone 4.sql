--How many stores does the buisness have in each country
SELECT country, COUNT(*) AS total_no_stores
FROM stores
GROUP BY country
ORDER BY total_no_stores DESC;


--Which location have the most stores?
SELECT locality, COUNT(*) AS total_no_stores
FROM stores
GROUP BY locality
ORDER BY total_no_stores DESC;

--Which months produce the average highest cost of sales typically? 
SELECT month, SUM (total_sales) AS total_sales
FROM SALES
GROUP BY month
ORDER BY total_sales DESC;

--how many sales are coming from online?
SELECT
  COUNT(*) AS numbers_of_sales,
  SUM(quantity) AS product_quantity_count,
  location
FROM sales
GROUP BY location;

--What percentage of sales come through each store? 
SELECT
  store_type,
  SUM(total_sales) AS total_sales,
  ROUND(100 * SUM(total_sales) / SUM(total_sales) OVER(), 2) AS percentage_total
FROM sales
GROUP BY store_type
ORDER BY percentage_total DESC;

--which month in each year produced the highest cost of sales ?
SELECT
  year,
  month,
  SUM(total_sales) AS total_sales
FROM sales
GROUP BY year, month
ORDER BY total_sales DESC;

--Which german store type is selling fast? 
SELECT
  SUM(total_sales) AS total_sales,
  store_type,
  country_code
FROM sales
WHERE country_code = 'DE'
GROUP BY store_type
ORDER BY total_sales DESC;
