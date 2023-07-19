--How quickly is the company making sales?
--mix of payment and year also 
SELECT
  year,
  AVG(
    TIMESTAMP_DIFF(lead(sale_date, 1), sale_date, 'SECOND')
  ) AS actual_time_taken
FROM sales
GROUP BY year
ORDER BY year;
