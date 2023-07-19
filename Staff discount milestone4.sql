--What is the staff discount?
SELECT
  COUNT(*) AS total_staff_numbers,
  country_code
FROM staff
GROUP BY country_code
ORDER BY total_staff_numbers DESC;
