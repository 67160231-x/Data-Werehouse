-- Adds the location dimension: revenue by province
SELECT province, SUM(amount) AS revenue
FROM sales
GROUP BY province
ORDER BY province;
