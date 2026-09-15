-- Roll-up: revenue by month (whole warehouse, one time-level up from a single day)
SELECT month, SUM(amount) AS revenue
FROM sales
GROUP BY month
ORDER BY month;
