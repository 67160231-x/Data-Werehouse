-- UNION ALL: monthly revenue plus an ALL grand-total row
SELECT month, SUM(amount) AS revenue
FROM sales
GROUP BY month
UNION ALL
SELECT 'ALL' AS month, SUM(amount) AS revenue
FROM sales
ORDER BY month;
