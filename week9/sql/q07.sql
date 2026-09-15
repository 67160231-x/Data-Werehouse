-- Row filter (which months to include) goes in WHERE;
-- group-level filter (threshold on the aggregate) goes in HAVING
SELECT month, SUM(amount) AS revenue
FROM sales
WHERE month IN ('2026-08', '2026-09')
GROUP BY month
HAVING SUM(amount) > 500
ORDER BY month;
