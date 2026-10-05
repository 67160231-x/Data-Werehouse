-- Bonus A part 2: AOV per month
SELECT month,
       SUM(amount) AS revenue,
       COUNT(DISTINCT order_id) AS orders,
       ROUND(SUM(amount) * 1.0 / COUNT(DISTINCT order_id), 2) AS aov
FROM sales
GROUP BY month
ORDER BY month;
