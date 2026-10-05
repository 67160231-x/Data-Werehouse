-- Bonus A part 3: overall AOV across the whole range, for comparison
SELECT SUM(amount) * 1.0 / COUNT(DISTINCT order_id) AS overall_aov
FROM sales;
