-- Bonus A part 1: top 3 stores by total revenue
SELECT store_name, SUM(amount) AS revenue
FROM sales
GROUP BY store_name
ORDER BY revenue DESC
LIMIT 3;
