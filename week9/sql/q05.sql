-- Slice: fix category = 'Drink', view remaining dimension (product_name)
SELECT product_name, SUM(amount) AS revenue
FROM sales
WHERE category = 'Drink'
GROUP BY product_name
ORDER BY product_name;
