SELECT COUNT(*)                AS line_count,
       COUNT(DISTINCT order_id) AS order_count,
       SUM(quantity)            AS units,
       SUM(amount)              AS revenue
FROM sales;
