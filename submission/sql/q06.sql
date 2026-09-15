-- Dice: sub-cube filtered on two dimensions (category, region), kept as two group-by dims
SELECT category, region, SUM(amount) AS revenue
FROM sales
WHERE category IN ('Drink', 'Snack') AND region = 'East'
GROUP BY category, region
ORDER BY category, region;
