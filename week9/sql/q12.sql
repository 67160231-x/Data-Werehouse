-- Province x month detail; summing all months per province reproduces q03's per-province total
SELECT province, month, SUM(amount) AS revenue
FROM sales
GROUP BY province, month
ORDER BY province, month;
