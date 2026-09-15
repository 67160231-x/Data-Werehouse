-- Drill-down: adds detail inside the time dimension (month -> full_date) for September
SELECT full_date, SUM(amount) AS revenue
FROM sales
WHERE month = '2026-09'
GROUP BY full_date
ORDER BY full_date;
