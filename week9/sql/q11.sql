-- Compare row counts / totals before JOIN (fact_sales) and after JOIN (sales view)
SELECT (SELECT COUNT(*) FROM fact_sales)                AS fact_rows,
       (SELECT COUNT(*) FROM sales)                     AS view_rows,
       (SELECT SUM(quantity * unit_price) FROM fact_sales) AS fact_revenue,
       (SELECT SUM(amount) FROM sales)                  AS view_revenue;
