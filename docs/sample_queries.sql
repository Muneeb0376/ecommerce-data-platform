-- Run these manually after loading data (not auto-run by Docker: file name sorts after schema but is only SELECTs)
-- Daily revenue
SELECT order_date::date AS day, SUM(total_amount) AS revenue
FROM orders WHERE status IN ('paid','shipped','delivered')
GROUP BY 1 ORDER BY 1 DESC LIMIT 10;

-- Top 10 products by units sold
SELECT p.name, SUM(oi.quantity) AS units
FROM order_items oi JOIN products p USING (product_id)
GROUP BY p.name ORDER BY units DESC LIMIT 10;
