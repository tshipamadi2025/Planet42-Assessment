-- 1. Total transactions per product category
SELECT product_category, COUNT(*) AS total_transactions
FROM transactions
GROUP BY product_category;

-- 2. Top 5 accounts by total transaction value
SELECT customer_id, SUM(transaction_amount) AS total_transaction_value
FROM transactions
GROUP BY customer_id
ORDER BY total_transaction_value DESC
LIMIT 5;

-- 3. Monthly spend trends over the past year
SELECT DATE_FORMAT(transaction_date, '%Y-%m') AS month, SUM(transaction_amount) as total_transaction_value
FROM transactions
GROUP BY DATE_FORMAT(transaction_date, '%Y-%m')