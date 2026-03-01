SELECT 'transactions' AS table_name, COUNT(*) AS row_count FROM transactions
UNION ALL
SELECT 'transaction_features', COUNT(*) FROM transaction_features
UNION ALL
SELECT 'fraud_labels', COUNT(*) FROM fraud_labels;

SELECT
  SUM(is_fraud) AS fraud_count,
  AVG(is_fraud::float) AS fraud_rate
FROM fraud_labels;

SELECT
  MIN(amount) AS amount_min,
  MAX(amount) AS amount_max,
  AVG(amount) AS amount_avg
FROM transactions;