SELECT
    SUM(CASE WHEN "Churned" = 1 THEN 1 ELSE 0 END) AS churn_amount,
    SUM(CASE WHEN "Churned" = 0 THEN 1 ELSE 0 END) AS active_amount
FROM ecommerce_customer_churn_cleaned;

