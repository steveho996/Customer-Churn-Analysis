-- Q16. Average Revenue Per User (ARPU) by status (Active vs Churned)
-- Using Lifetime_Value as the revenue proxy.

SELECT
    CASE WHEN "Churned" = 0 THEN 'Active' ELSE 'Churned' END AS status,
    ROUND(AVG("Lifetime_Value")::numeric, 2) AS arpu
FROM ecommerce_customer_churn_cleaned
GROUP BY status;
