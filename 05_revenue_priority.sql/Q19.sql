-- Q19. Early warning signals that predict churn risk
-- These 4 signals have 35-50%+ churn rates (vs overall 28.9%)

WITH risk_signals AS (
    SELECT *,
        -- Low engagement (bottom 20% login frequency)
        CASE WHEN "Login_Frequency" <= PERCENTILE_CONT(0.2) OVER() THEN 1 ELSE 0 END AS low_engagement,
        -- High cart abandonment (top 20%)
        CASE WHEN "Cart_Abandonment_Rate" >= PERCENTILE_CONT(0.8) OVER() THEN 1 ELSE 0 END AS high_abandonment,
        -- High service calls (top 20%)
        CASE WHEN "Customer_Service_Calls" >= PERCENTILE_CONT(0.8) OVER() THEN 1 ELSE 0 END AS high_service_calls,
        -- Dormant (no purchase in 90+ days)
        CASE WHEN "Days_Since_Last_Purchase" > 90 THEN 1 ELSE 0 END AS dormant
    FROM ecommerce_customer_churn_cleaned
)
SELECT
    low_engagement + high_abandonment + high_service_calls + dormant AS warning_signals_count,
    COUNT(*) AS customers,
    ROUND(AVG("Churned")::numeric, 4) AS predicted_churn_rate,
    ROUND(AVG("Lifetime_Value")::numeric, 0) AS avg_ltv
FROM risk_signals
GROUP BY warning_signals_count
HAVING COUNT(*) >= 50
ORDER BY warning_signals_count DESC;
