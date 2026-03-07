-- Q21. Monthly KPIs to control churn
-- Top 5 most predictive metrics (highest correlation with churn)

WITH monthly_metrics AS (
    SELECT
        -- Engagement KPIs (negative correlation = higher = better retention)
        ROUND(AVG("Login_Frequency")::numeric, 2) AS avg_login_frequency,
        ROUND(PERCENTILE_CONT(0.2) WITHIN GROUP (ORDER BY "Login_Frequency")::numeric, 2) AS login_20th_pctile,
        
        ROUND(AVG("Session_Duration_Avg")::numeric, 2) AS avg_session_duration,
        ROUND(PERCENTILE_CONT(0.2) WITHIN GROUP (ORDER BY "Session_Duration_Avg")::numeric, 2) AS session_20th_pctile,
        
        -- Risk signals (positive correlation = higher = worse retention)
        ROUND(AVG("Cart_Abandonment_Rate")::numeric, 2) AS avg_cart_abandonment,
        ROUND(PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY "Cart_Abandonment_Rate")::numeric, 2) AS cart_80th_pctile,
        
        ROUND(AVG("Customer_Service_Calls")::numeric, 2) AS avg_service_calls,
        ROUND(PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY "Customer_Service_Calls")::numeric, 2) AS service_80th_pctile,
        
        -- Overall churn
        ROUND(AVG("Churned")::numeric, 4) AS overall_churn_rate
    FROM ecommerce_customer_churn_cleaned
)
SELECT
    *,
    -- ALERTS (red flags)
    CASE WHEN login_20th_pctile < 8.0 THEN '⚠️  CRITICAL: Engagement dropping' ELSE '✅ OK' END AS engagement_alert,
    CASE WHEN cart_80th_pctile > 70.0 THEN '⚠️  CRITICAL: Abandonment spiking' ELSE '✅ OK' END AS abandonment_alert,
    CASE WHEN service_80th_pctile > 8.0 THEN '⚠️  CRITICAL: Service calls spiking' ELSE '✅ OK' END AS service_alert
FROM monthly_metrics;
