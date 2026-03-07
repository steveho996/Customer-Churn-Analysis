-- Q18. Which customers to prioritize for retention?
-- Priority Score = High LTV + Low Engagement + High Risk Signals

WITH prioritized_customers AS (
    SELECT *,
        -- High LTV (higher score = higher value)
        NTILE(5) OVER (ORDER BY "Lifetime_Value") AS ltv_score,
        -- Low engagement (higher score = FEWER logins)
        5 - NTILE(5) OVER (ORDER BY "Login_Frequency") AS low_engagement_score,
        -- High risk (higher score = MORE cart abandonment)
        NTILE(5) OVER (ORDER BY "Cart_Abandonment_Rate") AS risk_score,
        -- Combined priority score (max 15 = highest priority)
        NTILE(5) OVER (ORDER BY "Lifetime_Value") + 
        (5 - NTILE(5) OVER (ORDER BY "Login_Frequency")) + 
        NTILE(5) OVER (ORDER BY "Cart_Abandonment_Rate") AS priority_score
    FROM ecommerce_customer_churn_cleaned
)
SELECT
    priority_score,
    COUNT(*) AS customers,
    ROUND(AVG("Churned")::numeric, 4) AS churn_rate,
    SUM(CASE WHEN "Churned" = 1 THEN 1 ELSE 0 END) AS churned_customers,
    ROUND(AVG("Lifetime_Value")::numeric, 0) AS avg_ltv,
    ROUND(SUM(CASE WHEN "Churned" = 1 THEN "Lifetime_Value" ELSE 0 END)::numeric, 0) AS revenue_at_risk
FROM prioritized_customers
GROUP BY priority_score
HAVING COUNT(*) >= 10  -- Filter out tiny groups
ORDER BY priority_score DESC;
