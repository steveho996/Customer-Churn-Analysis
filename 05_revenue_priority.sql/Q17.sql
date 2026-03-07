-- Q17. Which customer groups represent the biggest revenue risk?
-- We identify "high LTV but high churn" segments using LTV quartiles.

WITH ltv_quartiles AS (
    SELECT *,
        NTILE(4) OVER (ORDER BY "Lifetime_Value") AS ltv_quartile
    FROM ecommerce_customer_churn_cleaned
),
risk_analysis AS (
    SELECT
        CASE ltv_quartile
            WHEN 1 THEN 'Q1 (Lowest 25%)'
            WHEN 2 THEN 'Q2 (25-50%)'
            WHEN 3 THEN 'Q3 (50-75%)'
            WHEN 4 THEN 'Q4 (Highest 25%)'
        END AS ltv_segment,
        COUNT(*) AS customers,
        ROUND(AVG("Churned")::numeric, 4) AS churn_rate,
        SUM(CASE WHEN "Churned" = 1 THEN 1 ELSE 0 END) AS churned_customers,
        ROUND(AVG("Lifetime_Value")::numeric, 0) AS avg_ltv,
        ROUND(SUM(CASE WHEN "Churned" = 1 THEN "Lifetime_Value" ELSE 0 END)::numeric, 0) AS revenue_lost
    FROM ltv_quartiles
    GROUP BY ltv_quartile
)
SELECT
    ltv_segment,
    customers,
    churn_rate,
    churned_customers,
    avg_ltv,
    revenue_lost,
    ROUND((churned_customers * 100.0 / SUM(churned_customers) OVER()), 1) AS pct_of_total_churn
FROM risk_analysis
ORDER BY revenue_lost DESC;
