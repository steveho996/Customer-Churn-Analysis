-- Q20. Best ROI segments for limited retention marketing budget
-- ROI = Revenue saved per prevented churn * Segment churn rate * Segment size

WITH ltv_segments AS (
    SELECT *,
        NTILE(4) OVER (ORDER BY "Lifetime_Value") AS ltv_quartile
    FROM ecommerce_customer_churn_cleaned
),
segment_analysis AS (
    SELECT
        CASE ltv_quartile
            WHEN 1 THEN 'Q1 (Lowest 25%)'
            WHEN 2 THEN 'Q2 (25-50%)'
            WHEN 3 THEN 'Q3 (50-75%)'
            WHEN 4 THEN 'Q4 (Highest 25%)'
        END AS segment,
        COUNT(*) AS customers,
        ROUND(AVG("Churned")::numeric, 4) AS churn_rate,
        SUM(CASE WHEN "Churned" = 1 THEN 1 ELSE 0 END) AS churned_count,
        ROUND(AVG("Lifetime_Value")::numeric, 0) AS avg_ltv_per_customer
    FROM ltv_segments
    GROUP BY ltv_quartile
)
SELECT
    segment,
    customers,
    churn_rate,
    churned_count,
    avg_ltv_per_customer,
    -- ROI: Revenue saved if you prevent ONE churn in this segment
    ROUND((avg_ltv_per_customer * churn_rate)::numeric, 0) AS roi_per_prevented_churn,
    ROUND((churned_count * 100.0 / SUM(churned_count) OVER())::numeric, 1) AS pct_of_total_churn
FROM segment_analysis
ORDER BY roi_per_prevented_churn DESC;
