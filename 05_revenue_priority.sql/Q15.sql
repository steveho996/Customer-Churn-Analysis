-- Q15. How much revenue is lost due to churn?
-- We use Lifetime_Value as the revenue proxy.

WITH revenue_by_status AS (
    SELECT
        CASE WHEN "Churned" = 0 THEN 'Retained' ELSE 'Churned' END AS status,
        COUNT(*) AS customer_count,
        SUM("Lifetime_Value") AS total_revenue
    FROM ecommerce_customer_churn_cleaned
    GROUP BY status
),
grand_total AS (
    SELECT SUM(total_revenue) AS grand_revenue
    FROM revenue_by_status
)
SELECT
    r.status,
    r.customer_count,
    ROUND(r.total_revenue::numeric, 2) AS total_revenue,
    ROUND((r.total_revenue / g.grand_revenue * 100)::numeric, 2) AS percent_of_total_revenue
FROM revenue_by_status r
CROSS JOIN grand_total g
ORDER BY status;
