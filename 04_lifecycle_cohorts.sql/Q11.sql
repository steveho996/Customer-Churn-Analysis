/* Q11. Is there a “danger zone” period where churn spikes?
*/

WITH lifecycle_cohorts AS (
    SELECT 
        CASE 
            WHEN "Membership_Years" <= 0.5 THEN '1. 0-6 Months'
            WHEN "Membership_Years" > 0.5 AND "Membership_Years" <= 1.0 THEN '2. 6-12 Months'
            WHEN "Membership_Years" > 1.0 AND "Membership_Years" <= 1.5 THEN '3. 1-1.5 Years'
            WHEN "Membership_Years" > 1.5 AND "Membership_Years" <= 2.0 THEN '4. 1.5-2 Years'
            WHEN "Membership_Years" > 2.0 AND "Membership_Years" <= 2.5 THEN '5. 2-2.5 Years'
            WHEN "Membership_Years" > 2.5 AND "Membership_Years" <= 3.0 THEN '6. 2.5-3 Years'
            WHEN "Membership_Years" > 3.0 AND "Membership_Years" <= 4.0 THEN '7. 3-4 Years'
            WHEN "Membership_Years" > 4.0 AND "Membership_Years" <= 5.0 THEN '8. 4-5 Years'
            WHEN "Membership_Years" > 5.0 THEN '9. 5+ Years'
        END AS lifecycle_stage,
        "Churned"
    FROM ecommerce_customer_churn_cleaned
)
SELECT 
    lifecycle_stage,
    COUNT(*) AS total_customers_in_stage,
    SUM(CASE WHEN "Churned" = 1 THEN 1 ELSE 0 END) AS churned_customers,
    ROUND(SUM(CASE WHEN "Churned" = 1 THEN 1.0 ELSE 0.0 END) / COUNT(*), 4) AS stage_churn_rate,
    ROUND((SUM(CASE WHEN "Churned" = 1 THEN 1.0 ELSE 0.0 END) / SUM(SUM(CASE WHEN "Churned" = 1 THEN 1.0 ELSE 0.0 END)) OVER()) * 100, 2) AS pct_of_total_company_churn
FROM lifecycle_cohorts
GROUP BY 1
ORDER BY 1;

/* Insight:
There is no dramatic spike in the *rate* of churn at any specific point (it hovers at ~28.5% - 29.3% for all cohorts). 
However, the *highest volume* of churn happens in the 1 to 2.5-year "danger zone" (accounting for ~35% of all churned customers). 
This suggests the product successfully onboards users but struggles with mid-term retention.
*/