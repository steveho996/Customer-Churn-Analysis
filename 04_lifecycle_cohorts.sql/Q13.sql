-- Q13. Which cohorts perform best / worst?
-- We compare signup cohorts (quarters) on churn rate and average LTV.

SELECT
    "Signup_Quarter" AS cohort,
    COUNT(*) AS cohort_size,
    ROUND(AVG("Churned")::numeric, 4) AS churn_rate,
    ROUND(AVG("Lifetime_Value")::numeric, 2) AS avg_ltv
FROM ecommerce_customer_churn_cleaned
GROUP BY "Signup_Quarter"
ORDER BY churn_rate ASC, avg_ltv DESC;
