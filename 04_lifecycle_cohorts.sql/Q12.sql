/*
QUARTERLY COHORT RETENTION ANALYSIS
Shows retention rates for customers acquired in each quarter, 
tracking survival to Year 1, Year 2, and Year 3.
*/

WITH quarterly_cohorts AS (
    SELECT 
        "Signup_Quarter",
        COUNT(*) AS cohort_size,
        -- Survived Year 1: Either still active OR churned *after* 1 year
        SUM(CASE WHEN "Churned" = 0 OR "Membership_Years" >= 1.0 THEN 1 ELSE 0 END) AS retained_year_1,
        -- Survived Year 2
        SUM(CASE WHEN "Churned" = 0 OR "Membership_Years" >= 2.0 THEN 1 ELSE 0 END) AS retained_year_2,
        -- Survived Year 3
        SUM(CASE WHEN "Churned" = 0 OR "Membership_Years" >= 3.0 THEN 1 ELSE 0 END) AS retained_year_3
    FROM ecommerce_customer_churn_cleaned
    GROUP BY "Signup_Quarter"
)
SELECT 
    "Signup_Quarter" AS cohort,
    cohort_size,
    ROUND((retained_year_1 * 100.0 / cohort_size), 1) AS "Yr1_Retention_%",
    ROUND((retained_year_2 * 100.0 / cohort_size), 1) AS "Yr2_Retention_%",
    ROUND((retained_year_3 * 100.0 / cohort_size), 1) AS "Yr3_Retention_%"
FROM quarterly_cohorts
ORDER BY "Signup_Quarter";

/*The retention by signup cohort is very stable: all quarters keep about 96% of customers to year 1, ~89% to year 2, and ~83% to year 3, so acquisition timing (Q1–Q4) does not materially change long‑term retention. /*
