/* Q10. What percentage of churn happens early vs late?
Example buckets:
•	0–30 days
•	31–90 days
•	90+ days
*/

SELECT 
    CASE 
        WHEN "Membership_Years" <= (30.0/365.0) THEN '1. Early (0-30 days)'
        WHEN "Membership_Years" > (30.0/365.0) AND "Membership_Years" <= (90.0/365.0) THEN '2. Mid (31-90 days)'
        WHEN "Membership_Years" > (90.0/365.0) THEN '3. Late (90+ days)'
    END AS churn_timing,
    COUNT(*) AS total_churned_customers,
    ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER()), 2) AS percentage_of_all_churn
FROM ecommerce_customer_churn_cleaned
WHERE "Churned" = 1
GROUP BY 1
ORDER BY 1;

--Churn in this dataset is almost entirely a "late" problem. 0% of churned customers leave in the first 30 days, 1.3% leave in days 31-90, and 98.7% leave after 90 days.