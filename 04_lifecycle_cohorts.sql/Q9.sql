/* Q9. When do customers typically churn?
Distribution of churn by tenure bucket
First month vs later months
*/

SELECT 
    CASE 
        WHEN "Membership_Years" <= (1.0/12.0) THEN '1. First month'
        WHEN "Membership_Years" > (1.0/12.0) AND "Membership_Years" <= 1 THEN '2. Month 2 to 1 Year'
        WHEN "Membership_Years" > 1 AND "Membership_Years" <= 3 THEN '3. 1 to 3 Years'
        WHEN "Membership_Years" > 3 AND "Membership_Years" <= 5 THEN '4. 3 to 5 Years'
        WHEN "Membership_Years" > 5 THEN '5. 5+ Years'
    END AS tenure_when_churned,
    COUNT(*) AS total_churned_customers,
    ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER()), 2) AS percentage_of_all_churn
FROM ecommerce_customer_churn_cleaned
WHERE "Churned" = 1
GROUP BY 1
ORDER BY 1;
--Insight: Nobody churns in the first month (minimum tenure is 1.2 months). The vast majority of churn (~44.7%) happens between years 1 and 3, followed by years 3 to 5 (~24.7%).
