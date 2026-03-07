SELECT
  "Signup_Quarter",
  CASE
    WHEN "Membership_Years" >= 0 AND "Membership_Years" < 1 THEN 'short'
    WHEN "Membership_Years" >= 1 AND "Membership_Years" < 3 THEN 'medium'
    WHEN "Membership_Years" >= 3 THEN 'long'
  END AS tenure_bucket,
  COUNT(*) AS customers,
  SUM(CASE WHEN "Churned" = 1 THEN 1 ELSE 0 END) AS churn_amount,
  ROUND(
    SUM(CASE WHEN "Churned" = 1 THEN 1.0 ELSE 0.0 END) / COUNT(*),
    4
  ) AS churn_rate
FROM ecommerce_customer_churn_cleaned
GROUP BY "Signup_Quarter", tenure_bucket
ORDER BY "Signup_Quarter", tenure_bucket;


	





