SELECT
  "Churned",
  COUNT(*) AS customers,
  ROUND(AVG("Membership_Years")::numeric, 4) AS avg_tenure_years
FROM ecommerce_customer_churn_cleaned
GROUP BY "Churned"
ORDER BY "Churned";



	





