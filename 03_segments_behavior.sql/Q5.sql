-- Q5. Which customer segments have the highest churn rate?
-- Depending on columns:
--•	Age group
--•	Region
--•	Gender
--•	Payment Method

-- Age Groups and 
WITH base AS (
  SELECT
    CASE
      WHEN "Age" < 25 THEN '<25'
      WHEN "Age" < 35 THEN '25-34'
      WHEN "Age" < 45 THEN '35-44'
      WHEN "Age" < 55 THEN '45-54'
      ELSE '55+'
    END AS age_group,
    "Churned"
  FROM ecommerce_customer_churn_cleaned
)
SELECT
  age_group,
  COUNT(*) AS customers,
  SUM(CASE WHEN "Churned" = 1 THEN 1 ELSE 0 END) AS churned_customers,
  ROUND(SUM(CASE WHEN "Churned" = 1 THEN 1.0 ELSE 0.0 END) / COUNT(*), 4) AS churn_rate
FROM base
GROUP BY age_group
ORDER BY churn_rate DESC;

-- Gender Groups
SELECT
  "Gender",
  COUNT(*) AS customers,
  SUM(CASE WHEN "Churned" = 1 THEN 1 ELSE 0 END) AS churned_customers,
  ROUND(SUM(CASE WHEN "Churned" = 1 THEN 1.0 ELSE 0.0 END) / COUNT(*), 4) AS churn_rate
FROM ecommerce_customer_churn_cleaned
GROUP BY "Gender"
ORDER BY churn_rate DESC;

-- Region 
SELECT
  "Country",
  COUNT(*) AS customers,
  ROUND(AVG("Churned")::numeric, 4) AS churn_rate
FROM ecommerce_customer_churn_cleaned
GROUP BY "Country"
HAVING COUNT(*) >= 500
ORDER BY churn_rate DESC;

