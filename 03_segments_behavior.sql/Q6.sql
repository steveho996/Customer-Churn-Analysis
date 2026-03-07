--Q6. Does engagement level affect churn?
--Example:
--•	Churn rate by login frequency
--•	Churn rate by activity / usage metrics
--•	Churn rate by number of transactions

--•	By login frequency
SELECT 
	CASE 
		WHEN "Login_Frequency" >= 0 AND "Login_Frequency" <= 15 THEN 'low'
		WHEN "Login_Frequency" > 15 AND "Login_Frequency" <= 30 THEN 'medium'
		WHEN "Login_Frequency" > 30 THEN 'high'
	END AS login_frequency_rank,
	SUM(CASE WHEN "Churned" = 1 THEN 1 ELSE 0 END) AS churned_customers,
    ROUND(SUM(CASE WHEN "Churned" = 1 THEN 1.0 ELSE 0.0 END) / COUNT(*), 4) AS churn_rate
FROM ecommerce_customer_churn_cleaned
GROUP BY login_frequency_rank
ORDER BY churn_rate

--•	By activity / usage metrics
SELECT 
	CASE 
		WHEN "Session_Duration_Avg" >= 0 AND "Session_Duration_Avg" <= 20 THEN 'low'
		WHEN "Session_Duration_Avg" > 20 AND "Session_Duration_Avg" <= 40 THEN 'medium'
		WHEN "Session_Duration_Avg" > 40 THEN 'high'
	END AS session_duration_rank,
	SUM(CASE WHEN "Churned" = 1 THEN 1 ELSE 0 END) AS churned_customers,
    ROUND(SUM(CASE WHEN "Churned" = 1 THEN 1.0 ELSE 0.0 END) / COUNT(*), 4) AS churn_rate
FROM ecommerce_customer_churn_cleaned
GROUP BY session_duration_rank
ORDER BY churn_rate DESC;
-- By Mobile App Usage
SELECT 
	CASE 
		WHEN "Mobile_App_Usage" >= 0 AND "Mobile_App_Usage" <= 10 THEN 'low'
		WHEN "Mobile_App_Usage" > 10 AND "Mobile_App_Usage" <= 20 THEN 'medium'
		WHEN "Mobile_App_Usage" > 20 THEN 'high'
	END AS mobile_usage_rank,
	SUM(CASE WHEN "Churned" = 1 THEN 1 ELSE 0 END) AS churned_customers,
    ROUND(SUM(CASE WHEN "Churned" = 1 THEN 1.0 ELSE 0.0 END) / COUNT(*), 4) AS churn_rate
FROM ecommerce_customer_churn_cleaned
GROUP BY mobile_usage_rank
ORDER BY churn_rate DESC;
-- By Transactions
SELECT 
	CASE 
		WHEN "Total_Purchases" >= 0 AND "Total_Purchases" <= 5 THEN 'low'
		WHEN "Total_Purchases" > 5 AND "Total_Purchases" <= 15 THEN 'medium'
		WHEN "Total_Purchases" > 15 THEN 'high'
	END AS transactions_rank,
	SUM(CASE WHEN "Churned" = 1 THEN 1 ELSE 0 END) AS churned_customers,
    ROUND(SUM(CASE WHEN "Churned" = 1 THEN 1.0 ELSE 0.0 END) / COUNT(*), 4) AS churn_rate
FROM ecommerce_customer_churn_cleaned
GROUP BY transactions_rank
ORDER BY churn_rate DESC;

