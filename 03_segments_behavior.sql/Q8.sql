--Q8. Which behavioral patterns are most associated with churn?
--•	High Cart Abandonment
--•	Dormancy 
--•	Customer Service Friction

--1.High Cart Abandonment
SELECT 
	CASE 
		WHEN "Cart_Abandonment_Rate" >= 0 AND "Cart_Abandonment_Rate" <= 30 THEN 'Low Abandonment (<30%)'
		WHEN "Cart_Abandonment_Rate" > 30 AND "Cart_Abandonment_Rate" <= 70 THEN 'Medium Abandonment (30-70%)'
		WHEN "Cart_Abandonment_Rate" > 70 THEN 'High Abandonment (>70%)'
	END AS abandonment_behavior,
	COUNT(*) AS customers,
    ROUND(SUM(CASE WHEN "Churned" = 1 THEN 1.0 ELSE 0.0 END) / COUNT(*), 4) AS churn_rate
FROM ecommerce_customer_churn_cleaned
GROUP BY 1
ORDER BY churn_rate DESC;
--Customers who abandon their carts frequently (>70% of the time) have a massive ~54.8% churn rate, compared to just ~15.5% for those who rarely abandon carts.

-- 2. Dormancy 
SELECT 
	CASE 
		WHEN "Days_Since_Last_Purchase" >= 0 AND "Days_Since_Last_Purchase" <= 30 THEN 'Recent (0-30 days)'
		WHEN "Days_Since_Last_Purchase" > 30 AND "Days_Since_Last_Purchase" <= 90 THEN 'At Risk (31-90 days)'
		WHEN "Days_Since_Last_Purchase" > 90 THEN 'Dormant (>90 days)'
	END AS purchase_recency,
	COUNT(*) AS customers,
    ROUND(SUM(CASE WHEN "Churned" = 1 THEN 1.0 ELSE 0.0 END) / COUNT(*), 4) AS churn_rate
FROM ecommerce_customer_churn_cleaned
GROUP BY 1
ORDER BY churn_rate DESC;
--Customers who haven't purchased in over 90 days are almost twice as likely to churn (~49.4%) as recent purchasers (~25.3%).

--3. Customer Service Friction
SELECT 
	CASE 
		WHEN "Customer_Service_Calls" >= 0 AND "Customer_Service_Calls" <= 1 THEN 'Low (0-1 calls)'
		WHEN "Customer_Service_Calls" > 1 AND "Customer_Service_Calls" <= 3 THEN 'Medium (2-3 calls)'
		WHEN "Customer_Service_Calls" > 3 THEN 'High (4+ calls)'
	END AS friction_level,
	COUNT(*) AS customers,
    ROUND(SUM(CASE WHEN "Churned" = 1 THEN 1.0 ELSE 0.0 END) / COUNT(*), 4) AS churn_rate
FROM ecommerce_customer_churn_cleaned
GROUP BY 1
ORDER BY churn_rate DESC;
--Customers who contact support 4 or more times have a churn rate of ~32.2%, compared to just ~11.5% for those who rarely contact support (0-1 times).
