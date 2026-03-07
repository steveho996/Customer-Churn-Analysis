-- Q14. How does behavior differ between retained vs churned cohorts?

SELECT
    CASE WHEN "Churned" = 0 THEN 'Retained' ELSE 'Churned' END AS cohort,
    ROUND(AVG("Login_Frequency")::numeric, 3)              AS avg_login_frequency,
    ROUND(AVG("Session_Duration_Avg")::numeric, 3)         AS avg_session_duration,
    ROUND(AVG("Pages_Per_Session")::numeric, 3)            AS avg_pages_per_session,
    ROUND(AVG("Total_Purchases")::numeric, 3)              AS avg_total_purchases,
    ROUND(AVG("Average_Order_Value")::numeric, 3)          AS avg_order_value,
    ROUND(AVG("Lifetime_Value")::numeric, 3)               AS avg_lifetime_value,
    ROUND(AVG("Cart_Abandonment_Rate")::numeric, 3)        AS avg_cart_abandonment,
    ROUND(AVG("Discount_Usage_Rate")::numeric, 3)          AS avg_discount_usage,
    ROUND(AVG("Returns_Rate")::numeric, 3)                 AS avg_returns_rate,
    ROUND(AVG("Email_Open_Rate")::numeric, 3)              AS avg_email_open_rate,
    ROUND(AVG("Customer_Service_Calls")::numeric, 3)       AS avg_service_calls,
    ROUND(AVG("Mobile_App_Usage")::numeric, 3)             AS avg_mobile_usage
FROM ecommerce_customer_churn_cleaned
GROUP BY cohort;
