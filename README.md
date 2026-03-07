# Customer-churn-analysis-to-support-retention-decisions
Customer churn analysis to support retention decisions
This project is an end-to-end **customer churn analysis** for an ecommerce business, built entirely in **PostgreSQL** and designed to showcase practical, hiring-ready analytics skills.

It answers a structured set of real-world business questions (**Q1–Q21**) about churn, retention, customer behavior, and revenue risk, and turns them into **clear SQL**, **concrete insights**, and **actionable recommendations**.

---

## 🎯 Project Overview

**Goal:**  
Help an ecommerce company understand **why customers churn**, **which customers are at risk**, and **where to focus retention budget**.

**Data:**  
Single fact-style table: `ecommerce_customer_churn_cleaned` with ~50k customers and features such as:

| Category | Columns |
|----------|---------|
| **Demographics** | `Age`, `Gender`, `Country`, `City` |
| **Tenure** | `Membership_Years`, `Signup_Quarter` |
| **Engagement** | `Login_Frequency`, `Session_Duration_Avg`, `Pages_Per_Session`, `Mobile_App_Usage` |
| **Commerce** | `Total_Purchases`, `Average_Order_Value`, `Cart_Abandonment_Rate`, `Discount_Usage_Rate`, `Returns_Rate`, `Days_Since_Last_Purchase` |
| **Communication** | `Email_Open_Rate`, `Customer_Service_Calls`, `Product_Reviews_Written`, `Social_Media_Engagement_Score` |
| **Value & Risk** | `Lifetime_Value`, `Credit_Balance`, `Churned` (0/1) |

**Tech Stack:**
PostgreSQL (pgAdmin 4) - SQL (CTEs, Window Functions, Cohorts) - VS Code - Git/GitHub

## 📊 Key Business Questions & Insights

### **Q1–Q4: Basic Churn & Tenure**
✅ Overall churn rate: ~29%
✅ Average tenure: ~3 years (identical for active/churned)
✅ Churn timing: 98.7% happens after 90+ days (0% in first 30 days)
✅ Retention stable across tenure buckets (~28-29%)

**Takeaway:** Churn is a **mid/long-term lifecycle problem**, not onboarding failure.

### **Q5–Q7: Who Churns? (Segments & Spend)**
🔴 Highest churn segments:

Age <25: 48.4% churn (vs overall 29%)

High payment diversity (5 methods): 30.3%

Australia/Canada top countries by churn rate

💰 Revenue impact:

Low-spend + very high-spend both ~35% churn

Churned customers = 28.6% of all historical revenue (~$20.6M)

**Takeaway:** **Young customers + high payment diversity** are highest-risk segments.

### **Q8–Q11: Behavioral Drivers & "Danger Zones"**
📉 Retained vs Churned behavior:
Retained: +3.5 logins, +5min sessions, +1.8 pages, +4 mobile usage
Churned: +10% cart abandonment, +1.7 service calls, +0.7% returns

⚠️ Early warning signals (40-55% churn):

High cart abandonment (>70%)

Dormant (>90 days no purchase)

High service calls (4+)

Low login frequency (bottom 20%)

⏰ Lifecycle danger zone: Years 1-3 (35% of all churn volume)

**Takeaway:** **Engagement + friction** are the biggest churn drivers.

### **Q12–Q14: Cohort & Retention Analysis**
📈 Quarterly cohort retention (all similar):
Q1-Q4: 96% → 89% → 83% (Yr1→Yr2→Yr3)
Cohort quality stable across quarters

text
**Takeaway:** Acquisition timing doesn't drive retention; **behavior does**.

### **Q15–Q18: Revenue Risk & Priority Targets**
💸 Revenue lost: $20.6M (28.6% of total LTV)
💰 ARPU: Active $1447 vs Churned $1425

🎯 Priority retention targets (Top 10% highest-risk):

49.6% churn rate

$1,664 avg LTV

$9.9M total revenue at risk

🏆 Best ROI: Q4 highest-LTV segment ($2,691 saved per prevented churn)

**Takeaway:** **High-LTV + low engagement** = highest ROI retention targets.

### **Q19–Q21: Early Warning & Monthly Monitoring**
📊 Top 5 monthly KPIs (by churn correlation):

Customer_Service_Calls (+0.29)

Cart_Abandonment_Rate (+0.28)

Pages_Per_Session (-0.22)

Session_Duration_Avg (-0.22)

Email_Open_Rate (-0.22)

**Takeaway:** Built **operational thresholds** for monthly churn dashboard.

---

## 🛠 Technical Skills Demonstrated

| Skill | Examples Used |
|-------|---------------|
| **Segmentation** | Age groups, LTV quartiles, tenure buckets, payment diversity |
| **CTEs** | Cohort analysis, risk scoring, lifecycle transformations |
| **Window Functions** | `NTILE`, `PERCENTILE_CONT`, `SUM() OVER()` for % calculations |
| **Cohort Analysis** | Quarterly signup retention curves (Yr1→Yr2→Yr3) |
| **Business Logic** | Priority scoring (LTV + engagement + risk), ROI per prevented churn |

---
