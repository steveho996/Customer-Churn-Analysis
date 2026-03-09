import pandas as pd
import numpy as np

# Load the original dataset
df_original = pd.read_csv(r'D:\Datapot class\Portfolio Dataset\Ecommerce Customer Churn\ecommerce_customer_churn_dataset.csv')
df_clean = df_original.copy()

print("="*100)
print("E-COMMERCE CUSTOMER CHURN DATA CLEANING REPORT")
print("="*100)
print(f"📊 Original Dataset: {df_original.shape[0]:,} rows × {df_original.shape[1]} columns")
print(f"📊 Missing values (original): {df_original.isnull().sum().sum():,}")
print("-"*100)

# 1. CLEAN AGE COLUMN
print("\n1️⃣ CLEANING AGE COLUMN")
print(f"   Original: Missing={df_clean['Age'].isnull().sum()}, >100={((df_original['Age'] > 100).sum())}, <18={((df_original['Age'] < 18).sum())}")

# Cap age at reasonable range (18-100)
outliers_high = (df_clean['Age'] > 100).sum()
outliers_low = (df_clean['Age'] < 18).sum()
df_clean.loc[df_clean['Age'] > 100, 'Age'] = np.nan
df_clean.loc[df_clean['Age'] < 18, 'Age'] = np.nan

age_median = df_clean['Age'].median()
missing_age = df_clean['Age'].isnull().sum()
df_clean['Age'] = df_clean['Age'].fillna(age_median)
df_clean['Age'] = df_clean['Age'].astype(int)

print(f"   ✅ Fixed: {outliers_high} high outliers + {outliers_low} low outliers → median {age_median:.0f}")
print(f"   Age range now: {df_clean['Age'].min()} - {df_clean['Age'].max()}")

# 2. CLEAN NEGATIVE TOTAL PURCHASES
print("\n2️⃣ CLEANING TOTAL_PURCHASES")
neg_purchases = (df_clean['Total_Purchases'] < 0).sum()
df_clean.loc[df_clean['Total_Purchases'] < 0, 'Total_Purchases'] = 0
print(f"   ✅ Fixed: {neg_purchases} negative values → 0")
print(f"   Purchases range now: {df_clean['Total_Purchases'].min()} - {df_clean['Total_Purchases'].max()}")

# 3. CLEAN PERCENTAGE COLUMNS
print("\n3️⃣ CLEANING PERCENTAGE COLUMNS")
percent_cols = ['Cart_Abandonment_Rate', 'Discount_Usage_Rate']
for col in percent_cols:
    if col in df_clean.columns:
        over_100 = (df_clean[col] > 100).sum()
        if over_100 > 0:
            df_clean.loc[df_clean[col] > 100, col] = 100
            print(f"   ✅ {col}: Capped {over_100} values >100%")

# 4. HANDLE MISSING VALUES (Numerical)
print("\n4️⃣ HANDLING MISSING VALUES")
numerical_cols = [
    'Membership_Years', 'Login_Frequency', 'Session_Duration_Avg', 
    'Pages_Per_Session', 'Wishlist_Items', 'Days_Since_Last_Purchase',
    'Discount_Usage_Rate', 'Returns_Rate', 'Email_Open_Rate',
    'Customer_Service_Calls', 'Product_Reviews_Written',
    'Social_Media_Engagement_Score', 'Mobile_App_Usage',
    'Payment_Method_Diversity', 'Credit_Balance'
]

total_missing_fixed = 0
for col in numerical_cols:
    if col in df_clean.columns:
        missing_count = df_clean[col].isnull().sum()
        if missing_count > 0:
            median_val = df_clean[col].median()
            df_clean[col] = df_clean[col].fillna(median_val)
            total_missing_fixed += missing_count
            print(f"   ✅ {col}: Filled {missing_count:,} values (median={median_val:.2f})")

print(f"   📈 Total missing values fixed: {total_missing_fixed:,}")

# 5. DATA TYPE OPTIMIZATION
print("\n5️⃣ OPTIMIZING DATA TYPES")
int_cols = ['Churned', 'Customer_Service_Calls', 'Product_Reviews_Written', 'Wishlist_Items']
for col in int_cols:
    if col in df_clean.columns and df_clean[col].isnull().sum() == 0:
        df_clean[col] = df_clean[col].astype(int)

# 6. POSTGRESQL COMPATIBILITY
print("\n6️⃣ POSTGRESQL TYPE FIXING")
postgres_int_cols = ['Age', 'Login_Frequency', 'Days_Since_Last_Purchase', 'Credit_Balance']
for col in postgres_int_cols:
    if col in df_clean.columns:
        df_clean[col] = df_clean[col].round(0).astype(int)

postgres_float_cols = [
    'Membership_Years', 'Session_Duration_Avg', 'Pages_Per_Session',
    'Cart_Abandonment_Rate', 'Total_Purchases', 'Average_Order_Value',
    'Discount_Usage_Rate', 'Returns_Rate', 'Email_Open_Rate',
    'Social_Media_Engagement_Score', 'Mobile_App_Usage', 'Lifetime_Value'
]
for col in postgres_float_cols:
    if col in df_clean.columns:
        df_clean[col] = df_clean[col].round(2)

# 7. FINAL VALIDATION REPORT
print("\n" + "="*100)
print("✅ FINAL VALIDATION REPORT")
print("="*100)
print(f"📊 Dataset shape: {df_original.shape[0]:,} → {df_clean.shape[0]:,} rows (0 rows dropped)")
print(f"🔍 Missing values: {df_original.isnull().sum().sum():,} → {df_clean.isnull().sum().sum():,} (ALL FIXED)")
print(f"🔍 Duplicates: {df_original.duplicated().sum():,} → {df_clean.duplicated().sum():,}")
print("\n📈 KEY METRICS VALIDATION:")
print(f"   • Age: {df_clean['Age'].min():,} - {df_clean['Age'].max():,} years ✓")
print(f"   • Purchases: {df_clean['Total_Purchases'].min():.1f} - {df_clean['Total_Purchases'].max():.1f} ✓")
print(f"   • Cart Rate: {df_clean['Cart_Abandonment_Rate'].min():.1f}% - {df_clean['Cart_Abandonment_Rate'].max():.1f}% ✓")
print(f"   • Discount Rate: {df_clean['Discount_Usage_Rate'].min():.1f}% - {df_clean['Discount_Usage_Rate'].max():.1f}% ✓")
print(f"   • Churn rate: {df_clean['Churned'].mean()*100:.1f}% of {len(df_clean):,} customers")

print("\n" + "="*100)
print("💾 SAVING CLEANED DATASET")
print("="*100)
output_path = r'D:\Datapot class\Portfolio Dataset\Ecommerce Customer Churn\ecommerce_customer_churn_cleaned.csv'
df_clean.to_csv(output_path, index=False)
print(f"✅ Saved to: {output_path}")
