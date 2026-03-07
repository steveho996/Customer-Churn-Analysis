
import pandas as pd
import numpy as np

# Load the original dataset
df = pd.read_csv('D:\\Datapot class\\Portfolio Dataset\\Ecommerce Customer Churn\\ecommerce_customer_churn_cleaned.csv')
df_clean = df.copy()  

# 1. CLEAN AGE COLUMN

print("\n1. Cleaning Age column")
print(f" - Missing values: {df_clean['Age'].isnull().sum()}")
print(f" - Values > 100: {(df_clean['Age'] > 100).sum()}")
print(f" - Values < 18: {(df_clean['Age'] < 18).sum()}")

# Cap age at reasonable range (18-80)
df_clean.loc[df_clean['Age'] > 100, 'Age'] = np.nan  # Set outliers to NaN
df_clean.loc[df_clean['Age'] < 18, 'Age'] = np.nan   # Set unrealistic ages to NaN

# Impute missing ages with median
age_median = df_clean['Age'].median()
df_clean['Age'] = df_clean['Age'].fillna(age_median)

df_clean['Age'] = df_clean['Age'].astype(int)
df_clean.to_csv('ecommerce_customer_churn_cleaned.csv', index=False)


print(f"✓ Capped ages to 18-100 range")
print(f"✓ Filled missing values with median: {age_median:.1f}")


# 2. CLEAN NEGATIVE TOTAL PURCHASES

print("\n2. Cleaning Total_Purchases column")
print(f" - Negative values: {(df_clean['Total_Purchases'] < 0).sum()}")

# Set negative purchases to 0
df_clean.loc[df_clean['Total_Purchases'] < 0, 'Total_Purchases'] = 0

print(f"✓ Set negative values to 0")


# 3. CLEAN PERCENTAGE COLUMNS

print("\n3. Cleaning percentage columns")

# Cart Abandonment Rate
cart_over_100 = (df_clean['Cart_Abandonment_Rate'] > 100).sum()
if cart_over_100 > 0:
    df_clean.loc[df_clean['Cart_Abandonment_Rate'] > 100, 'Cart_Abandonment_Rate'] = 100
    print(f" - Cart_Abandonment_Rate: Capped {cart_over_100} values at 100%")

# Discount Usage Rate
discount_over_100 = (df_clean['Discount_Usage_Rate'] > 100).sum()
if discount_over_100 > 0:
    df_clean.loc[df_clean['Discount_Usage_Rate'] > 100, 'Discount_Usage_Rate'] = 100
    print(f"   - Discount_Usage_Rate: Capped {discount_over_100} values at 100%")

print(f"   ✓ All percentage columns now in 0-100 range")


# 4. HANDLE MISSING VALUES

print("\n4. Handling missing values")

# Numerical columns - fill with median
numerical_cols_to_fill = [
    'Membership_Years', 'Login_Frequency', 'Session_Duration_Avg', 
    'Pages_Per_Session', 'Wishlist_Items', 'Days_Since_Last_Purchase',
    'Discount_Usage_Rate', 'Returns_Rate', 'Email_Open_Rate',
    'Customer_Service_Calls', 'Product_Reviews_Written',
    'Social_Media_Engagement_Score', 'Mobile_App_Usage',
    'Payment_Method_Diversity', 'Credit_Balance'
]

for col in numerical_cols_to_fill:
    if col in df_clean.columns:
        missing_count = df_clean[col].isnull().sum()
        if missing_count > 0:
            median_val = df_clean[col].median()
            df_clean[col] = df_clean[col].fillna(median_val)
            print(f"   - {col}: Filled {missing_count} values with median ({median_val:.2f})")

print(f"   ✓ All missing values handled")


# 5. DATA TYPE OPTIMIZATION

print("\n5. Optimizing data types")

# Convert integer-like columns to int 
int_columns = ['Churned', 'Customer_Service_Calls', 'Product_Reviews_Written', 
               'Wishlist_Items', 'Payment_Method_Diversity']

for col in int_columns:
    if col in df_clean.columns:
        # First ensure no NaN values
        if df_clean[col].isnull().sum() == 0:
            df_clean[col] = df_clean[col].astype(int)
        else:
            # Fill any remaining NaN with 0 then convert
            df_clean[col] = df_clean[col].fillna(0).astype(int)

print(f"   ✓ Optimized data types")


# 6. POSTGRESQL TYPE FIXING (clean integers + round floats to 2 decimals)

print("\n6. Fixing types for PostgreSQL compatibility")

# Columns that are whole numbers — convert float64 (e.g. 43.0) to clean int
postgres_int_columns = [
    'Age', 'Login_Frequency', 'Days_Since_Last_Purchase', 'Credit_Balance'
]
for col in postgres_int_columns:
    if col in df_clean.columns:
        df_clean[col] = df_clean[col].round(0).astype(int)
        print(f"   - {col}: converted to INTEGER (e.g. 43.0 -> 43)")

# Columns that are decimal — round to 2 decimal places to remove
# floating-point noise like 12.239999999999998
postgres_float_columns = [
    'Membership_Years', 'Session_Duration_Avg', 'Pages_Per_Session',
    'Cart_Abandonment_Rate', 'Total_Purchases', 'Average_Order_Value',
    'Discount_Usage_Rate', 'Returns_Rate', 'Email_Open_Rate',
    'Social_Media_Engagement_Score', 'Mobile_App_Usage', 'Lifetime_Value'
]
for col in postgres_float_columns:
    if col in df_clean.columns:
        df_clean[col] = df_clean[col].round(2)
        print(f"   - {col}: rounded to 2 decimal places")


# 6. FINAL VALIDATION

print("\n" + "="*80)
print("CLEANING SUMMARY")
print("="*80)

print(f"\nOriginal dataset: {df.shape[0]} rows")
print(f"Cleaned dataset: {df_clean.shape[0]} rows")
print(f"Rows removed: 0 (no rows deleted, only values corrected)")

print(f"\nMissing values after cleaning: {df_clean.isnull().sum().sum()}")
print(f"Duplicate rows: {df_clean.duplicated().sum()}")

# Check for any remaining issues
print("\n✓ Validation checks:")
print(f"   - Age range: {df_clean['Age'].min():.1f} - {df_clean['Age'].max():.1f}")
print(f"   - Negative purchases: {(df_clean['Total_Purchases'] < 0).sum()}")
print(f"   - Cart abandonment > 100: {(df_clean['Cart_Abandonment_Rate'] > 100).sum()}")
print(f"   - Discount usage > 100: {(df_clean['Discount_Usage_Rate'] > 100).sum()}")


# 7. SAVE CLEANED DATASET

print("\n" + "="*80)
print("SAVING CLEANED DATASET")
print("="*80)

output_path = r'D:\Datapot class\Portfolio Dataset\Ecommerce Customer Churn\ecommerce_customer_churn_cleaned.csv'
df_clean.to_csv(output_path, index=False)
print(f"✓ Cleaned dataset saved to: {output_path}")

