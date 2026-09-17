# Databricks notebook source
from pyspark.sql import functions as F

BASE = "s3://shopkart-data-2026"

# Read existing Gold Delta table
daily_sales = spark.read.format("delta").load(
    f"{BASE}/gold/daily_sales"
)

# Export as Parquet
daily_sales.coalesce(1).write.mode("overwrite").parquet(
    f"{BASE}/redshift/daily_sales"
)

print("daily_sales exported successfully")
print("Rows:", daily_sales.count())

# COMMAND ----------

from pyspark.sql import functions as F

BASE = "s3://shopkart-data-2026"
EXPORT = f"{BASE}/redshift"

# ============================================================
# READ EXISTING GOLD DELTA TABLES
# ============================================================

product_performance = spark.read.format("delta").load(
    f"{BASE}/gold/product_performance"
)

customer_analytics = spark.read.format("delta").load(
    f"{BASE}/gold/customer_analytics"
)

category_sales = spark.read.format("delta").load(
    f"{BASE}/gold/category_sales"
)

payment_analytics = spark.read.format("delta").load(
    f"{BASE}/gold/payment_analytics"
)

overall_kpi = spark.read.format("delta").load(
    f"{BASE}/gold/overall_kpi"
)

# ============================================================
# EXPORT AS PARQUET
# ============================================================

product_performance.coalesce(1).write.mode("overwrite").parquet(
    f"{EXPORT}/product_performance"
)

customer_analytics.coalesce(1).write.mode("overwrite").parquet(
    f"{EXPORT}/customer_analytics"
)

category_sales.coalesce(1).write.mode("overwrite").parquet(
    f"{EXPORT}/category_sales"
)

payment_analytics.coalesce(1).write.mode("overwrite").parquet(
    f"{EXPORT}/payment_analytics"
)

overall_kpi.coalesce(1).write.mode("overwrite").parquet(
    f"{EXPORT}/overall_kpi"
)

print("=" * 60)
print("ALL GOLD DATA EXPORTED FOR REDSHIFT")
print("=" * 60)

print("Product Performance :", product_performance.count())
print("Customer Analytics  :", customer_analytics.count())
print("Category Sales      :", category_sales.count())
print("Payment Analytics   :", payment_analytics.count())
print("Overall KPI         :", overall_kpi.count())

# COMMAND ----------

from pyspark.sql import functions as F

BASE = "s3://shopkart-data-2026"
EXPORT = f"{BASE}/redshift"

# Read Silver tables
customers = spark.read.format("delta").load(f"{BASE}/silver/customers")
products = spark.read.format("delta").load(f"{BASE}/silver/products")
orders = spark.read.format("delta").load(f"{BASE}/silver/orders")
payments = spark.read.format("delta").load(f"{BASE}/silver/payments")

# Export as Parquet for Redshift
customers.coalesce(1).write.mode("overwrite").parquet(
    f"{EXPORT}/silver_customers"
)

products.coalesce(1).write.mode("overwrite").parquet(
    f"{EXPORT}/silver_products"
)

orders.coalesce(1).write.mode("overwrite").parquet(
    f"{EXPORT}/silver_orders"
)

payments.coalesce(1).write.mode("overwrite").parquet(
    f"{EXPORT}/silver_payments"
)

print("=" * 70)
print("SILVER DATA EXPORTED FOR REDSHIFT")
print("=" * 70)

print("Customers :", customers.count())
print("Products  :", products.count())
print("Orders    :", orders.count())
print("Payments  :", payments.count())

# COMMAND ----------

from pyspark.sql import functions as F

BASE = "s3://shopkart-data-2026"
EXPORT = f"{BASE}/redshift_clean"

customers = spark.read.format("delta").load(f"{BASE}/silver/customers")
products = spark.read.format("delta").load(f"{BASE}/silver/products")
orders = spark.read.format("delta").load(f"{BASE}/silver/orders")
payments = spark.read.format("delta").load(f"{BASE}/silver/payments")

customers.coalesce(1).write.mode("overwrite").parquet(
    f"{EXPORT}/customers"
)

products.coalesce(1).write.mode("overwrite").parquet(
    f"{EXPORT}/products"
)

orders.coalesce(1).write.mode("overwrite").parquet(
    f"{EXPORT}/orders"
)

payments.coalesce(1).write.mode("overwrite").parquet(
    f"{EXPORT}/payments"
)

print("CLEAN PARQUET EXPORT COMPLETE")
print("Customers:", customers.count())
print("Products :", products.count())
print("Orders   :", orders.count())
print("Payments :", payments.count())