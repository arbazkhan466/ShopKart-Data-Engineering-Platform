# Databricks notebook source
orders_path = "s3://shopkart-data-2026/raw/orders/orders.csv"

df_orders = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(orders_path)
)

display(df_orders)

# COMMAND ----------

from pyspark.sql import functions as F

# ============================================================
# SHOPKART - BRONZE ETL
# RAW CSV → BRONZE DELTA
# ============================================================

BASE = "s3://shopkart-data-2026"

print("==========================================")
print("SHOPKART BRONZE ETL STARTED")
print("==========================================")


# ============================================================
# 1. CUSTOMERS
# ============================================================

customers_path = f"{BASE}/raw/customers/customers.csv"

df_customers = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(customers_path)
    .withColumn("_ingestion_timestamp", F.current_timestamp())
)

display(df_customers)


# ============================================================
# 2. PRODUCTS
# ============================================================

products_path = f"{BASE}/raw/products/products.csv"

df_products = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(products_path)
    .withColumn("_ingestion_timestamp", F.current_timestamp())
)

display(df_products)


# ============================================================
# 3. ORDERS
# ============================================================

orders_path = f"{BASE}/raw/orders/orders.csv"

df_orders = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(orders_path)
    .withColumn("_ingestion_timestamp", F.current_timestamp())
)

display(df_orders)


# ============================================================
# 4. PAYMENTS
# ============================================================

payments_path = f"{BASE}/raw/payments/payments.csv"

df_payments = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(payments_path)
    .withColumn("_ingestion_timestamp", F.current_timestamp())
)

display(df_payments)


# ============================================================
# 5. WRITE CUSTOMERS → BRONZE
# ============================================================

df_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{BASE}/bronze/customers")


# ============================================================
# 6. WRITE PRODUCTS → BRONZE
# ============================================================

df_products.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{BASE}/bronze/products")


# ============================================================
# 7. WRITE ORDERS → BRONZE
# ============================================================

df_orders.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{BASE}/bronze/orders")


# ============================================================
# 8. WRITE PAYMENTS → BRONZE
# ============================================================

df_payments.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{BASE}/bronze/payments")


# ============================================================
# 9. VALIDATION
# ============================================================

print("\n==========================================")
print("BRONZE TABLE COUNTS")
print("==========================================")

print("Customers:", df_customers.count())
print("Products :", df_products.count())
print("Orders   :", df_orders.count())
print("Payments :", df_payments.count())

print("\n==========================================")
print("SHOPKART BRONZE ETL COMPLETED")
print("==========================================")

# COMMAND ----------

from pyspark.sql import functions as F

# ============================================================
# SHOPKART - SILVER ETL
# BRONZE DELTA → SILVER DELTA
# ============================================================

BASE = "s3://shopkart-data-2026"

print("==========================================")
print("SHOPKART SILVER ETL STARTED")
print("==========================================")


# ============================================================
# 1. READ BRONZE
# ============================================================

customers = spark.read.format("delta").load(
    f"{BASE}/bronze/customers"
)

products = spark.read.format("delta").load(
    f"{BASE}/bronze/products"
)

orders = spark.read.format("delta").load(
    f"{BASE}/bronze/orders"
)

payments = spark.read.format("delta").load(
    f"{BASE}/bronze/payments"
)

print("Bronze data loaded")

print("Customers:", customers.count())
print("Products :", products.count())
print("Orders   :", orders.count())
print("Payments :", payments.count())


# ============================================================
# 2. CLEAN CUSTOMERS
# ============================================================

customers_clean = (
    customers
    .filter(F.col("customer_id").isNotNull())
    .dropDuplicates(["customer_id"])
    .withColumn(
        "customer_name",
        F.trim(F.col("customer_name"))
    )
    .withColumn(
        "email",
        F.lower(F.trim(F.col("email")))
    )
    .withColumn(
        "city",
        F.trim(F.col("city"))
    )
    .withColumn(
        "state",
        F.trim(F.col("state"))
    )
    .withColumn(
        "country",
        F.trim(F.col("country"))
    )
)

print("Clean Customers:", customers_clean.count())


# ============================================================
# 3. CLEAN PRODUCTS
# ============================================================

products_clean = (
    products
    .filter(F.col("product_id").isNotNull())
    .dropDuplicates(["product_id"])
    .withColumn(
        "product_name",
        F.trim(F.col("product_name"))
    )
    .withColumn(
        "category",
        F.trim(F.col("category"))
    )
    .withColumn(
        "subcategory",
        F.trim(F.col("subcategory"))
    )
    .withColumn(
        "price",
        F.col("price").cast("double")
    )
    .withColumn(
        "cost",
        F.col("cost").cast("double")
    )
    .filter(F.col("price") >= 0)
    .filter(F.col("cost") >= 0)
)

print("Clean Products:", products_clean.count())


# ============================================================
# 4. CLEAN ORDERS
# ============================================================

orders_clean = (
    orders
    .filter(F.col("order_id").isNotNull())
    .filter(F.col("customer_id").isNotNull())
    .filter(F.col("product_id").isNotNull())
    .dropDuplicates(["order_id"])

    # IMPORTANT:
    # Explicitly convert order_date to DATE
    .withColumn(
        "order_date",
        F.to_date(F.col("order_date"))
    )

    .withColumn(
        "quantity",
        F.col("quantity").cast("int")
    )

    .withColumn(
        "discount",
        F.col("discount").cast("double")
    )

    .withColumn(
        "order_amount",
        F.col("order_amount").cast("double")
    )

    .filter(F.col("quantity") > 0)
    .filter(F.col("order_amount") >= 0)
)

print("Clean Orders:", orders_clean.count())

print("Order schema:")
orders_clean.printSchema()


# ============================================================
# 5. ORDER → CUSTOMER VALIDATION
# ============================================================

orders_valid = (
    orders_clean
    .join(
        customers_clean
        .select("customer_id")
        .distinct(),
        on="customer_id",
        how="inner"
    )
)

print(
    "Orders after Customer validation:",
    orders_valid.count()
)


# ============================================================
# 6. ORDER → PRODUCT VALIDATION
# ============================================================

orders_valid = (
    orders_valid
    .join(
        products_clean.select(
            "product_id",
            "product_name",
            "category"
        ),
        on="product_id",
        how="inner"
    )
)

print(
    "Orders after Product validation:",
    orders_valid.count()
)


# ============================================================
# 7. CLEAN PAYMENTS
# ============================================================

payments_clean = (
    payments
    .filter(F.col("payment_id").isNotNull())
    .filter(F.col("order_id").isNotNull())
    .dropDuplicates(["payment_id"])
    .withColumn(
        "payment_date",
        F.to_date(F.col("payment_date"))
    )
    .withColumn(
        "amount",
        F.col("amount").cast("double")
    )
    .filter(F.col("amount") >= 0)
)

print(
    "Payments before Order validation:",
    payments_clean.count()
)


# ============================================================
# 8. PAYMENT → ORDER VALIDATION
# ============================================================

payments_valid = (
    payments_clean
    .join(
        orders_valid
        .select("order_id")
        .distinct(),
        on="order_id",
        how="inner"
    )
)

invalid_payments = (
    payments_clean.count()
    - payments_valid.count()
)

print(
    "Payments after Order validation:",
    payments_valid.count()
)

print(
    "Invalid payments removed:",
    invalid_payments
)


# ============================================================
# 9. PAYMENT INFORMATION
# ============================================================

payment_info = (
    payments_valid
    .select(
        "order_id",
        "payment_id",
        "payment_method",
        "payment_status"
    )
    .dropDuplicates(["order_id"])
)


# ============================================================
# 10. FINAL SILVER ORDERS
# ============================================================

orders_silver = (
    orders_valid
    .join(
        payment_info,
        on="order_id",
        how="left"
    )
    .withColumn(
        "revenue",
        F.col("order_amount")
    )
    .withColumn(
        "silver_processed_at",
        F.current_timestamp()
    )
)


# ============================================================
# 11. FINAL SILVER CUSTOMERS
# ============================================================

customers_silver = (
    customers_clean
    .withColumn(
        "silver_processed_at",
        F.current_timestamp()
    )
)


# ============================================================
# 12. FINAL SILVER PRODUCTS
# ============================================================

products_silver = (
    products_clean
    .withColumn(
        "silver_processed_at",
        F.current_timestamp()
    )
)


# ============================================================
# 13. FINAL SILVER PAYMENTS
# ============================================================

payments_silver = (
    payments_valid
    .withColumn(
        "silver_processed_at",
        F.current_timestamp()
    )
)


# ============================================================
# 14. WRITE CUSTOMERS
# ============================================================

customers_silver.write \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .save(f"{BASE}/silver/customers")


# ============================================================
# 15. WRITE PRODUCTS
# ============================================================

products_silver.write \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .save(f"{BASE}/silver/products")


# ============================================================
# 16. WRITE ORDERS
# ============================================================

orders_silver.write \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .save(f"{BASE}/silver/orders")


# ============================================================
# 17. WRITE PAYMENTS
# ============================================================

payments_silver.write \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .save(f"{BASE}/silver/payments")


# ============================================================
# 18. FINAL COUNTS
# ============================================================

print("\n==========================================")
print("SILVER TABLE COUNTS")
print("==========================================")

print("Customers:", customers_silver.count())
print("Products :", products_silver.count())
print("Orders   :", orders_silver.count())
print("Payments :", payments_silver.count())

print("\nInvalid payments removed:", invalid_payments)

print("\n==========================================")
print("SHOPKART SILVER ETL COMPLETED SUCCESSFULLY")
print("==========================================")

# COMMAND ----------

from pyspark.sql import functions as F

# ============================================================
# SHOPKART - GOLD ETL
# SILVER DELTA → GOLD DELTA
# ============================================================

BASE = "s3://shopkart-data-2026"

print("==========================================")
print("SHOPKART GOLD ETL STARTED")
print("==========================================")


# ============================================================
# 1. READ SILVER
# ============================================================

customers = spark.read.format("delta").load(
    f"{BASE}/silver/customers"
)

products = spark.read.format("delta").load(
    f"{BASE}/silver/products"
)

orders = spark.read.format("delta").load(
    f"{BASE}/silver/orders"
)

payments = spark.read.format("delta").load(
    f"{BASE}/silver/payments"
)

print("Silver data loaded")

print("Customers:", customers.count())
print("Products :", products.count())
print("Orders   :", orders.count())
print("Payments :", payments.count())


# ============================================================
# 2. DAILY SALES
# ============================================================

daily_sales = (
    orders
    .withColumn(
        "order_day",
        F.to_date("order_date")
    )
    .groupBy("order_day")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("quantity").alias("total_items"),
        F.sum("order_amount").alias("total_revenue"),
        F.avg("order_amount").alias("average_order_value")
    )
    .orderBy("order_day")
)

print("Daily Sales created")


# ============================================================
# 3. PRODUCT PERFORMANCE
# ============================================================

product_performance = (
    orders
    .groupBy(
        "product_id",
        "product_name",
        "category"
    )
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("quantity").alias("total_quantity"),
        F.sum("order_amount").alias("total_revenue"),
        F.avg("order_amount").alias("average_order_value")
    )
    .orderBy(F.desc("total_revenue"))
)

print("Product Performance created")


# ============================================================
# 4. CUSTOMER ANALYTICS
# ============================================================

customer_analytics = (
    orders
    .groupBy("customer_id")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("quantity").alias("total_items"),
        F.sum("order_amount").alias("total_spend"),
        F.avg("order_amount").alias("average_order_value")
    )
    .orderBy(F.desc("total_spend"))
)

print("Customer Analytics created")


# ============================================================
# 5. CATEGORY SALES
# ============================================================

category_sales = (
    orders
    .groupBy("category")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("quantity").alias("total_quantity"),
        F.sum("order_amount").alias("total_revenue"),
        F.avg("order_amount").alias("average_order_value")
    )
    .orderBy(F.desc("total_revenue"))
)

print("Category Sales created")


# ============================================================
# 6. PAYMENT ANALYTICS
# ============================================================

payment_analytics = (
    payments
    .groupBy(
        "payment_method",
        "payment_status"
    )
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("amount").alias("total_payment_amount")
    )
    .orderBy(F.desc("total_payment_amount"))
)

print("Payment Analytics created")


# ============================================================
# 7. OVERALL KPI
# ============================================================

overall_kpi = (
    orders
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.countDistinct("customer_id").alias("total_customers"),
        F.countDistinct("product_id").alias("total_products"),
        F.sum("quantity").alias("total_items"),
        F.sum("order_amount").alias("total_revenue"),
        F.avg("order_amount").alias("aov")
    )
)

print("Overall KPI created")


# ============================================================
# 8. WRITE GOLD - DAILY SALES
# ============================================================

daily_sales.write \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .save(f"{BASE}/gold/daily_sales")


# ============================================================
# 9. WRITE GOLD - PRODUCT PERFORMANCE
# ============================================================

product_performance.write \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .save(f"{BASE}/gold/product_performance")


# ============================================================
# 10. WRITE GOLD - CUSTOMER ANALYTICS
# ============================================================

customer_analytics.write \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .save(f"{BASE}/gold/customer_analytics")


# ============================================================
# 11. WRITE GOLD - CATEGORY SALES
# ============================================================

category_sales.write \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .save(f"{BASE}/gold/category_sales")


# ============================================================
# 12. WRITE GOLD - PAYMENT ANALYTICS
# ============================================================

payment_analytics.write \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .save(f"{BASE}/gold/payment_analytics")


# ============================================================
# 13. WRITE GOLD - OVERALL KPI
# ============================================================

overall_kpi.write \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .save(f"{BASE}/gold/overall_kpi")


# ============================================================
# 14. DISPLAY KPI
# ============================================================

print("\n==========================================")
print("OVERALL BUSINESS KPI")
print("==========================================")

display(overall_kpi)


# ============================================================
# 15. DISPLAY SAMPLE GOLD DATA
# ============================================================

print("\nDAILY SALES")
display(daily_sales.limit(10))

print("\nPRODUCT PERFORMANCE")
display(product_performance.limit(10))

print("\nCUSTOMER ANALYTICS")
display(customer_analytics.limit(10))

print("\nCATEGORY SALES")
display(category_sales)

print("\nPAYMENT ANALYTICS")
display(payment_analytics)


# ============================================================
# FINAL
# ============================================================

print("\n==========================================")
print("SHOPKART GOLD ETL COMPLETED SUCCESSFULLY")
print("==========================================")