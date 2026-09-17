from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator


# ============================================================
# SHOPKART CONFIGURATION
# ============================================================

S3_BUCKET = "shopkart-data-2026"

AWS_CONN_ID = "aws_shopkart"

RAW_PREFIX = "raw"

DATABRICKS_CONN_ID = "databricks_shopkart"

DATABRICKS_JOB_ID = 478726098061363


# ============================================================
# FUNCTIONS
# ============================================================

def start_pipeline():

    print("=" * 70)
    print("SHOPKART DAILY DATA ENGINEERING PIPELINE STARTED")
    print("=" * 70)

    print(f"S3 Bucket         : {S3_BUCKET}")
    print(f"AWS Connection    : {AWS_CONN_ID}")
    print(f"Raw Prefix        : {RAW_PREFIX}")
    print(f"Databricks Job ID : {DATABRICKS_JOB_ID}")


def pipeline_success():

    print("=" * 70)
    print("SHOPKART PIPELINE COMPLETED SUCCESSFULLY")
    print("=" * 70)


# ============================================================
# DAG
# ============================================================

with DAG(

    dag_id="shopkart_daily_pipeline",

    start_date=datetime(2026, 9, 14),

    schedule="@daily",

    catchup=False,

    default_args={
        "owner": "shopkart-data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },

    tags=[
        "shopkart",
        "aws",
        "s3",
        "databricks",
        "pyspark",
        "delta-lake",
    ],

) as dag:

    # ========================================================
    # START
    # ========================================================

    start = PythonOperator(
        task_id="start_pipeline",
        python_callable=start_pipeline,
    )


    # ========================================================
    # CUSTOMERS
    # ========================================================

    check_customers = S3KeySensor(

        task_id="check_customers_file",

        bucket_name=S3_BUCKET,

        bucket_key=f"{RAW_PREFIX}/customers/customers.csv",

        aws_conn_id=AWS_CONN_ID,

        poke_interval=30,

        timeout=300,

        mode="reschedule",

    )


    # ========================================================
    # PRODUCTS
    # ========================================================

    check_products = S3KeySensor(

        task_id="check_products_file",

        bucket_name=S3_BUCKET,

        bucket_key=f"{RAW_PREFIX}/products/products.csv",

        aws_conn_id=AWS_CONN_ID,

        poke_interval=30,

        timeout=300,

        mode="reschedule",

    )


    # ========================================================
    # ORDERS
    # ========================================================

    check_orders = S3KeySensor(

        task_id="check_orders_file",

        bucket_name=S3_BUCKET,

        bucket_key=f"{RAW_PREFIX}/orders/orders.csv",

        aws_conn_id=AWS_CONN_ID,

        poke_interval=30,

        timeout=300,

        mode="reschedule",

    )


    # ========================================================
    # PAYMENTS
    # ========================================================

    check_payments = S3KeySensor(

        task_id="check_payments_file",

        bucket_name=S3_BUCKET,

        bucket_key=f"{RAW_PREFIX}/payments/payments.csv",

        aws_conn_id=AWS_CONN_ID,

        poke_interval=30,

        timeout=300,

        mode="reschedule",

    )


    # ========================================================
    # DATABRICKS JOB
    # ========================================================

    run_databricks = DatabricksRunNowOperator(

        task_id="run_shopkart_databricks_job",

        databricks_conn_id=DATABRICKS_CONN_ID,

        job_id=DATABRICKS_JOB_ID,

    )


    # ========================================================
    # SUCCESS
    # ========================================================

    success = PythonOperator(

        task_id="pipeline_success",

        python_callable=pipeline_success,

    )


    # ========================================================
    # DEPENDENCIES
    # ========================================================

    start >> [
        check_customers,
        check_products,
        check_orders,
        check_payments,
    ]

    [
        check_customers,
        check_products,
        check_orders,
        check_payments,
    ] >> run_databricks >> success