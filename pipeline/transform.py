"""Silver Layer - Standardize types, deduplicate, link accounts to customers"""

import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, row_number, to_date, to_timestamp, concat, lit
from pyspark.sql.window import Window

from .common import get_spark_session, get_logger

# Import DQ handler for Stage 2+
try:
    from .dq_handler import (
        load_dq_rules, deduplicate_transactions, handle_orphaned_transactions,
        normalize_amount, normalize_dates, normalize_currency
    )
    DQ_AVAILABLE = True
except ImportError:
    DQ_AVAILABLE = False

logger = get_logger(__name__)


def read_bronze(spark, bronze_path, source_name):
    """Read from Bronze layer"""
    full_path = os.path.join(bronze_path, source_name)
    return spark.read.format("parquet").load(full_path)


def write_silver(df, silver_path, source_name):
    """Write to Silver layer"""
    full_path = os.path.join(silver_path, source_name)
    logger.info(f"Writing Silver {source_name} to: {full_path}")
    df.write.mode("overwrite").format("parquet").save(full_path)


def deduplicate(df, key_col, order_cols):
    """Deduplicate keeping first record by order_cols"""
    window_spec = Window.partitionBy(key_col).orderBy(*order_cols)
    return df.withColumn("row_num", row_number().over(window_spec)) \
             .filter(col("row_num") == 1) \
             .drop("row_num")


def run_transform(config: dict):
    """Transform Bronze to Silver"""
    logger.info("=" * 50)
    logger.info("Starting Silver Layer Transformation")
    logger.info("=" * 50)
    
    bronze_path = config['paths']['output']['bronze']
    silver_path = config['paths']['output']['silver']
    
    spark = get_spark_session(config)
    
    # Read from Bronze
    customers_df = read_bronze(spark, bronze_path, "customers")
    accounts_df = read_bronze(spark, bronze_path, "accounts")
    transactions_df = read_bronze(spark, bronze_path, "transactions")
    
    logger.info(f"Raw counts - Customers: {customers_df.count()}, Accounts: {accounts_df.count()}, Transactions: {transactions_df.count()}")
    
    # --- Standardize dates ---
    # Customers: dob to DateType
    customers_df = customers_df.withColumn("dob", to_date(col("dob"), "yyyy-MM-dd"))
    
    # Accounts: open_date and last_activity_date to DateType
    accounts_df = accounts_df.withColumn("open_date", to_date(col("open_date"), "yyyy-MM-dd"))
    if "last_activity_date" in accounts_df.columns:
        accounts_df = accounts_df.withColumn("last_activity_date", to_date(col("last_activity_date"), "yyyy-MM-dd"))
    
    # Transactions: transaction_date to DateType
    transactions_df = transactions_df.withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd"))
    
    # --- Create transaction_timestamp from date and time ---
    transactions_df = transactions_df.withColumn(
        "transaction_timestamp",
        to_timestamp(concat(col("transaction_date"), lit(" "), col("transaction_time")), "yyyy-MM-dd HH:mm:ss")
    )
    
    # --- Deduplicate ---
    customers_df = deduplicate(customers_df, "customer_id", ["customer_id"])
    accounts_df = deduplicate(accounts_df, "account_id", ["open_date"])
    
    # Stage 2+: Use DQ-aware deduplication for transactions
    if DQ_AVAILABLE:
        try:
            dq_rules = load_dq_rules()
            transactions_df, dup_count = deduplicate_transactions(transactions_df)
        except Exception as e:
            logger.warning(f"DQ handler not available, using basic deduplication: {e}")
            transactions_df = deduplicate(transactions_df, "transaction_id", ["transaction_date", "transaction_time"])
    else:
        transactions_df = deduplicate(transactions_df, "transaction_id", ["transaction_date", "transaction_time"])
    
    # --- Link accounts to customers (validate referential integrity) ---
    # Rename customer_ref to customer_id for consistency
    # First rename, then validate with inner join
    accounts_df = accounts_df.withColumnRenamed("customer_ref", "customer_id")
    
    # Validate referential integrity by doing inner join
    accounts_df = accounts_df.join(
        customers_df.select("customer_id"),
        "customer_id",
        "inner"  # Stage 1: all accounts have valid customer_ref
    )
    
    logger.info(f"Silver counts - Customers: {customers_df.count()}, Accounts: {accounts_df.count()}, Transactions: {transactions_df.count()}")
    
    # Write to Silver
    write_silver(customers_df, silver_path, "customers")
    write_silver(accounts_df, silver_path, "accounts")
    write_silver(transactions_df, silver_path, "transactions")
    
    logger.info("Silver layer transformation completed")