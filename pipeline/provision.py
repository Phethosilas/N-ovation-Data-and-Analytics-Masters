"""Gold Layer - Dimensional model: dim_customers, dim_accounts, fact_transactions"""

import os
from pyspark.sql.functions import col, lit, when, current_date, datediff, floor, row_number
from pyspark.sql.window import Window

from .common import get_spark_session, get_logger

logger = get_logger(__name__)


def read_silver(spark, silver_path, source_name):
    """Read from Silver layer"""
    full_path = os.path.join(silver_path, source_name)
    return spark.read.format("parquet").load(full_path)


def write_gold(df, gold_path, table_name):
    """Write to Gold layer"""
    full_path = os.path.join(gold_path, table_name)
    logger.info(f"Writing Gold {table_name} to: {full_path}")
    df.write.mode("overwrite").format("parquet").save(full_path)


def generate_surrogate_key(df, key_col, sk_name):
    """Generate deterministic surrogate key using row_number"""
    window = Window.orderBy(key_col)
    return df.withColumn(sk_name, row_number().over(window))


def build_dim_customers(df, config):
    """Build dim_customers with derived age_band"""
    logger.info("Building dim_customers")
    
    # Calculate age from dob
    age_days = datediff(current_date(), col("dob"))
    age_years = floor(age_days / 365.25)
    
    # Apply age bands
    age_band = lit(None)
    for min_age, max_age, band in config['gold']['age_band']['buckets']:
        age_band = when(
            (age_years >= min_age) & (age_years < max_age),
            lit(band)
        ).otherwise(age_band)
    
    df = df.withColumn("age_band", age_band)
    
    # Generate surrogate key
    df = generate_surrogate_key(df, "customer_id", "customer_sk")
    
    # Select columns in correct order (9 columns per schema)
    return df.select(
        "customer_sk",
        "customer_id",
        "gender",
        "province",
        "income_band",
        "segment",
        "risk_score",
        "kyc_status",
        "age_band"
    )


def build_dim_accounts(df):
    """Build dim_accounts with customer_id (required for validation query 2)"""
    logger.info("Building dim_accounts")
    
    # Generate surrogate key
    df = generate_surrogate_key(df, "account_id", "account_sk")
    
    # Rename customer_id column (already renamed in transform)
    # Select columns in correct order (11 columns per schema)
    return df.select(
        "account_sk",
        "account_id",
        "customer_id",  # Critical for validation query 2
        "account_type",
        "account_status",
        "open_date",
        "product_tier",
        "digital_channel",
        "credit_limit",
        "current_balance",
        "last_activity_date"
    )


def build_fact_transactions(transactions_df, dim_accounts_df, dim_customers_df):
    """Build fact_transactions with foreign keys"""
    logger.info("Building fact_transactions")
    
    # Join to accounts to get account_sk and customer_id
    fact = transactions_df.join(
        dim_accounts_df.select("account_id", "account_sk", "customer_id"),
        transactions_df["account_id"] == dim_accounts_df["account_id"],
        "inner"
    )
    
    # Join to customers to get customer_sk
    fact = fact.join(
        dim_customers_df.select("customer_id", "customer_sk"),
        fact["customer_id"] == dim_customers_df["customer_id"],
        "inner"
    )
    
    # Generate transaction surrogate key
    fact = generate_surrogate_key(fact, "transaction_id", "transaction_sk")
    
    # Extract province from nested location struct
    fact = fact.withColumn("province", col("location.province"))
    
    # Stage 1: No DQ flags (all clean)
    fact = fact.withColumn("dq_flag", lit(None))
    
    # merchant_subcategory not in Stage 1 data, will be null
    if "merchant_subcategory" not in fact.columns:
        fact = fact.withColumn("merchant_subcategory", lit(None))
    
    # Select columns in correct order (15 columns per schema)
    return fact.select(
        "transaction_sk",
        "transaction_id",
        "account_sk",
        "customer_sk",
        "transaction_date",
        "transaction_timestamp",
        "transaction_type",
        "merchant_category",
        "merchant_subcategory",
        "amount",
        "currency",
        "channel",
        "province",
        "dq_flag",
        "ingestion_timestamp"
    )


def run_provision(config: dict):
    """Build Gold layer dimensional model"""
    logger.info("=" * 50)
    logger.info("Starting Gold Layer Provisioning")
    logger.info("=" * 50)
    
    silver_path = config['paths']['output']['silver']
    gold_path = config['paths']['output']['gold']
    
    spark = get_spark_session(config)
    
    # Read from Silver
    customers_df = read_silver(spark, silver_path, "customers")
    accounts_df = read_silver(spark, silver_path, "accounts")
    transactions_df = read_silver(spark, silver_path, "transactions")
    
    # Build dimension tables
    dim_customers = build_dim_customers(customers_df, config)
    dim_accounts = build_dim_accounts(accounts_df)
    
    # Build fact table (depends on dimensions)
    fact_transactions = build_fact_transactions(transactions_df, dim_accounts, dim_customers)
    
    # Write to Gold
    write_gold(dim_customers, gold_path, "dim_customers")
    write_gold(dim_accounts, gold_path, "dim_accounts")
    write_gold(fact_transactions, gold_path, "fact_transactions")
    
    logger.info(f"Gold counts - dim_customers: {dim_customers.count()}, dim_accounts: {dim_accounts.count()}, fact_transactions: {fact_transactions.count()}")
    logger.info("Gold layer provisioning completed")