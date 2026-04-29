"""Bronze Layer - Raw ingestion with timestamp"""

import os
from pyspark.sql.functions import current_timestamp

from .common import get_spark_session, get_logger

logger = get_logger(__name__)


def write_bronze(df, output_path, source_name):
    """Write DataFrame to Bronze layer"""
    full_path = os.path.join(output_path, source_name)
    logger.info(f"Writing Bronze {source_name} to: {full_path}")
    df.write.mode("overwrite").format("parquet").save(full_path)


def run_ingest(config: dict):
    """Ingest all sources and write to Bronze"""
    logger.info("=" * 50)
    logger.info("Starting Bronze Layer Ingestion")
    logger.info("=" * 50)
    
    paths = config['paths']['input']
    output_bronze = config['paths']['output']['bronze']
    
    spark = get_spark_session(config)
    
    # Read customers.csv
    logger.info(f"Reading customers from: {paths['customers']}")
    customers_df = spark.read.option("header", "true").option("inferSchema", "true").csv(paths['customers'])
    customers_df = customers_df.withColumn("ingestion_timestamp", current_timestamp())
    write_bronze(customers_df, output_bronze, "customers")
    logger.info(f"Customers rows: {customers_df.count()}")
    
    # Read accounts.csv
    logger.info(f"Reading accounts from: {paths['accounts']}")
    accounts_df = spark.read.option("header", "true").option("inferSchema", "true").csv(paths['accounts'])
    accounts_df = accounts_df.withColumn("ingestion_timestamp", current_timestamp())
    write_bronze(accounts_df, output_bronze, "accounts")
    logger.info(f"Accounts rows: {accounts_df.count()}")
    
    # Read transactions.jsonl
    logger.info(f"Reading transactions from: {paths['transactions']}")
    transactions_df = spark.read.json(paths['transactions'])
    transactions_df = transactions_df.withColumn("ingestion_timestamp", current_timestamp())
    write_bronze(transactions_df, output_bronze, "transactions")
    logger.info(f"Transactions rows: {transactions_df.count()}")
    
    logger.info("Bronze layer ingestion completed")