"""Stage 3 - Streaming Ingestion from /data/stream/"""

import os
import glob
import time
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, desc, row_number, when
from pyspark.sql.window import Window

from .common import get_spark_session, get_logger

logger = get_logger(__name__)


def discover_stream_files(stream_dir="/data/stream/", processed_files=set()):
    """Discover new stream files that haven't been processed"""
    pattern = os.path.join(stream_dir, "stream_*.jsonl")
    all_files = sorted(glob.glob(pattern))
    new_files = [f for f in all_files if f not in processed_files]
    logger.info(f"Found {len(new_files)} new stream files (total: {len(all_files)})")
    return new_files


def process_stream_batch(spark, file_path: str, config: dict) -> DataFrame:
    """Process a single stream batch file"""
    logger.info(f"Processing stream file: {file_path}")
    
    # Read JSONL file
    df = spark.read.json(file_path)
    
    # Add ingestion timestamp
    df = df.withColumn("ingestion_timestamp", current_timestamp())
    
    # Create transaction_timestamp from date + time
    df = df.withColumn(
        "transaction_timestamp",
        col("transaction_date").cast("timestamp") + 
        (col("transaction_time").cast("string").substr(1, 2).cast("int") * 3600 +
         col("transaction_time").cast("string").substr(4, 2).cast("int") * 60 +
         col("transaction_time").cast("string").substr(7, 2).cast("int")).cast("interval second")
    )
    
    logger.info(f"Loaded {df.count()} events from {os.path.basename(file_path)}")
    
    return df


def update_current_balances(spark, stream_df: DataFrame, gold_path: str):
    """Update current_balances table with new transactions"""
    logger.info("Updating current_balances...")
    
    balance_path = os.path.join(gold_path, "../stream_gold/current_balances")
    
    # Calculate balance changes per account
    balance_updates = stream_df.groupBy("account_id").agg(
        {"transaction_timestamp": "max", "amount": "sum"}
    ).withColumnRenamed("max(transaction_timestamp)", "last_transaction_timestamp") \
      .withColumnRenamed("sum(amount)", "balance_change")
    
    # Try to read existing balances
    try:
        existing_balances = spark.read.format("parquet").load(balance_path)
        
        # Merge with new updates
        updated_balances = existing_balances.join(
            balance_updates, "account_id", "left"
        ).withColumn(
            "current_balance",
            col("current_balance") + col("balance_change").fillna(0)
        ).withColumn(
            "last_transaction_timestamp",
            when(col("last_transaction_timestamp").isNotNull(),
                 col("last_transaction_timestamp"))
            .otherwise(existing_balances["last_transaction_timestamp"])
        )
    except:
        # First run - create initial balances
        updated_balances = balance_updates.withColumnRenamed("balance_change", "current_balance")
    
    # Add updated_at timestamp
    updated_balances = updated_balances.withColumn("updated_at", current_timestamp())
    
    # Write updated balances
    updated_balances.write.mode("overwrite").format("parquet").save(balance_path)
    
    logger.info(f"Updated {updated_balances.count()} account balances")


def update_recent_transactions(spark, stream_df: DataFrame, gold_path: str):
    """Update recent_transactions table (last 50 per account)"""
    logger.info("Updating recent_transactions...")
    
    recent_path = os.path.join(gold_path, "../stream_gold/recent_transactions")
    
    # Prepare new transactions
    new_transactions = stream_df.select(
        "account_id",
        "transaction_id",
        "transaction_timestamp",
        "amount",
        "transaction_type",
        "channel"
    ).withColumn("updated_at", current_timestamp())
    
    # Try to read existing transactions
    try:
        existing_transactions = spark.read.format("parquet").load(recent_path)
        all_transactions = existing_transactions.union(new_transactions)
    except:
        all_transactions = new_transactions
    
    # Keep only last 50 per account
    window_spec = Window.partitionBy("account_id").orderBy(desc("transaction_timestamp"))
    recent_50 = all_transactions.withColumn("row_num", row_number().over(window_spec)) \
                                .filter(col("row_num") <= 50) \
                                .drop("row_num")
    
    # Write updated recent transactions
    recent_50.write.mode("overwrite").format("parquet").save(recent_path)
    
    logger.info(f"Updated recent transactions: {recent_50.count()} total records")


def run_streaming_pipeline(config: dict, poll_interval=60, max_iterations=None):
    """Main streaming pipeline - polls for new files"""
    logger.info("=" * 50)
    logger.info("Starting Stage 3 Streaming Pipeline")
    logger.info("=" * 50)
    
    spark = get_spark_session(config)
    stream_dir = config.get('paths', {}).get('stream', '/data/stream/')
    gold_path = config['paths']['output']['gold']
    
    processed_files = set()
    iteration = 0
    
    while True:
        iteration += 1
        logger.info(f"\n--- Polling iteration {iteration} ---")
        
        # Discover new files
        new_files = discover_stream_files(stream_dir, processed_files)
        
        if not new_files:
            logger.info("No new files found")
        else:
            for file_path in new_files:
                try:
                    # Process batch
                    stream_df = process_stream_batch(spark, file_path, config)
                    
                    # Update stream gold tables
                    update_current_balances(spark, stream_df, gold_path)
                    update_recent_transactions(spark, stream_df, gold_path)
                    
                    # Mark as processed
                    processed_files.add(file_path)
                    
                except Exception as e:
                    logger.error(f"Error processing {file_path}: {e}")
        
        # Check exit condition
        if max_iterations and iteration >= max_iterations:
            logger.info(f"Reached max iterations ({max_iterations}), exiting")
            break
        
        # Check if all expected files are processed (for testing)
        all_files = discover_stream_files(stream_dir, set())
        if len(processed_files) == len(all_files) and len(all_files) > 0:
            logger.info("All stream files processed, exiting")
            break
        
        # Wait before next poll
        if new_files:
            logger.info(f"Waiting {poll_interval} seconds before next poll...")
            time.sleep(poll_interval)
        else:
            # No files yet, check if any exist at all
            if not all_files:
                logger.info("No stream files found, exiting (batch mode)")
                break
            time.sleep(5)  # Quick retry
    
    logger.info("Streaming pipeline completed")
