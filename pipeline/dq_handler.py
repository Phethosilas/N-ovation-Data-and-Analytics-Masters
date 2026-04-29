"""Data Quality Handler - Stage 2+"""

import os
import json
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit, row_number, to_date, to_timestamp, 
    regexp_extract, length, upper, trim
)
from pyspark.sql.window import Window

from .common import get_logger

logger = get_logger(__name__)


def load_dq_rules(config_path="/data/config/dq_rules.yaml"):
    """Load DQ rules from YAML config"""
    import yaml
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def deduplicate_transactions(df: DataFrame) -> tuple:
    """Remove duplicate transactions, keep first by timestamp"""
    logger.info("Checking for duplicate transactions...")
    
    initial_count = df.count()
    
    # Find duplicates
    window_spec = Window.partitionBy("transaction_id").orderBy("transaction_timestamp")
    df_with_row = df.withColumn("row_num", row_number().over(window_spec))
    
    duplicates_count = df_with_row.filter(col("row_num") > 1).count()
    
    # Keep only first occurrence
    df_clean = df_with_row.filter(col("row_num") == 1).drop("row_num")
    
    logger.info(f"Found {duplicates_count} duplicate transactions, kept first occurrence")
    
    return df_clean, duplicates_count


def handle_orphaned_transactions(transactions_df: DataFrame, accounts_df: DataFrame) -> tuple:
    """Identify and quarantine orphaned transactions"""
    logger.info("Checking for orphaned transactions...")
    
    # Mark orphaned transactions
    transactions_with_flag = transactions_df.join(
        accounts_df.select("account_id").withColumnRenamed("account_id", "valid_account_id"),
        transactions_df["account_id"] == col("valid_account_id"),
        "left"
    )
    
    orphaned_count = transactions_with_flag.filter(col("valid_account_id").isNull()).count()
    
    # Flag orphaned records
    transactions_flagged = transactions_with_flag.withColumn(
        "dq_flag",
        when(col("valid_account_id").isNull(), lit("ORPHANED_ACCOUNT")).otherwise(col("dq_flag"))
    ).drop("valid_account_id")
    
    logger.info(f"Found {orphaned_count} orphaned transactions")
    
    return transactions_flagged, orphaned_count


def normalize_amount(df: DataFrame) -> tuple:
    """Cast string amounts to DECIMAL"""
    logger.info("Normalizing amount field...")
    
    # Try to cast to decimal, mark failures
    df_normalized = df.withColumn(
        "amount_temp",
        when(col("amount").cast("decimal(18,2)").isNotNull(), 
             col("amount").cast("decimal(18,2)"))
        .otherwise(None)
    )
    
    # Count type mismatches
    type_mismatch_count = df_normalized.filter(
        col("amount").isNotNull() & col("amount_temp").isNull()
    ).count()
    
    # Update amount column
    df_normalized = df_normalized.withColumn("amount", col("amount_temp")).drop("amount_temp")
    
    # Flag records that were cast
    if type_mismatch_count > 0:
        logger.info(f"Cast {type_mismatch_count} STRING amounts to DECIMAL")
    
    return df_normalized, type_mismatch_count


def normalize_dates(df: DataFrame, date_columns: list) -> tuple:
    """Normalize date formats to YYYY-MM-DD"""
    logger.info(f"Normalizing date formats for columns: {date_columns}")
    
    total_normalized = 0
    
    for date_col in date_columns:
        if date_col not in df.columns:
            continue
            
        # Try multiple date formats
        df = df.withColumn(
            f"{date_col}_normalized",
            when(to_date(col(date_col), "yyyy-MM-dd").isNotNull(), 
                 to_date(col(date_col), "yyyy-MM-dd"))
            .when(to_date(col(date_col), "dd/MM/yyyy").isNotNull(),
                  to_date(col(date_col), "dd/MM/yyyy"))
            .when(to_date(col(date_col), "MM/dd/yyyy").isNotNull(),
                  to_date(col(date_col), "MM/dd/yyyy"))
            .when(length(col(date_col)) == 10 & col(date_col).cast("long").isNotNull(),
                  to_date(col(date_col).cast("long").cast("timestamp")))
            .otherwise(None)
        )
        
        # Count how many were normalized (non-standard format)
        non_standard = df.filter(
            col(date_col).isNotNull() & 
            (to_date(col(date_col), "yyyy-MM-dd").isNull())
        ).count()
        
        total_normalized += non_standard
        
        # Replace original column
        df = df.withColumn(date_col, col(f"{date_col}_normalized")).drop(f"{date_col}_normalized")
    
    logger.info(f"Normalized {total_normalized} non-standard date values")
    
    return df, total_normalized


def normalize_currency(df: DataFrame, dq_rules: dict) -> tuple:
    """Normalize currency variants to ZAR"""
    logger.info("Normalizing currency field...")
    
    variant_mappings = dq_rules['dq_rules']['currency_variants']['variant_mappings']
    
    # Count non-standard values
    variant_count = df.filter(col("currency") != "ZAR").count()
    
    # Apply mappings
    currency_expr = col("currency")
    for variant, standard in variant_mappings.items():
        currency_expr = when(upper(trim(col("currency"))) == variant.upper(), standard).otherwise(currency_expr)
    
    df = df.withColumn("currency", currency_expr)
    
    logger.info(f"Normalized {variant_count} currency variants to ZAR")
    
    return df, variant_count


def generate_dq_report(dq_stats: dict, config: dict) -> str:
    """Generate DQ report JSON"""
    logger.info("Generating DQ report...")
    
    report = {
        "run_timestamp": datetime.utcnow().isoformat() + "Z",
        "stage": "2",
        "source_record_counts": dq_stats.get("source_counts", {}),
        "dq_issues": [],
        "gold_layer_record_counts": dq_stats.get("gold_counts", {}),
        "execution_duration_seconds": dq_stats.get("duration", 0)
    }
    
    # Add DQ issue details
    for issue_type, stats in dq_stats.get("issues", {}).items():
        if stats['records_affected'] > 0:
            total_source = dq_stats['source_counts'].get(stats.get('source_file', 'transactions_raw'), 1)
            report["dq_issues"].append({
                "issue_type": issue_type,
                "records_affected": stats['records_affected'],
                "percentage_of_total": round(stats['records_affected'] * 100.0 / total_source, 2),
                "handling_action": stats['handling_action'],
                "records_in_output": stats['records_in_output']
            })
    
    output_path = config.get('paths', {}).get('output', {}).get('bronze', '/data/output') + "/../dq_report.json"
    output_path = os.path.normpath(output_path)
    
    with open(output_path, 'w') as f:
        json.dump(report, f, indent=2)
    
    logger.info(f"DQ report written to {output_path}")
    
    return output_path
