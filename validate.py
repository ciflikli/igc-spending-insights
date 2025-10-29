"""
Data validation and quality checking module.

Validates ingested data for completeness, consistency, and quality issues.
"""

import logging
import polars as pl

logger = logging.getLogger(__name__)


def validate_data(df: pl.DataFrame) -> dict:
    """
    Validate data quality and return summary statistics.
    
    Args:
        df: Standardised DataFrame from ingest.py
        
    Returns:
        Dictionary containing validation metrics and quality issues
    """
    validation_results = {
        'total_rows': len(df),
        'issues': [],
        'warnings': [],
        'quality_metrics': {}
    }
    
    logger.info(f"Validating {len(df):,} transactions")
    
    # Check required columns exist
    required_cols = [
        'department', 'entity', 'date', 'month', 'expense_type', 
        'expense_area', 'supplier', 'amount', 'description', 
        'transaction_number', 'postcode', 'source_file'
    ]
    
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        validation_results['issues'].append(f"Missing columns: {missing_cols}")
        logger.error(f"Missing required columns: {missing_cols}")
        return validation_results
    
    # Check for null values in critical fields
    critical_fields = ['department', 'date', 'amount', 'supplier']
    for field in critical_fields:
        null_count = df[field].is_null().sum()
        if null_count > 0:
            pct = (null_count / len(df)) * 100
            validation_results['issues'].append(
                f"{field}: {null_count:,} null values ({pct:.2f}%)"
            )
            logger.error(f"{field} has {null_count:,} null values")
    
    # Check for empty strings in important fields
    string_fields = ['department', 'expense_type', 'supplier']
    for field in string_fields:
        empty_count = (df[field] == '').sum()
        if empty_count > 0:
            pct = (empty_count / len(df)) * 100
            validation_results['warnings'].append(
                f"{field}: {empty_count:,} empty strings ({pct:.2f}%)"
            )
            logger.warning(f"{field} has {empty_count:,} empty strings")
    
    # Check amount distribution
    amounts = df['amount']
    negative_count = (amounts < 0).sum()
    zero_count = (amounts == 0).sum()
    
    if negative_count > 0:
        pct = (negative_count / len(df)) * 100
        validation_results['warnings'].append(
            f"Negative amounts: {negative_count:,} ({pct:.2f}%) - likely refunds"
        )
        logger.warning(f"{negative_count:,} negative amounts (refunds)")
    
    if zero_count > 0:
        pct = (zero_count / len(df)) * 100
        validation_results['warnings'].append(
            f"Zero amounts: {zero_count:,} ({pct:.2f}%)"
        )
        logger.warning(f"{zero_count:,} zero amounts")
    
    validation_results['quality_metrics']['negative_amounts'] = int(negative_count)
    validation_results['quality_metrics']['zero_amounts'] = int(zero_count)
    
    # Check date range
    date_range = {
        'min': df['date'].min().strftime("%Y-%m-%d"),
        'max': df['date'].max().strftime("%Y-%m-%d")
    }
    validation_results['quality_metrics']['date_range'] = date_range
    logger.info(f"Date range: {date_range['min']} to {date_range['max']}")
    
    # Check department distribution
    dept_counts = df.group_by('department').agg(pl.len().alias('count')).sort('count', descending=True)
    validation_results['quality_metrics']['departments'] = {
        row['department']: int(row['count']) 
        for row in dept_counts.iter_rows(named=True)
    }
    
    for row in dept_counts.iter_rows(named=True):
        logger.info(f"  {row['department']}: {row['count']:,} transactions")
    
    # Check for duplicate transaction numbers
    txn_counts = df.group_by('transaction_number').agg(pl.len().alias('count'))
    duplicates = txn_counts.filter(pl.col('count') > 1)
    
    if len(duplicates) > 0:
        dup_txn_count = duplicates['count'].sum()
        validation_results['warnings'].append(
            f"Duplicate transaction numbers: {len(duplicates):,} IDs with {dup_txn_count:,} total occurrences"
        )
        logger.warning(
            f"{len(duplicates):,} transaction IDs appear multiple times "
            f"(total {dup_txn_count:,} occurrences) - likely legitimate recurring payments"
        )
    
    # Description quality check
    desc_null = df['description'].is_null().sum()
    desc_empty = (df['description'] == '').sum()
    desc_short = (df['description'].str.len_chars() < 10).sum()
    desc_useful = len(df) - desc_null - desc_empty - desc_short
    
    validation_results['quality_metrics']['description_quality'] = {
        'null': int(desc_null),
        'empty': int(desc_empty),
        'too_short': int(desc_short),
        'useful': int(desc_useful),
        'useful_pct': (desc_useful / len(df)) * 100
    }
    
    logger.info(
        f"Description quality: {desc_useful:,}/{len(df):,} useful "
        f"({validation_results['quality_metrics']['description_quality']['useful_pct']:.1f}%)"
    )
    
    # Supplier normalisation check
    unique_suppliers = df['supplier'].n_unique()
    validation_results['quality_metrics']['unique_suppliers'] = int(unique_suppliers)
    logger.info(f"Unique suppliers: {unique_suppliers:,}")
    
    # Amount statistics
    validation_results['quality_metrics']['amount_stats'] = {
        'min': float(amounts.min()),
        'q25': float(amounts.quantile(0.25)),
        'median': float(amounts.median()),
        'q75': float(amounts.quantile(0.75)),
        'q95': float(amounts.quantile(0.95)),
        'max': float(amounts.max()),
        'total': float(amounts.sum())
    }
    
    logger.info(
        f"Amount range: £{amounts.min():,.2f} to £{amounts.max():,.2f} "
        f"(median: £{amounts.median():,.2f})"
    )
    
    if not validation_results['issues']:
        logger.info("All validation checks passed")
    else:
        logger.error(f"Found {len(validation_results['issues'])} critical issues")
    
    if validation_results['warnings']:
        logger.warning(f"Found {len(validation_results['warnings'])} warnings")
    
    return validation_results
