"""
Anomaly detection module.

Identifies potential inefficiencies, risks, or unusual patterns in spending:
1. High payments (department-specific thresholds)
2. Duplicate patterns (same supplier + amount within time window)
3. Supplier concentration (spend % and transaction %)
"""

import logging
import polars as pl
import config

logger = logging.getLogger(__name__)


def _empty_anomaly_df() -> pl.DataFrame:
    """
    Return empty anomaly DataFrame with correct schema.
    
    This ensures consistent schema when no anomalies are found.
    """
    return pl.DataFrame({
        'anomaly_type': pl.Series([], dtype=pl.Utf8),
        'severity': pl.Series([], dtype=pl.Utf8),
        'department': pl.Series([], dtype=pl.Utf8),
        'supplier': pl.Series([], dtype=pl.Utf8),
        'details': pl.Series([], dtype=pl.Utf8),
        'amount': pl.Series([], dtype=pl.Float64),
        'count': pl.Series([], dtype=pl.Int64)
    })


def detect_anomalies(df: pl.DataFrame) -> pl.DataFrame:
    """
    Detect spending anomalies across multiple dimensions.
    
    Args:
        df: Classified DataFrame with columns:
            - department: str
            - supplier: str (normalised)
            - amount: float
            - date: pl.Date
            - expense_type: str
            
    Returns:
        DataFrame with columns:
        - anomaly_type: str
        - severity: str ('info', 'medium', 'high')
        - department: str
        - supplier: str
        - details: str (human-readable description)
        - amount: float (total or example amount)
        - count: int (number of occurrences)
    """
    logger.info(f"Detecting anomalies in {len(df):,} transactions")
    
    all_anomalies = []
    
    # High Payments
    logger.info("Detecting high payments...")
    high_payments = detect_high_payments(df)
    all_anomalies.append(high_payments)
    logger.info(f"  Found {len(high_payments)} high payment anomalies")
    
    # Duplicate Patterns
    logger.info("Detecting duplicate patterns...")
    duplicates = detect_duplicate_patterns(df)
    all_anomalies.append(duplicates)
    logger.info(f"  Found {len(duplicates)} duplicate pattern anomalies")
    
    # Supplier Concentration
    logger.info("Detecting supplier concentration...")
    concentration = detect_supplier_concentration(df)
    all_anomalies.append(concentration)
    logger.info(f"  Found {len(concentration)} supplier concentration anomalies")
    
    if all_anomalies:
        combined = pl.concat([a for a in all_anomalies if len(a) > 0], how="vertical")
        logger.info(f"Total anomalies detected: {len(combined)}")
        return combined
    else:
        return _empty_anomaly_df()


def detect_high_payments(df: pl.DataFrame) -> pl.DataFrame:
    """
    Detect payments above department-specific 95th percentile thresholds.
    
    Returns anomalies with severity='high'
    """
    anomalies = []
    
    for dept, threshold in config.ANOMALY_THRESHOLDS['high_payment'].items():
        high = df.filter(
            (pl.col('department') == dept) & 
            (pl.col('amount') > threshold)
        )
        
        for row in high.iter_rows(named=True):
            anomalies.append({
                'anomaly_type': 'high_payment',
                'severity': 'high',
                'department': row['department'],
                'supplier': row['supplier'],
                'details': f"Payment of £{row['amount']:,.0f} exceeds £{threshold:,.0f} threshold",
                'amount': row['amount'],
                'count': 1
            })
    
    if anomalies:
        return pl.DataFrame(anomalies)
    else:
        return _empty_anomaly_df()


def detect_duplicate_patterns(df: pl.DataFrame) -> pl.DataFrame:
    """
    Detect potential duplicate payments: same supplier + amount within 7-day window.
    
    Severity:
    - 'medium' if 2-3 occurrences
    - 'high' if 4+ occurrences
    """
    anomalies = []
    
    grouped = df.group_by(['department', 'supplier', 'amount']).agg([
        pl.col('date').sort(),
        pl.len().alias('count')
    ])

    potential_dupes = grouped.filter(pl.col('count') >= 2)
    
    window_days = config.ANOMALY_THRESHOLDS['duplicate_window_days']
    
    for row in potential_dupes.iter_rows(named=True):
        dates = row['date']
        
        # Check if any consecutive dates are within window_days
        # More efficient: sorted dates, only check adjacent pairs
        has_close_dates = False
        for i in range(len(dates) - 1):
            days_apart = (dates[i + 1] - dates[i]).days
            if days_apart <= window_days:
                has_close_dates = True
                break
        
        if has_close_dates:
            count = row['count']
            severity = 'high' if count >= 4 else 'medium'
            
            anomalies.append({
                'anomaly_type': 'duplicate_pattern',
                'severity': severity,
                'department': row['department'],
                'supplier': row['supplier'],
                'details': f"£{row['amount']:,.0f} paid {count} times within {window_days} days",
                'amount': row['amount'],
                'count': count
            })
    
    if anomalies:
        return pl.DataFrame(anomalies)
    else:
        return _empty_anomaly_df()


def detect_supplier_concentration(df: pl.DataFrame) -> pl.DataFrame:
    """
    Detect supplier concentration by both spend and transaction count.
    
    Returns DataFrame with both metric types:
    - anomaly_type='supplier_concentration_spend' for spend % (severity='high')
    - anomaly_type='supplier_concentration_txn' for transaction % (severity='medium')
    """
    results = []
    
    # Spend Concentration (>15%, arbitrary)
    dept_spend = df.group_by('department').agg(
        pl.col('amount').sum().alias('dept_total')
    )
    
    supplier_spend = df.group_by(['department', 'supplier']).agg([
        pl.col('amount').sum().alias('supplier_total'),
        pl.len().alias('txn_count')
    ]).join(dept_spend, on='department')
    
    threshold_spend = config.ANOMALY_THRESHOLDS['concentration_threshold_spend']
    high_spend = supplier_spend.filter(
        (pl.col('supplier_total') / pl.col('dept_total')) > threshold_spend
    )
    
    for row in high_spend.iter_rows(named=True):
        pct = (row['supplier_total'] / row['dept_total']) * 100
        results.append({
            'anomaly_type': 'supplier_concentration_spend',
            'severity': 'high',
            'department': row['department'],
            'supplier': row['supplier'],
            'details': f"{pct:.1f}% of department total spend (>{threshold_spend*100}% threshold)",
            'amount': row['supplier_total'],
            'count': row['txn_count']
        })
    
    # Transaction Concentration (>10%, arbitrary)
    dept_txns = df.group_by('department').agg(
        pl.len().alias('dept_txn_total')
    )
    
    supplier_txns = df.group_by(['department', 'supplier']).agg([
        pl.len().alias('supplier_txn_count'),
        pl.col('amount').sum().alias('supplier_amount')
    ]).join(dept_txns, on='department')
    
    threshold_txn = config.ANOMALY_THRESHOLDS['concentration_threshold_txn']
    high_txn = supplier_txns.filter(
        (pl.col('supplier_txn_count') / pl.col('dept_txn_total')) > threshold_txn
    )
    
    for row in high_txn.iter_rows(named=True):
        pct = (row['supplier_txn_count'] / row['dept_txn_total']) * 100
        results.append({
            'anomaly_type': 'supplier_concentration_txn',
            'severity': 'medium',
            'department': row['department'],
            'supplier': row['supplier'],
            'details': f"{pct:.1f}% of department transactions (>{threshold_txn*100}% threshold)",
            'amount': row['supplier_amount'],
            'count': row['supplier_txn_count']
        })
    
    if results:
        return pl.DataFrame(results)
    else:
        return _empty_anomaly_df()

