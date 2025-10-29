"""
Payment classification module.

Classifies government spending transactions into categories using a 3-tier cascade:
- Tier 0: Direct expense type mapping
- Tier 1: Keyword matching on `description` field
- Tier 2: Keyword matching on `expense_type` field
"""

import logging

import polars as pl

import config

logger = logging.getLogger(__name__)


def classify_payments(
    df: pl.DataFrame, 
    use_direct_map: bool = True
) -> pl.DataFrame:
    """
    Classify payments into categories using cascading classification tiers.
    
    Args:
        df: Standardised DataFrame from ingest.py
        use_direct_map: If True, use Tier 0 direct mapping (recommended)
        
    Required columns in df:
        - department: str (must match keys in DIRECT_EXPENSE_TYPE_MAPPING)
        - expense_type: str (can be empty/null)
        - description: str (can be empty/null)
        - supplier: str
        
    Returns:
        DataFrame with added column:
        - category: str (value from CATEGORIES or 'Uncategorised')
    """
    logger.info(f"Starting classification of {len(df):,} transactions")
    
    result = df.with_columns(pl.lit('').alias('category'))
    
    stats = {
        'total': len(df),
        'tier0': 0,
        'tier1': 0,
        'tier2': 0,
        'uncategorised': 0
    }
    
    # Tier 0: Direct Expense Type Mapping
    if use_direct_map:
        logger.info("Tier 0: Direct expense type mapping...")
        
        tier0_conditions = []
        for dept in config.DIRECT_EXPENSE_TYPE_MAPPING.keys():
            mapping = config.DIRECT_EXPENSE_TYPE_MAPPING[dept]
            
            for expense_type, category in mapping.items():
                condition = (
                    (pl.col('department') == dept) & 
                    (pl.col('expense_type') == expense_type) &
                    (pl.col('category') == '')
                )
                tier0_conditions.append((condition, category))
        
        expr = pl.col('category')
        for condition, category in tier0_conditions:
            expr = pl.when(condition).then(pl.lit(category)).otherwise(expr)
        
        result = result.with_columns(expr.alias('category'))
        
        stats['tier0'] = (result['category'] != '').sum()
        logger.info(f"  Tier 0 classified: {stats['tier0']:,} ({stats['tier0']/stats['total']*100:.1f}%)")
    
    # Tier 1: Keyword Matching on Description
    logger.info("Tier 1: Keyword matching on description...")
    
    for category, keywords in config.CATEGORY_KEYWORDS.items():
        # Create regex pattern: (keyword1|keyword2|...) using word boundaries to avoid partial matches
        pattern = '|'.join([f'(?i){kw}' for kw in keywords])  # (?i) for case-insensitive
        
        condition = (
            (pl.col('category') == '') &  # Not yet classified
            (pl.col('description').str.contains(pattern))
        )
        
        result = result.with_columns(
            pl.when(condition)
            .then(pl.lit(category))
            .otherwise(pl.col('category'))
            .alias('category')
        )
    
    tier1_total = (result['category'] != '').sum()
    stats['tier1'] = tier1_total - stats['tier0']
    logger.info(f"  Tier 1 classified: {stats['tier1']:,} (cumulative: {tier1_total:,}, {tier1_total/stats['total']*100:.1f}%)")
    
    # Tier 2: Keyword Matching on Expense Type
    logger.info("Tier 2: Keyword matching on expense_type...")
    
    for category, keywords in config.CATEGORY_KEYWORDS.items():
        pattern = '|'.join([f'(?i){kw}' for kw in keywords])
        
        condition = (
            (pl.col('category') == '') &
            (pl.col('expense_type').str.contains(pattern))
        )
        
        result = result.with_columns(
            pl.when(condition)
            .then(pl.lit(category))
            .otherwise(pl.col('category'))
            .alias('category')
        )
    
    tier2_total = (result['category'] != '').sum()
    stats['tier2'] = tier2_total - tier1_total
    logger.info(f"  Tier 2 classified: {stats['tier2']:,} (cumulative: {tier2_total:,}, {tier2_total/stats['total']*100:.1f}%)")
    
    # Mark remaining as Uncategorised
    result = result.with_columns(
        pl.when(pl.col('category') == '')
        .then(pl.lit('Uncategorised'))
        .otherwise(pl.col('category'))
        .alias('category')
    )
    
    stats['uncategorised'] = (result['category'] == 'Uncategorised').sum()
    
    logger.info(f"\nClassification Summary:")
    logger.info(f"  Total: {stats['total']:,}")
    logger.info(f"  Tier 0 (Direct mapping): {stats['tier0']:,} ({stats['tier0']/stats['total']*100:.1f}%)")
    logger.info(f"  Tier 1 (Description keywords): {stats['tier1']:,} ({stats['tier1']/stats['total']*100:.1f}%)")
    logger.info(f"  Tier 2 (Expense type keywords): {stats['tier2']:,} ({stats['tier2']/stats['total']*100:.1f}%)")
    logger.info(f"  Uncategorised: {stats['uncategorised']:,} ({stats['uncategorised']/stats['total']*100:.1f}%)")
    
    logger.info(f"\nCategory Distribution:")
    category_counts = result.group_by('category').agg(pl.len().alias('count')).sort('count', descending=True)
    for row in category_counts.iter_rows(named=True):
        pct = (row['count'] / stats['total']) * 100
        logger.info(f"  {row['category']}: {row['count']:,} ({pct:.1f}%)")
    
    return result
