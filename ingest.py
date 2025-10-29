"""
Data ingestion and standardisation module.

Loads raw CSV files from government departments and standardises them
into a unified schema for analysis.
"""

import logging
from pathlib import Path
import polars as pl
import config

logger = logging.getLogger(__name__)


def load_and_standardize(data_dir: str) -> pl.DataFrame:
    """
    Load and standardise spending data from all departments.
    
    Args:
        data_dir: Path to directory containing department folders
        
    Returns:
        DataFrame with standardised schema containing:
        - department: str (normalised, e.g., "HMRC", "Home Office", "DfT")
        - entity: str
        - date: pl.Date
        - month: str (e.g., "2025-01", "2025-02")
        - expense_type: str (cleaned, may be empty)
        - expense_area: str
        - supplier: str (normalised: uppercase, stripped)
        - amount: float (positive or negative)
        - description: str (may be empty, Home Office uses expense_type as fallback)
        - transaction_number: str
        - postcode: str (may be empty)
        - source_file: str (filename)
    """
    data_path = Path(data_dir)
    all_dfs = []
    
    dept_map = {
        'HMRC': 'HMRC',
        'Home Office': 'Home Office',
        'DfT': 'DfT'
    }
    
    csv_files = list(data_path.rglob("*.csv"))
    logger.info(f"Found {len(csv_files)} CSV files in {data_dir}")
    
    for file in csv_files:
        dept_folder = file.parent.name
        department = dept_map.get(dept_folder, dept_folder)
        
        if department not in config.SCHEMA_MAPPING:
            logger.warning(f"Skipping {file.name}: Unknown department '{department}'")
            continue
        
        logger.info(f"Processing {department}: {file.name}")
        
        schema = config.SCHEMA_MAPPING[department]
        
        try:
            df = pl.read_csv(file, encoding="utf8-lossy")
            
            missing_cols = []
            for std_col, raw_col in schema.items():
                if raw_col is not None and raw_col not in df.columns:
                    missing_cols.append(raw_col)
            
            if missing_cols:
                logger.error(f"Missing columns in {file.name}: {missing_cols}")
                continue
            
            cols = []
            
            cols.append(pl.lit(department).alias('department'))
            
            for std_col in ['entity', 'expense_area', 'transaction_number']:
                raw_col = schema[std_col]
                if raw_col:
                    cols.append(pl.col(raw_col).cast(pl.String).fill_null('').alias(std_col))
                else:
                    cols.append(pl.lit('').alias(std_col))
            
            raw_date_col = schema['date']
            cols.append(
                pl.col(raw_date_col).str.to_date("%d/%m/%Y", strict=False).alias('date')
            )
            
            raw_amount_col = schema['amount']
            if raw_amount_col in df.columns and df[raw_amount_col].dtype == pl.String:
                cols.append(
                    pl.col(raw_amount_col)
                    .str.replace_all(r'[£,"]', '')
                    .cast(pl.Float64, strict=False)
                    .alias('amount')
                )
            elif raw_amount_col in df.columns:
                cols.append(
                    pl.col(raw_amount_col).cast(pl.Float64, strict=False).alias('amount')
                )
            else:
                logger.error(f"Amount column '{raw_amount_col}' not found in {file.name}")
                continue
            
            raw_expense_type = schema['expense_type']
            cols.append(
                pl.col(raw_expense_type)
                .fill_null('')
                .str.strip_chars()
                .str.replace('#', '')
                .alias('expense_type')
            )
            
            raw_supplier = schema['supplier']
            cols.append(
                pl.col(raw_supplier)
                .fill_null('')
                .str.strip_chars()
                .str.to_uppercase()
                .alias('supplier')
            )
            
            raw_desc = schema['description']
            if raw_desc:
                cols.append(
                    pl.col(raw_desc)
                    .fill_null('')
                    .str.strip_chars()
                    .str.replace('#', '')
                    .alias('description')
                )
            else:
                cols.append(
                    pl.col(raw_expense_type)
                    .fill_null('')
                    .str.strip_chars()
                    .str.replace('#', '')
                    .alias('description')
                )
            
            raw_postcode = schema['postcode']
            if raw_postcode:
                cols.append(pl.col(raw_postcode).fill_null('').alias('postcode'))
            else:
                cols.append(pl.lit('').alias('postcode'))
            
            cols.append(pl.lit(file.name).alias('source_file'))
            
            try:
                standardised = df.select(cols)
            except Exception as e:
                logger.error(f"Failed to create standardised DataFrame for {file.name}: {e}")
                continue
            
            standardised = standardised.with_columns([
                pl.col('date').dt.strftime("%Y-%m").alias('month')
            ])
            
            before_filter = len(standardised)
            standardised = standardised.filter(
                pl.col('amount').is_not_null() & 
                pl.col('date').is_not_null()
            )
            after_filter = len(standardised)
            
            if before_filter > after_filter:
                logger.warning(
                    f"Filtered {before_filter - after_filter} rows with null amount/date from {file.name}"
                )
            
            logger.info(f"✅ Loaded {len(standardised):,} transactions from {file.name}")
            all_dfs.append(standardised)
            
        except Exception as e:
            logger.error(f"Failed to process {file.name}: {e}", exc_info=True)
            continue
    
    if not all_dfs:
        logger.error("No data loaded from any files!")
        return None
    
    combined = pl.concat(all_dfs, how="diagonal")
    logger.info(f"📊 Total loaded: {len(combined):,} transactions from {len(all_dfs)} files")
    
    return combined

