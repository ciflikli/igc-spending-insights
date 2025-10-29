"""
Main pipeline for UK Government Spending Insights Tool.

Orchestrates the full workflow:
1. Configuration validation
2. Data ingestion and standardisation
3. Data quality validation
4. Payment classification
5. Anomaly detection
6. Insights generation and reporting
"""

import json
import logging
import sys
from pathlib import Path

import polars as pl
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

import anomalies
import classify
import config
import insights
import ingest
import validate


def main():
    """
    Main entry point for the spending insights tool.
    """
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(config.LOG_FILE),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger(__name__)

    console = Console()

    # Load environment variables before any API usage
    config.load_environment_variables()

    console.print("\n[bold blue]" + "=" * 80 + "[/]")
    console.print("[bold blue]UK GOVERNMENT SPENDING INSIGHTS TOOL[/]")
    console.print("[bold blue]Cabinet Office - Transparency Analysis Prototype[/]")
    console.print("[bold blue]" + "=" * 80 + "[/]")
    

    # STEP 1: Validate Configuration

    console.print("\n🔧 [bold]Step 1: Validating configuration...[/]")
    try:
        config.validate_config()
        console.print("   Configuration valid")
    except ValueError as e:
        console.print(f"  [bold red]Configuration error: {e}[/]")
        logger.error(f"Configuration validation failed: {e}")
        return 1
    

    # STEP 2: Data Ingestion
    console.print("\n [bold]Step 2: Loading and standardising data...[/]")
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console
    ) as progress:
        task = progress.add_task("Ingesting CSV files...", total=None)
        
        df = ingest.load_and_standardize("data/")
        
        if df is None or len(df) == 0:
            console.print("  [bold red]No data loaded[/]")
            logger.error("Data ingestion failed")
            return 1
        
        progress.update(task, completed=True)
    
    console.print(f"   Loaded [bold green]{len(df):,}[/] transactions")
    

    # STEP 3: Data Validation
    console.print("\n [bold]Step 3: Validating data quality...[/]")
    
    validation_results = validate.validate_data(df)
    
    if validation_results['issues']:
        console.print(f"   [yellow]{len(validation_results['issues'])} critical issues found[/]")
        for issue in validation_results['issues'][:3]:
            console.print(f"       • {issue}")
    else:
        console.print("    No critical issues")
    
    if validation_results['warnings']:
        console.print(f"    [dim]{len(validation_results['warnings'])} warnings[/]")
    

    # STEP 4: Classification
    console.print("\n  [bold]Step 4: Classifying transactions...[/]")
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console
    ) as progress:
        task = progress.add_task("Running classification cascade...", total=None)
        
        classified_df = classify.classify_payments(df, use_direct_map=True)
        
        progress.update(task, completed=True)
    
    # Show classification summary table
    cat_summary = classified_df.group_by('category').agg([
        pl.len().alias('count'),
        pl.col('amount').sum().alias('total_spend')
    ]).sort('total_spend', descending=True)
    
    table = Table(title="Category Distribution", show_header=True, header_style="bold magenta")
    table.add_column("Category", style="cyan")
    table.add_column("Transactions", justify="right")
    table.add_column("Total Spend (£)", justify="right")
    table.add_column("% of Total", justify="right")
    
    total_spend = float(classified_df['amount'].sum())
    for row in cat_summary.iter_rows(named=True):
        pct = (row['total_spend'] / total_spend) * 100
        table.add_row(
            row['category'],
            f"{row['count']:,}",
            f"{row['total_spend']:,.0f}",
            f"{pct:.1f}%"
        )
    
    console.print(table)
    

    # STEP 5: Anomaly Detection
    console.print("\n [bold]Step 5: Detecting anomalies...[/]")
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console
    ) as progress:
        task = progress.add_task("Scanning for patterns...", total=None)
        
        anomalies_df = anomalies.detect_anomalies(classified_df)
        
        progress.update(task, completed=True)
    
    console.print(f"   Found [bold red]{len(anomalies_df):,}[/] anomalies")
    
    # Show anomaly summary
    if len(anomalies_df) > 0:
        anom_summary = anomalies_df.group_by(['anomaly_type', 'severity']).agg(
            pl.len().alias('count')
        ).sort('count', descending=True)
        
        for row in anom_summary.iter_rows(named=True):
            severity_color = "red" if row['severity'] == 'high' else "yellow"
            console.print(
                f"       • {row['anomaly_type']}: {row['count']:,} "
                f"[{severity_color}]({row['severity']})[/]"
            )
    

    # STEP 6: Generate Insights
    console.print("\n [bold]Step 6: Generating insights report...[/]")
    
    stats = insights.build_summary_stats(classified_df, anomalies_df)
    summary_text = insights.generate_summary(classified_df, anomalies_df, stats=stats)
    
    console.print("   Report generated")
    

    # STEP 7: Save Outputs
    console.print("\n [bold]Step 7: Saving outputs...[/]")
    
    output_dir = config.OUTPUT_DIR
    output_dir.mkdir(exist_ok=True)
    
    classified_output = output_dir / "classified_transactions.csv"
    classified_df.write_csv(classified_output)
    console.print(f"   Saved {classified_output}")
    
    if len(anomalies_df) > 0:
        anomalies_output = output_dir / "anomalies.csv"
        anomalies_df.write_csv(anomalies_output)
        console.print(f"   Saved {anomalies_output}")
    
    summary_output = output_dir / "summary.txt"
    summary_output.write_text(summary_text)
    console.print(f"   Saved {summary_output}")
    
    stats_output = output_dir / "stats.json"
    stats_output.write_text(json.dumps(stats, indent=2))
    console.print(f"   Saved {stats_output}")
    

    # Final Summary
    console.print("\n" + "=" * 80)
    console.print("[bold green] ANALYSIS COMPLETE[/]")
    console.print("=" * 80)
    console.print(f"\n Analysed [bold]{len(classified_df):,}[/] transactions")
    console.print(f" Total spend: [bold]£{float(classified_df['amount'].sum()):,.0f}[/]")
    console.print(f" Anomalies detected: [bold]{len(anomalies_df):,}[/]")
    console.print(f"\n Outputs saved to: [cyan]{output_dir.absolute()}[/]\n")
    
    # Print summary to console
    console.print("\n" + summary_text)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
