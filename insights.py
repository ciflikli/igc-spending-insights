"""
Insights generation module for policymakers.

Builds summary statistics and generates LLM-based analytical summaries.
"""

import json
import logging
import os

import polars as pl
from anthropic import Anthropic, APIError

import config

logger = logging.getLogger(__name__)

TOP_N_ITEMS = 5  # Number of top items to include in summaries


def build_summary_stats(df: pl.DataFrame, anomalies: pl.DataFrame | None = None) -> dict:
    """Construct reusable summary statistics for reporting and prompting."""
    if len(df) == 0:
        raise ValueError("Cannot build statistics without transactions.")

    if anomalies is None:
        anomalies = pl.DataFrame()

    total_transactions = len(df)
    total_spend = float(df["amount"].sum())
    unique_suppliers = int(df["supplier"].n_unique())
    unique_departments = int(df["department"].n_unique())

    start_date = df["date"].min()
    end_date = df["date"].max()
    date_range = {
        "start": start_date.strftime("%Y-%m-%d"),
        "end": end_date.strftime("%Y-%m-%d"),
    }
    month_span = max(
        1,
        (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month) + 1,
    )

    average_value = float(total_spend / total_transactions) if total_transactions else 0.0
    median_value = float(df["amount"].median())
    amount_distribution = {
        "min": float(df["amount"].min()),
        "quartile_25": float(df["amount"].quantile(0.25)),
        "median": median_value,
        "quartile_75": float(df["amount"].quantile(0.75)),
        "percentile_95": float(df["amount"].quantile(0.95)),
        "max": float(df["amount"].max()),
    }

    negative_amounts = int((df["amount"] < 0).sum())
    zero_amounts = int((df["amount"] == 0).sum())

    def _with_shares(records: list[dict]) -> list[dict]:
        for record in records:
            spend_value = float(record.get("spend", 0.0))
            txn_value = int(record.get("transactions", 0))
            record["spend"] = spend_value
            record["transactions"] = txn_value
            record["pct_of_total_spend"] = (
                (spend_value / total_spend) * 100 if total_spend else 0.0
            )
            record["pct_of_total_transactions"] = (
                (txn_value / total_transactions) * 100 if total_transactions else 0.0
            )
        return records

    dept_records = (
        df.group_by("department")
        .agg(
            [
                pl.len().alias("transactions"),
                pl.col("amount").sum().alias("spend"),
                pl.col("supplier").n_unique().alias("unique_suppliers"),
            ]
        )
        .sort("spend", descending=True)
        .to_dicts()
    )
    top_departments = _with_shares(dept_records)[:TOP_N_ITEMS]

    category_records = (
        df.group_by("category")
        .agg(
            [
                pl.len().alias("transactions"),
                pl.col("amount").sum().alias("spend"),
            ]
        )
        .sort("spend", descending=True)
        .to_dicts()
    )
    top_categories = _with_shares(category_records)[:TOP_N_ITEMS]

    supplier_records = (
        df.group_by("supplier")
        .agg(
            [
                pl.len().alias("transactions"),
                pl.col("amount").sum().alias("spend"),
            ]
        )
        .sort("spend", descending=True)
        .to_dicts()
    )
    top_suppliers = _with_shares(supplier_records)[:TOP_N_ITEMS]

    monthly_records = (
        df.with_columns(pl.col("date").dt.strftime("%Y-%m").alias("period"))
        .group_by("period")
        .agg(
            [
                pl.len().alias("transactions"),
                pl.col("amount").sum().alias("spend"),
            ]
        )
        .sort("period")
        .to_dicts()
    )
    monthly_trends = _with_shares(monthly_records)

    uncategorised_count = int(df.filter(pl.col("category") == "Uncategorised").height)
    classification_summary = {
        "uncategorised_transactions": uncategorised_count,
        "uncategorised_pct": (uncategorised_count / total_transactions * 100)
        if total_transactions
        else 0.0,
    }

    anomaly_summary = {"total": len(anomalies)}
    if len(anomalies) > 0:
        anomaly_summary["by_type"] = [
            {"anomaly_type": row["anomaly_type"], "count": int(row["count"])}
            for row in anomalies.group_by("anomaly_type")
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
            .iter_rows(named=True)
        ]
        anomaly_summary["by_severity"] = [
            {"severity": row["severity"], "count": int(row["count"])}
            for row in anomalies.group_by("severity")
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
            .iter_rows(named=True)
        ]
        anomaly_summary["by_department"] = [
            {"department": row["department"], "count": int(row["count"])}
            for row in anomalies.group_by("department")
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
            .iter_rows(named=True)
        ]
        anomaly_summary["high_severity"] = int(
            anomalies.filter(pl.col("severity") == "high").height
        )
        anomaly_summary["medium_severity"] = int(
            anomalies.filter(pl.col("severity") == "medium").height
        )
        anomaly_summary["info_severity"] = int(
            anomalies.filter(pl.col("severity") == "info").height
        )
        anomaly_summary["examples"] = [
            {
                **row,
                "amount": float(row["amount"]),
            }
            for row in anomalies.sort("amount", descending=True)
            .select(
                [
                    "anomaly_type",
                    "severity",
                    "supplier",
                    "department",
                    "amount",
                    "details",
                ]
            )
            .head(TOP_N_ITEMS)
            .to_dicts()
        ]

    return {
        "overview": {
            "date_range": date_range,
            "months_covered": month_span,
            "total_transactions": total_transactions,
            "total_spend_gbp": total_spend,
            "average_transaction_value": average_value,
            "median_transaction_value": median_value,
            "unique_suppliers": unique_suppliers,
            "unique_departments": unique_departments,
            "average_monthly_spend_gbp": (
                total_spend / month_span if month_span else total_spend
            ),
            "negative_transactions": negative_amounts,
            "zero_value_transactions": zero_amounts,
            "largest_supplier_share_pct": top_suppliers[0]["pct_of_total_spend"]
            if top_suppliers
            else 0.0,
        },
        "amount_distribution": amount_distribution,
        "top_departments": top_departments,
        "top_categories": top_categories,
        "top_suppliers": top_suppliers,
        "monthly_trends": monthly_trends,
        "classification": classification_summary,
        "anomalies": anomaly_summary,
    }


def generate_summary(
    df: pl.DataFrame, anomalies: pl.DataFrame, stats: dict | None = None
) -> str:
    """Generate a spending summary via Anthropic's Claude models."""
    if len(df) == 0:
        raise ValueError("Cannot generate summary without transactions.")

    if anomalies is None:
        anomalies = pl.DataFrame()

    stats = stats or build_summary_stats(df, anomalies)

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY must be set in .env file to generate LLM summaries.")

    model = os.getenv("ANTHROPIC_MODEL", config.DEFAULT_MODEL)
    client = Anthropic(api_key=api_key)

    payload = json.dumps(stats, indent=2)

    prompt = (
        "You are an expert public finance analyst for the UK Cabinet Office. "
        "Prepare a concise briefing for HM Treasury using only the structured statistics provided. "
        "Highlight significant spending trends, departmental movements, supplier concentration, "
        "and the most pressing anomalies or risks. Respond with clear headings and short paragraphs "
        "suitable for decision-makers.\n\n"
        f"Structured statistics:\n{payload}"
    )

    logger.info("Requesting LLM-generated summary via %s", model)

    try:
        message = client.messages.create(
            model=model,
            max_tokens=config.LLM_MAX_TOKENS,
            temperature=config.LLM_TEMPERATURE,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                    ],
                },
            ],
        )
    except APIError as e:
        logger.error("API call failed: %s", e, exc_info=True)
        raise RuntimeError(f"Failed to generate LLM summary: {e}") from e

    return message.content[0].text.strip()
