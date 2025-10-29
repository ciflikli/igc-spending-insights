# UK Government Spending Insights Tool

An AI-assisted tool that analyses 'Spend over £25,000' datasets from UK government departments to identify inefficiencies, duplicate spending, and procurement risks.

## Features

- Data ingestion and standardisation: Handles inconsistent CSV schemas across three departments  
- Automated classification: >93% accuracy with tiered cascade (no LLM for MVP)  
- Anomaly detection: Identifies high payments, duplicates, and supplier concentration  
- Policy insights: Generates plain-language summaries for policymakers using LLM

### 1. Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Data

Place CSV files in `data/` directory organised by department ([HMRC](https://www.gov.uk/government/collections/spending-over-25-000), [Home Office](https://www.gov.uk/government/publications/home-office-spending-over-25000-2025) and [DfT](https://www.data.gov.uk/dataset/804be0c6-ae01-4899-bbb9-2317dff1a4f9/financial-transactions-data-dft)) in the following format:

```text
data/
├── HMRC/
│   ├── HMRC Spending Over 25000 Jan 2025.csv
│   └── ...
├── Home Office/
│   ├── Home Office Spending Over 25000 Jan 2025.csv
│   └── ...
└── DfT/
    ├── DfT Spending Over 25000 Jan 2025.csv
    └── ...
```

### 3. Analysis

```bash
python main.py
```

### 4. Results

Outputs saved to `output/` directory:
- `classified_transactions.csv` - All transactions with assigned categories
- `anomalies.csv` - Detected issues with severity levels
- `summary.txt` - Executive summary report
- `stats.json` - Rich, nested summary metrics (overview, top entities, anomalies, trends)

## Summary Stats

```text
Total Transactions: 22,738
Total Spend: £12.2 billion
Anomalies Detected: 1,606
```

### Classification

1. **Tier 0**: Direct expense type mapping; 74.8% (17,004 transactions)
2. **Tier 1**: Keyword matching on `description` field; 15.4% (3,512 transactions)
3. **Tier 2**: Keyword matching on `expense_type` field; 3.1% (694 transactions)

**Overall**: 93.3% classified without LLM

**Uncategorised**: 6.7% (1,528 transactions); would be send to LLM in production

### Anomaly Detection

1. **High Payments**: Department-specific 95th percentile thresholds
2. **Duplicate Patterns**: Same supplier + amount within 7 days
3. **Supplier Concentration**: >15% spend or >10% transactions

### Key Findings

- **Network Rail** accounts for 14.1% of total spend (£1.72B)
- **Clearsprings Ready Homes** represents 16.4% of Home Office spend
- 1,152 high-value payments exceed department thresholds
- 450 potential duplicate payment patterns identified
- 4 suppliers show concerning concentration levels

### Top Spending Categories

1. **Grants**: £3.41B (27.9%)
2. **Operations**: £2.65B (21.7%)
3. **IT**: £2.65B (21.7%)
4. **Construction**: £1.80B (14.7%)
5. **HR/Staffing**: £648M (5.3%)

## Architecture

### Module Structure

```
config.py          - Configuration, thresholds, keywords, validation
ingest.py          - CSV loading and schema standardisation  
validate.py        - Data quality checks and profiling
classify.py        - 4-tier payment classification cascade
anomalies.py       - Pattern detection (high payments, duplicates, concentration)
insights.py        - LLM summary generation for policymakers
main.py            - Pipeline orchestration
```

### Dependencies

- `polars>=1.13.1` - Fast DataFrame library
- `anthropic>=0.39.0` - LLM API
- `rich>=13.9.4` - Terminal UI
- `httpx<0.27.2` - Anthropic API dependency (breaking change >0.28)

- `ANTHROPIC_API_KEY` in `.env`

### Known Issues
- No automated tests or coverage
- Not typed throughout
