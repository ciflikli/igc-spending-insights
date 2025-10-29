"""
Configuration module for UK Government Spending Insights Tool.

Contains all constants, thresholds, keywords, and validation logic.
"""

from pathlib import Path
import os


def load_environment_variables(path: str | Path = ".env") -> None:
    """Load key=value pairs from a .env file into os.environ if not already set."""

    env_path = Path(path)
    if not env_path.exists():
        return

    for raw_line in env_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")

        if key and key not in os.environ:
            os.environ[key] = value

# Schema mappings - actual column mappings from CSV files

SCHEMA_MAPPING = {
    'HMRC': {
        'department': 'Department family',
        'entity': 'Entity',
        'date': 'Date',
        'expense_type': 'Expense type',
        'expense_area': 'Expense area',
        'supplier': 'Supplier',
        'transaction_number': 'Transaction number',
        'amount': 'Amount',
        'description': 'Description',
        'postcode': 'Supplier Postcode'
    },
    'Home Office': {
        'department': 'Department',
        'entity': 'Entity',
        'date': 'Date',
        'expense_type': 'Expense Type',
        'expense_area': 'Expense Area',
        'supplier': 'Supplier',
        'transaction_number': 'Transaction Number',
        'amount': 'Amount',
        'description': None,  # No 'description' field - use 'expense_type' as fallback
        'postcode': None
    },
    'DfT': {
        'department': 'Department Family',
        'entity': 'Entity',
        'date': 'Date',
        'expense_type': 'Expense Type',
        'expense_area': 'Expense Area',
        'supplier': 'Supplier',
        'transaction_number': 'Transaction No',
        'amount': ' £ ',  # column name has spaces and £ symbol - needs cleaning
        'description': 'Item Text',
        'postcode': 'Postal Code'
    }
}

# Classification categories

# Single source of truth for categories (8 categories based on exploratory data analysis)
CATEGORIES = [
    'IT',
    'Consultancy',
    'Construction',
    'Operations',
    'Legal',
    'HR/Staffing',
    'Grants',
    'Administrative'
]

# Keywords for rule-based classification, based on analysis of 465 unique expense types across 3 departments

CATEGORY_KEYWORDS = {
    'IT': [
        # Core IT
        'software', 'IT', 'licence', 'license', 'system', 'digital', 'cloud',
        'hosting', 'server', 'hardware', 'telephony', 'computer', 'laptop',
        'ICT', 'infrastructure', 'network', 'data centre', 'data center',
        # Specific patterns from data
        'IT RUN', 'END USER COMPUTER', 'NETWORKING', 'DATA CHARGES',
        'application licensing', 'technical service', 'connectivity'
    ],
    'Consultancy': [
        'consultancy', 'consulting', 'consultant', 'advisory', 'professional services',
        'technical', 'organisational', 'organizational', 'market', 'research',
        'audit', 'accounting', 'finance', 'tax', 'forensic',
        # Specific patterns
        'HIRE OF CONSULTANTS', 'MANAGEMENT CONSULT', 'user research'
    ],
    'Construction': [
        'construction', 'building', 'infrastructure', 'AUC',  # Assets Under Construction
        'capital', 'renewal', 'maintenance', 'estate', 'facilities',
        'refurbishment', 'repair', 'property', 'leasehold',
        # Specific patterns
        'BUILDING SERVICE', 'ESTATE MANAGEMENT', 'PROPERTY MAINTENANCE',
        'vessel maintenance', 'motorcycle maintenance'
    ],
    'Operations': [
        # Transport
        'TOC', 'train', 'rail', 'travel', 'accommodation', 'hotel',
        'vehicle', 'fleet', 'fuel', 'fares', 'aviation', 'franchise',
        # Utilities & Communications
        'utilities', 'electricity', 'gas', 'water', 'energy',
        'marketing', 'advertising', 'campaign', 'media', 'PR',
        'postal', 'courier', 'mail',
        # Specific patterns from data
        'ARVAL FUEL', 'RAIL FARES', 'ASYLUM SEEKER TRAVEL',
        'eurocontrol', 'flying charge', 'corporate travel'
    ],
    'Legal': [
        'legal', 'barrister', 'solicitor', 'counsel', 'appeal',
        'litigation', 'tribunal', 'LEGAL ADVICE', 'LEGAL REPRESENTATION',
        'ADVERSE LEGAL', 'claim', 'liability'
    ],
    'HR/Staffing': [
        'contingent labour', 'mandays', 'recruitment', 'contractor',
        'agency', 'temporary', 'staffing', 'personnel',
        'CONTINGENT LABOUR', 'PROJECT MANDAYS', 'AGENCY STAFF',
        'basic salary', 'salary', 'apprentice levy',
        # Partner/contractor patterns
        'partner: staffing', 'technical partner', 'commercial partner'
    ],
    'Grants': [
        'grant', 'grt', 'subsidy', 'subsid', 'aid', 'fund', 'payment to', 'transfer',
        # Specific UK patterns
        'GRT AID', 'CAP GRT', 'CURR GRT', 'CAPITAL GRANT', 'grant in aid'
    ],
    'Administrative': [
        # Office & general admin
        'business rates', 'insurance', 'car parking', 'parking',
        'conference', 'training', 'learning', 'subscription', 'membership',
        'office', 'stationery', 'supplies', 'camera', 'equipment',
        # Financial admin
        'bank charges', 'block charges', 'service charge', 'allocation',
        'PFI', 'unitary', 'suspense', 'GR/IR',
        # Other admin patterns from data
        'CS LEARNING', 'CONFERENCES', 'BPO VOLUMETRIC', 'DIRECT COSTS',
        'FM ALLOCATION', 'printing', 'non-stock', 'expense claim'
    ]
}

# Direct expense type mappings

# Based on analysis: top 20 expense types cover >70% of transactions (HMRC: 78%, Home Office: 79%, DfT: 71%)
DIRECT_EXPENSE_TYPE_MAPPING = {
    'HMRC': {
        'PROJECT Mandays Supp': 'HR/Staffing',
        'Utility Payments - electricity': 'Operations',
        'PROJECT Mandays HMRC': 'HR/Staffing',
        'Project Development': 'IT',
        'Physical Hosting and Infrastructure': 'IT',
        'System Maintenance': 'IT',
        'Property Management Services (Irrecoverable VAT)': 'Construction',
        'Project support': 'IT',
        'Desktop Services': 'IT',
        'Rent (Irrec VAT)': 'Construction',
        'Virtual Hosting and Infrastructure': 'IT',
        'IT Software Licenses and Support': 'IT',
        'Employee education': 'Administrative',
        'Contin Labor Build': 'HR/Staffing',
        'Contracted Services': 'Operations',
        'Consultancy - IT': 'Consultancy',
        'Projects VAT irrec': 'IT',
        'Tribunal appellant costs': 'Legal',
        'Maintenance fees': 'Construction',
        'Contingent Labour Build': 'HR/Staffing'
    },
    'Home Office': {
        'IT RUN COST': 'IT',
        'CONTINGENT LABOUR OTHER': 'HR/Staffing',
        'OTHER ICT COSTS': 'IT',
        'SYSTEM CLEARING': 'IT',
        'FULL COST': 'HR/Staffing',
        'RESEARCH AND DEVELOPMENT': 'Consultancy',
        'HOSTING': 'IT',
        'ASYLUM CASES': 'Operations',
        'END USER COMPUTER SOFTWARE': 'IT',
        'IN COUNTRY ESCORT': 'Operations',
        'ADVICE': 'Consultancy',
        'RUN COSTS': 'Operations',
        'SPECIALIST USER SOFTWARE & HARDWARE': 'IT',
        'BASIC SALARY': 'HR/Staffing',
        'CONTRACTS': 'Operations',
        'PROJECT': 'IT',
        'LEGAL ADVICE': 'Legal',
        'AD PRODUCTION': 'Operations',
        'FLEET MANAGEMENT': 'Operations',
        'OTHER': 'Administrative'
    },
    'DfT': {
        'TA Cost AUC - Programme': 'Construction',
        'TA Renewal of Roads - Capital': 'Construction',
        'Subsidies Private Se': 'Grants',
        'TA Renewal of Structures - Capital': 'Construction',
        'Cap Grt Loc Auth': 'Grants',
        'TA Cost AUC  Non SRN': 'Construction',
        'CM - Lump Sum Fees': 'Construction',
        'AUC - Phase 1': 'Construction',
        'Contractor Costs': 'HR/Staffing',
        'RM Cost Reimbursable': 'Construction',
        'TA Cost AUC – Non SRN': 'Construction',
        'Support Services': 'Administrative',
        'Research': 'Consultancy',
        'Professional Services': 'Consultancy',
        'Cap Grt Pri Sec-Cos.': 'Grants',
        'Mail Collection/Deli': 'Operations',
        'IT Ser Running Costs': 'IT',
        'Consultants Costs': 'Consultancy',
        'PFI Service Payments': 'Administrative',
        'TOCOpCosts(Pub)': 'Operations'
    }
}

# Anomaly thresholds

# Metadata for thresholds
THRESHOLD_METADATA = {
    'calculated_date': '2025-10-29',
    'data_source': 'Jan-Aug 2025 sample (9,809 transactions)',
    'data_size': {'HMRC': 2831, 'Home Office': 4344, 'DfT': 2586},
    'percentile': 95,
    'note': 'Recalculate if adding >3 months of new data'
}

ANOMALY_THRESHOLDS = {
    # Department-specific high payment thresholds (from 95th percentile analysis) - data-driven thresholds
    'high_payment': {
        'HMRC': 934_000,         # £934K (95th percentile from 2,831 transactions)
        'Home Office': 884_000,  # £884K (95th percentile from 4,344 transactions)
        'DfT': 1_360_000        # £1.36M (95th percentile from 2,586 transactions)
    }, # the following are more arbitrary thresholds
    'concentration_threshold_spend': 0.15,  # Supplier has >15% of department total spend
    'concentration_threshold_txn': 0.10,    # Supplier has >10% of department transactions
    'duplicate_window_days': 7  # Same amount+supplier within week
}

# File paths

OUTPUT_DIR = Path("output")
LOG_FILE = Path("spending_insights.log")

# Logging

LOGGING_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'handlers': ['file', 'console']
}

# LLM configuration

DEFAULT_MODEL = "claude-sonnet-4-5-20250929"
LLM_MAX_TOKENS = 1500
LLM_TEMPERATURE = 0.2

# Validation

def validate_config():
    """Validate configuration consistency. Call at startup."""
    if set(CATEGORY_KEYWORDS.keys()) != set(CATEGORIES):
        raise ValueError(
            f"Category mismatch!\n"
            f"CATEGORIES: {CATEGORIES}\n"
            f"CATEGORY_KEYWORDS: {list(CATEGORY_KEYWORDS.keys())}"
        )

    for dept, mappings in DIRECT_EXPENSE_TYPE_MAPPING.items():
        invalid = set(mappings.values()) - set(CATEGORIES)
        if invalid:
            raise ValueError(
                f"{dept} has invalid categories in direct mapping: {invalid}\n"
                f"Valid categories: {CATEGORIES}"
            )

    required_depts = {'HMRC', 'Home Office', 'DfT'}
    missing = required_depts - set(ANOMALY_THRESHOLDS['high_payment'].keys())
    if missing:
        raise ValueError(f"Missing threshold definitions for: {missing}")
