CATALOG = "workspace"
SCHEMA = "default"

SOURCE_SCHEMA = "samples.tpch"

BRONZE_PREFIX = "finance_bronze"
SILVER_PREFIX = "finance_silver"
GOLD_PREFIX = "finance_gold"
VALIDATION_PREFIX = "finance_validation"
MONITORING_PREFIX = "finance_monitoring"

TABLES = [
    "customer",
    "orders",
    "lineitem",
    "supplier",
    "part",
    "partsupp",
    "nation",
    "region",
]

MARKET_SEGMENTS = {
    "AUTOMOBILE",
    "BUILDING",
    "FURNITURE",
    "HOUSEHOLD",
    "MACHINERY",
}

DISCOUNT_MIN = 0.00
DISCOUNT_MAX = 0.10

TAX_MIN = 0.00
TAX_MAX = 0.08

ORDER_TOTAL_TOLERANCE = 0.01


def source_table(table: str) -> str:
    return f"{SOURCE_SCHEMA}.{table}"


def project_table(prefix: str, table: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{prefix}_{table}"
