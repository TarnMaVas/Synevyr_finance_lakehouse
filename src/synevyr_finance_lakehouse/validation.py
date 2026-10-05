from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class ValidationResult:
    check_name: str
    table_name: str
    failed_rows: int
    details: str = ""

    @property
    def passed(self) -> bool:
        return self.failed_rows == 0

    def as_tuple(self) -> tuple[str, str, int, bool, str]:
        return (
            self.check_name,
            self.table_name,
            self.failed_rows,
            self.passed,
            self.details,
        )


def invalid_count(df: DataFrame, valid_condition) -> int:
    """
    Count rows that do not satisfy a rule.

    NULL is treated as invalid rather than disappearing through SQL
    three-valued logic.
    """
    valid = F.coalesce(valid_condition, F.lit(False))
    return df.filter(~valid).count()


def duplicate_count(df: DataFrame, *keys: str) -> int:
    """
    Count duplicated key groups rather than duplicate rows.
    Zero means the candidate key is unique.
    """
    return (
        df.groupBy(*keys)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )


def orphan_count(
    child: DataFrame,
    parent: DataFrame,
    condition,
) -> int:
    return child.join(parent, condition, "left_anti").count()


def truncate_to_cents(column):
    """
    TPC-H monetary truncation for non-negative values.
    """
    return F.floor(column * F.lit(100)) / F.lit(100)


def table_local_validations(
    silver: dict[str, DataFrame],
    *,
    market_segments: set[str],
    discount_min: float,
    discount_max: float,
    tax_min: float,
    tax_max: float,
) -> list[ValidationResult]:
    customer = silver["customer"]
    orders = silver["orders"]
    lineitem = silver["lineitem"]
    supplier = silver["supplier"]
    part = silver["part"]
    partsupp = silver["partsupp"]
    nation = silver["nation"]
    region = silver["region"]

    results: list[ValidationResult] = []

    key_checks = [
        ("customer PK unique", "customer", customer, ("c_custkey",)),
        ("orders PK unique", "orders", orders, ("o_orderkey",)),
        ("supplier PK unique", "supplier", supplier, ("s_suppkey",)),
        ("part PK unique", "part", part, ("p_partkey",)),
        ("nation PK unique", "nation", nation, ("n_nationkey",)),
        ("region PK unique", "region", region, ("r_regionkey",)),
        (
            "lineitem composite PK unique",
            "lineitem",
            lineitem,
            ("l_orderkey", "l_linenumber"),
        ),
        (
            "partsupp composite PK unique",
            "partsupp",
            partsupp,
            ("ps_partkey", "ps_suppkey"),
        ),
    ]

    for check_name, table_name, df, keys in key_checks:
        results.append(
            ValidationResult(
                check_name,
                table_name,
                duplicate_count(df, *keys),
                f"keys={keys}",
            )
        )

    required_fields = {
        "customer": [
            "c_custkey",
            "c_nationkey",
            "c_mktsegment",
        ],
        "orders": [
            "o_orderkey",
            "o_custkey",
            "o_totalprice",
            "o_orderdate",
        ],
        "lineitem": [
            "l_orderkey",
            "l_partkey",
            "l_suppkey",
            "l_linenumber",
            "l_quantity",
            "l_extendedprice",
            "l_discount",
            "l_tax",
        ],
        "supplier": [
            "s_suppkey",
            "s_nationkey",
        ],
        "part": [
            "p_partkey",
            "p_retailprice",
        ],
        "partsupp": [
            "ps_partkey",
            "ps_suppkey",
        ],
        "nation": [
            "n_nationkey",
            "n_regionkey",
        ],
        "region": [
            "r_regionkey",
        ],
    }

    for table_name, columns in required_fields.items():
        df = silver[table_name]
        for column in columns:
            results.append(
                ValidationResult(
                    f"{column} non-null",
                    table_name,
                    df.filter(F.col(column).isNull()).count(),
                )
            )

    results.extend(
        [
            ValidationResult(
                "market segment allowed",
                "customer",
                invalid_count(
                    customer,
                    F.col("c_mktsegment").isin(sorted(market_segments)),
                ),
                f"allowed={sorted(market_segments)}",
            ),
            ValidationResult(
                "order total positive",
                "orders",
                invalid_count(orders, F.col("o_totalprice") > 0),
            ),
            ValidationResult(
                "discount range",
                "lineitem",
                invalid_count(
                    lineitem,
                    F.col("l_discount").between(
                        discount_min,
                        discount_max,
                    ),
                ),
                f"[{discount_min}, {discount_max}]",
            ),
            ValidationResult(
                "tax range",
                "lineitem",
                invalid_count(
                    lineitem,
                    F.col("l_tax").between(
                        tax_min,
                        tax_max,
                    ),
                ),
                f"[{tax_min}, {tax_max}]",
            ),
            ValidationResult(
                "quantity positive",
                "lineitem",
                invalid_count(lineitem, F.col("l_quantity") > 0),
            ),
            ValidationResult(
                "extended price positive",
                "lineitem",
                invalid_count(lineitem, F.col("l_extendedprice") > 0),
            ),
            ValidationResult(
                "retail price positive",
                "part",
                invalid_count(part, F.col("p_retailprice") > 0),
            ),
        ]
    )

    return results


def referential_validations(
    silver: dict[str, DataFrame],
) -> list[ValidationResult]:
    customer = silver["customer"]
    orders = silver["orders"]
    lineitem = silver["lineitem"]
    supplier = silver["supplier"]
    part = silver["part"]
    partsupp = silver["partsupp"]
    nation = silver["nation"]
    region = silver["region"]

    checks = [
        ValidationResult(
            "orders -> customer",
            "orders",
            orphan_count(
                orders,
                customer,
                orders["o_custkey"] == customer["c_custkey"],
            ),
        ),
        ValidationResult(
            "lineitem -> orders",
            "lineitem",
            orphan_count(
                lineitem,
                orders,
                lineitem["l_orderkey"] == orders["o_orderkey"],
            ),
        ),
        ValidationResult(
            "lineitem -> part",
            "lineitem",
            orphan_count(
                lineitem,
                part,
                lineitem["l_partkey"] == part["p_partkey"],
            ),
        ),
        ValidationResult(
            "lineitem -> supplier",
            "lineitem",
            orphan_count(
                lineitem,
                supplier,
                lineitem["l_suppkey"] == supplier["s_suppkey"],
            ),
        ),
        ValidationResult(
            "customer -> nation",
            "customer",
            orphan_count(
                customer,
                nation,
                customer["c_nationkey"] == nation["n_nationkey"],
            ),
        ),
        ValidationResult(
            "supplier -> nation",
            "supplier",
            orphan_count(
                supplier,
                nation,
                supplier["s_nationkey"] == nation["n_nationkey"],
            ),
        ),
        ValidationResult(
            "nation -> region",
            "nation",
            orphan_count(
                nation,
                region,
                nation["n_regionkey"] == region["r_regionkey"],
            ),
        ),
        ValidationResult(
            "partsupp -> part",
            "partsupp",
            orphan_count(
                partsupp,
                part,
                partsupp["ps_partkey"] == part["p_partkey"],
            ),
        ),
        ValidationResult(
            "partsupp -> supplier",
            "partsupp",
            orphan_count(
                partsupp,
                supplier,
                partsupp["ps_suppkey"] == supplier["s_suppkey"],
            ),
        ),
        ValidationResult(
            "lineitem(part, supplier) -> partsupp",
            "lineitem",
            orphan_count(
                lineitem,
                partsupp,
                (
                    lineitem["l_partkey"] == partsupp["ps_partkey"]
                )
                & (
                    lineitem["l_suppkey"] == partsupp["ps_suppkey"]
                ),
            ),
        ),
    ]

    return checks


def order_total_reconciliation(
    orders: DataFrame,
    lineitem: DataFrame,
    *,
    tolerance: float,
) -> tuple[ValidationResult, DataFrame]:
    discounted_price = truncate_to_cents(
        F.col("l_extendedprice")
        * (F.lit(1) - F.col("l_discount"))
    )

    line_total = truncate_to_cents(
        discounted_price
        * (F.lit(1) + F.col("l_tax"))
    )

    calculated = (
        lineitem
        .withColumn("_calculated_line_total", line_total)
        .groupBy("l_orderkey")
        .agg(
            F.sum("_calculated_line_total")
            .alias("calculated_order_total")
        )
    )

    reconciliation = (
        orders
        .select("o_orderkey", "o_totalprice")
        .join(
            calculated,
            F.col("o_orderkey") == F.col("l_orderkey"),
            "inner",
        )
        .withColumn(
            "difference",
            F.abs(
                F.col("o_totalprice")
                - F.col("calculated_order_total")
            ),
        )
    )

    failed = reconciliation.filter(
        F.col("difference") > tolerance
    ).count()

    return (
        ValidationResult(
            "order-total reconciliation",
            "orders + lineitem",
            failed,
            f"tolerance={tolerance}",
        ),
        reconciliation,
    )


def net_revenue_total(lineitem: DataFrame):
    """
    Canonical Finance net revenue:
    extended price net of discount, excluding tax.
    """
    return (
        lineitem
        .agg(
            F.sum(
                F.col("l_extendedprice")
                * (F.lit(1) - F.col("l_discount"))
            ).alias("net_revenue")
        )
        .first()["net_revenue"]
    )


def assert_all_passed(results: list[ValidationResult]) -> None:
    failed = [result for result in results if not result.passed]

    if failed:
        lines = [
            f"- {r.check_name}: {r.failed_rows} failures"
            for r in failed
        ]
        raise ValueError(
            "Silver validation failed:\n" + "\n".join(lines)
        )
