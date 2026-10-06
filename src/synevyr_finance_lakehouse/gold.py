from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .validation import net_revenue_total


NET_REVENUE = F.col("l_extendedprice") * (
    F.lit(1) - F.col("l_discount")
)


def build_gold_customer_revenue(
    silver_customer: DataFrame,
    silver_orders: DataFrame,
    silver_lineitem: DataFrame,
) -> DataFrame:
    """Build customer-level Finance metrics for questions 1 and 3."""
    order_items = (
        silver_lineitem
        .groupBy("l_orderkey")
        .agg(F.sum(NET_REVENUE).alias("order_net_revenue"))
    )

    orders_with_revenue = (
        silver_orders
        .join(
            order_items,
            silver_orders["o_orderkey"] == order_items["l_orderkey"],
            "left",
        )
        .select(
            "o_orderkey",
            "o_custkey",
            "o_orderdate",
            F.coalesce(F.col("order_net_revenue"), F.lit(0.0))
            .alias("order_net_revenue"),
        )
    )

    return (
        silver_customer
        .join(
            orders_with_revenue,
            silver_customer["c_custkey"] == orders_with_revenue["o_custkey"],
            "left",
        )
        .groupBy("c_custkey", "c_name", "c_mktsegment")
        .agg(
            F.coalesce(F.sum("order_net_revenue"), F.lit(0.0))
            .alias("total_net_revenue"),
            F.count("o_orderkey").alias("total_orders"),
            F.coalesce(F.avg("order_net_revenue"), F.lit(0.0))
            .alias("avg_order_value"),
        )
    )


def build_gold_yearly_revenue(
    silver_lineitem: DataFrame,
    silver_orders: DataFrame,
) -> DataFrame:
    """Build yearly gross, discounted, and tax-inclusive revenue."""
    return (
        silver_lineitem
        .join(
            silver_orders,
            silver_lineitem["l_orderkey"] == silver_orders["o_orderkey"],
            "inner",
        )
        .withColumn("year", F.year("o_orderdate"))
        .groupBy("year")
        .agg(
            F.sum("l_extendedprice").alias("gross_revenue"),
            F.sum(NET_REVENUE).alias("net_revenue_discount"),
            F.sum(
                F.col("l_extendedprice")
                * (F.lit(1) - F.col("l_discount"))
                * (F.lit(1) + F.col("l_tax"))
            ).alias("net_revenue_discount_tax"),
        )
        .orderBy("year")
    )


def build_gold_supplier_performance(
    silver_supplier: DataFrame,
    silver_lineitem: DataFrame,
) -> DataFrame:
    """Build supplier revenue and average discount metrics for question 4."""
    return (
        silver_supplier
        .join(
            silver_lineitem,
            silver_supplier["s_suppkey"] == silver_lineitem["l_suppkey"],
            "left",
        )
        .groupBy("s_suppkey", "s_name")
        .agg(
            F.coalesce(F.sum(NET_REVENUE), F.lit(0.0))
            .alias("supplier_net_revenue"),
            F.coalesce(F.avg("l_discount"), F.lit(0.0))
            .alias("avg_discount"),
        )
    )


def build_gold_segment_yearly_order_value(
    silver_customer: DataFrame,
    silver_orders: DataFrame,
    silver_lineitem: DataFrame,
) -> DataFrame:
    """Build yearly average order value by customer market segment."""
    order_revenue = (
        silver_lineitem
        .groupBy("l_orderkey")
        .agg(F.sum(NET_REVENUE).alias("order_net_revenue"))
    )

    return (
        silver_orders
        .join(
            order_revenue,
            silver_orders["o_orderkey"] == order_revenue["l_orderkey"],
            "inner",
        )
        .join(
            silver_customer,
            silver_orders["o_custkey"] == silver_customer["c_custkey"],
            "inner",
        )
        .withColumn("year", F.year("o_orderdate"))
        .groupBy("year", "c_mktsegment")
        .agg(
            F.avg("order_net_revenue").alias("avg_order_value"),
            F.countDistinct("o_orderkey").alias("order_count"),
        )
        .orderBy("year", "c_mktsegment")
    )


def reconcile_bronze_silver_gold(
    bronze_lineitem: DataFrame,
    silver_lineitem: DataFrame,
    gold_customer_revenue: DataFrame,
    tolerance: float = 0.01,
) -> bool:
    """Reconcile canonical net revenue across Bronze, Silver, and Gold."""
    bronze_revenue = net_revenue_total(bronze_lineitem)
    silver_revenue = net_revenue_total(silver_lineitem)
    gold_revenue = (
        gold_customer_revenue
        .agg(F.sum("total_net_revenue").alias("net_revenue"))
        .first()["net_revenue"]
    )

    if bronze_revenue is None or silver_revenue is None or gold_revenue is None:
        raise ValueError("Reconciliation cannot run on empty revenue data.")

    bronze_value = float(bronze_revenue)
    silver_value = float(silver_revenue)
    gold_value = float(gold_revenue)
    diff_bronze_silver = abs(bronze_value - silver_value)
    diff_silver_gold = abs(silver_value - gold_value)

    print(f"Bronze Net Revenue: {bronze_value:.2f}")
    print(f"Silver Net Revenue: {silver_value:.2f}")
    print(f"Gold Net Revenue:   {gold_value:.2f}")

    if diff_bronze_silver > tolerance or diff_silver_gold > tolerance:
        raise ValueError(
            "Reconciliation failed: "
            f"Bronze-Silver={diff_bronze_silver:.4f}, "
            f"Silver-Gold={diff_silver_gold:.4f}, "
            f"tolerance={tolerance:.2f}"
        )

    print("Reconciliation passed successfully.")
    return True
