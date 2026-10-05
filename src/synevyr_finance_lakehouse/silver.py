from collections.abc import Callable

from pyspark.sql import DataFrame, SparkSession


SILVER_COLUMNS: dict[str, list[str]] = {
    "customer": [
        "c_custkey",
        "c_name",
        "c_address",
        "c_nationkey",
        "c_phone",
        "c_acctbal",
        "c_mktsegment",
        "c_comment",
    ],
    "orders": [
        "o_orderkey",
        "o_custkey",
        "o_orderstatus",
        "o_totalprice",
        "o_orderdate",
        "o_orderpriority",
        "o_clerk",
        "o_shippriority",
        "o_comment",
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
        "l_returnflag",
        "l_linestatus",
        "l_shipdate",
        "l_commitdate",
        "l_receiptdate",
        "l_shipinstruct",
        "l_shipmode",
        "l_comment",
    ],
    "supplier": [
        "s_suppkey",
        "s_name",
        "s_address",
        "s_nationkey",
        "s_phone",
        "s_acctbal",
        "s_comment",
    ],
    "part": [
        "p_partkey",
        "p_name",
        "p_mfgr",
        "p_brand",
        "p_type",
        "p_size",
        "p_container",
        "p_retailprice",
        "p_comment",
    ],
    "partsupp": [
        "ps_partkey",
        "ps_suppkey",
        "ps_availqty",
        "ps_supplycost",
        "ps_comment",
    ],
    "nation": [
        "n_nationkey",
        "n_name",
        "n_regionkey",
        "n_comment",
    ],
    "region": [
        "r_regionkey",
        "r_name",
        "r_comment",
    ],
}


def build_silver_table(df: DataFrame, table: str) -> DataFrame:
    """
    Build the trusted relational representation of one Bronze table.

    Bronze ingestion metadata is intentionally omitted. No business measures or
    aggregates are introduced in Silver.
    """
    try:
        columns = SILVER_COLUMNS[table]
    except KeyError as exc:
        raise ValueError(f"Unsupported Silver table: {table}") from exc

    return df.select(*columns)


def build_all_silver(bronze_dfs: dict[str, DataFrame]) -> dict[str, DataFrame]:
    return {
        table: build_silver_table(df, table)
        for table, df in bronze_dfs.items()
    }


def write_silver_table(
    df: DataFrame,
    target_table_name: str,
) -> None:
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_table_name)
    )


def write_all_silver(
    silver_dfs: dict[str, DataFrame],
    target_name: Callable[[str], str],
) -> None:
    for table, df in silver_dfs.items():
        write_silver_table(
            df=df,
            target_table_name=target_name(table),
        )
