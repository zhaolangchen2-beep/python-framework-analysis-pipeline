"""TPC-H Q3 — Shipping Priority"""

import time
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, DoubleType, LongType, StringType
from udfs.tpch_constants import SEGMENTS, sql_array

_schema = StructType([
    StructField("l_orderkey",    LongType(),   True),
    StructField("revenue",       DoubleType(), True),
    StructField("o_orderdate",   LongType(),   True),
    StructField("o_shippriority", LongType(),  True),
    StructField("passed_filter", LongType(),   True),
    StructField("row_id",        LongType(),   True),
    StructField("py_duration",   LongType(),   True),
])


def _gen_data(spark, n_rows, parallelism):
    segs = sql_array(SEGMENTS)
    n_orders = max(n_rows // 4, 1000)
    return spark.range(0, n_rows, numPartitions=parallelism).selectExpr(
        "id AS l_rowid",
        f"CAST(id % {n_orders} AS BIGINT) AS l_orderkey",
        "CAST(900.00 + (id % 99000) * 0.01 AS DOUBLE) AS l_extendedprice",
        "CAST((id % 11) * 0.01 AS DOUBLE) AS l_discount",
        "datediff(date_add(DATE '1992-01-01', CAST(id % 2556 AS INT)), DATE '1970-01-01') AS l_shipdate_days",
        "datediff(date_add(DATE '1992-01-01', CAST((id * 3) % 2556 AS INT)), DATE '1970-01-01') AS o_orderdate_days",
        f"element_at({segs}, CAST((id % {len(SEGMENTS)}) + 1 AS INT)) AS c_mktsegment",
        "CAST(id % 3 AS BIGINT) AS o_shippriority",
    )


def _make_udf():
    _cutoff_order = 9204  # 1995-03-15
    _cutoff_ship = 9204

    def fn(row_id, l_orderkey, l_extendedprice, l_discount,
           l_shipdate_days, o_orderdate_days, c_mktsegment,
           o_shippriority, _java_ts):
        t0 = time.perf_counter_ns()
        if (c_mktsegment == "BUILDING"
                and o_orderdate_days < _cutoff_order
                and l_shipdate_days > _cutoff_ship):
            revenue = l_extendedprice * (1.0 - l_discount)
            passed = 1
        else:
            revenue = 0.0
            passed = 0
        elapsed = time.perf_counter_ns() - t0
        return (l_orderkey, revenue, o_orderdate_days,
                o_shippriority, passed, row_id, elapsed)
    return udf(fn, _schema)


def setup(spark, args):
    N = args.row_count
    df = _gen_data(spark, N, args.parallelism)
    spark.udf.register("process", _make_udf())
    df.createOrReplaceTempView("src")
    sql = """
        SELECT r.l_orderkey, r.revenue, r.o_orderdate, r.passed_filter,
               calc_overhead(r.row_id, r.py_duration) AS overhead
        FROM (
            SELECT process(
                l_rowid, l_orderkey, l_extendedprice, l_discount,
                l_shipdate_days, o_orderdate_days, c_mktsegment,
                o_shippriority, mark_start(l_rowid)
            ) AS r FROM src
        ) t
    """
    return sql, "TPC-H Q3", N, N


UDF_SPEC = {
    "name": "tpch_q3",
    "description": "TPC-H Q3 — Shipping Priority",
    "setup": setup,
}
