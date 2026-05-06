"""TPC-H Q12 — Shipping Modes and Order Priority"""

import time
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, DoubleType, LongType, StringType
from udfs.tpch_constants import SHIP_MODES, sql_array

_schema = StructType([
    StructField("l_shipmode",    StringType(), True),
    StructField("high_priority", LongType(),   True),
    StructField("low_priority",  LongType(),   True),
    StructField("passed_filter", LongType(),   True),
    StructField("row_id",        LongType(),   True),
    StructField("py_duration",   LongType(),   True),
])


def _gen_data(spark, n_rows, parallelism):
    modes = sql_array(SHIP_MODES)
    return spark.range(0, n_rows, numPartitions=parallelism).selectExpr(
        "id AS l_rowid",
        f"element_at({modes}, CAST((id % {len(SHIP_MODES)}) + 1 AS INT)) AS l_shipmode",
        "datediff(date_add(DATE '1992-01-01', CAST(id % 2556 AS INT)), DATE '1970-01-01') AS l_receiptdate",
        "datediff(date_add(DATE '1992-01-01', CAST((id * 2) % 2556 AS INT)), DATE '1970-01-01') AS l_commitdate",
        "element_at(array('1-URGENT', '2-HIGH', '3-MEDIUM', '4-NOT SPECIFIED', '5-LOW'), "
        "            CAST((id % 5) + 1 AS INT)) AS o_orderpriority",
    )


def _make_udf():
    _cutoff_date = 10471  # 1998-09-02
    _high_priority_keywords = ["1-URGENT", "2-HIGH"]

    def fn(row_id, l_shipmode, l_receiptdate, l_commitdate, o_orderpriority, _java_ts):
        t0 = time.perf_counter_ns()
        if l_receiptdate > l_commitdate:
            if o_orderpriority in _high_priority_keywords:
                high = 1
                low = 0
            else:
                high = 0
                low = 1
            passed = 1
        else:
            high, low, passed = 0, 0, 0
        elapsed = time.perf_counter_ns() - t0
        return (l_shipmode, high, low, passed, row_id, elapsed)
    return udf(fn, _schema)


def setup(spark, args):
    N = args.row_count
    df = _gen_data(spark, N, args.parallelism)
    spark.udf.register("process", _make_udf())
    df.createOrReplaceTempView("src")
    sql = """
        SELECT r.l_shipmode, r.high_priority, r.low_priority, r.passed_filter,
               calc_overhead(r.row_id, r.py_duration) AS overhead
        FROM (
            SELECT process(
                l_rowid, l_shipmode, l_receiptdate, l_commitdate, o_orderpriority,
                mark_start(l_rowid)
            ) AS r FROM src
        ) t
    """
    return sql, "TPC-H Q12", N, N


UDF_SPEC = {
    "name": "tpch_q12",
    "description": "TPC-H Q12 — Shipping Modes and Order Priority",
    "setup": setup,
}
