"""TPC-H Q6 — Forecasting Revenue Change"""

import time
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, DoubleType, LongType

_schema = StructType([
    StructField("revenue",       DoubleType(), True),
    StructField("passed_filter", LongType(),   True),
    StructField("row_id",        LongType(),   True),
    StructField("py_duration",   LongType(),   True),
])


def _gen_data(spark, n_rows, parallelism):
    return spark.range(0, n_rows, numPartitions=parallelism).selectExpr(
        "id AS l_rowid",
        "CAST(900.00 + (id % 99000) * 0.01 AS DOUBLE) AS l_extendedprice",
        "CAST((id % 11) * 0.01 AS DOUBLE) AS l_discount",
        "CAST((id % 100) AS BIGINT) AS l_quantity",
        "datediff(date_add(DATE '1992-01-01', CAST(id % 2556 AS INT)), DATE '1970-01-01') AS l_shipdate_days",
    )


def _make_udf():
    _cutoff_date = 9204  # 1995-03-15
    _min_qty = 24
    _min_disc = 0.05
    _max_disc = 0.07

    def fn(row_id, l_extendedprice, l_discount, l_quantity, l_shipdate_days, _java_ts):
        t0 = time.perf_counter_ns()
        if (l_shipdate_days >= _cutoff_date
                and l_shipdate_days < _cutoff_date + 365
                and l_quantity < _min_qty
                and l_discount >= _min_disc
                and l_discount <= _max_disc):
            revenue = l_extendedprice * l_discount
            passed = 1
        else:
            revenue = 0.0
            passed = 0
        elapsed = time.perf_counter_ns() - t0
        return (revenue, passed, row_id, elapsed)
    return udf(fn, _schema)


def setup(spark, args):
    N = args.row_count
    df = _gen_data(spark, N, args.parallelism)
    spark.udf.register("process", _make_udf())
    df.createOrReplaceTempView("src")
    sql = """
        SELECT r.revenue, r.passed_filter,
               calc_overhead(r.row_id, r.py_duration) AS overhead
        FROM (
            SELECT process(
                l_rowid, l_extendedprice, l_discount, l_quantity,
                l_shipdate_days, mark_start(l_rowid)
            ) AS r FROM src
        ) t
    """
    return sql, "TPC-H Q6", N, N


UDF_SPEC = {
    "name": "tpch_q6",
    "description": "TPC-H Q6 — Forecasting Revenue Change",
    "setup": setup,
}
