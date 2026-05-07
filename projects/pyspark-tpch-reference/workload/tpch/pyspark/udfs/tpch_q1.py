"""TPC-H Q1 — Pricing Summary Report"""

import time
from pyspark.sql.functions import udf
from pyspark.sql.types import (
    StructType, StructField, DoubleType, LongType, StringType,
)

_schema = StructType([
    StructField("l_returnflag",    StringType(), True),
    StructField("l_linestatus",    StringType(), True),
    StructField("l_quantity",      DoubleType(), True),
    StructField("l_extendedprice", DoubleType(), True),
    StructField("disc_price",      DoubleType(), True),
    StructField("charge",          DoubleType(), True),
    StructField("l_discount",      DoubleType(), True),
    StructField("passed_filter",   LongType(),   True),
    StructField("row_id",          LongType(),   True),
    StructField("py_duration",     LongType(),   True),
])


def _gen_lineitem(spark, n_rows, parallelism):
    return spark.range(0, n_rows, numPartitions=parallelism).selectExpr(
        "id AS l_rowid",
        "CAST(1 + (id % 50) AS DOUBLE) AS l_quantity",
        "CAST(900.00 + (id % 99000) * 0.01 AS DOUBLE) AS l_extendedprice",
        "CAST((id % 11) * 0.01 AS DOUBLE) AS l_discount",
        "CAST((id % 9) * 0.01 AS DOUBLE) AS l_tax",
        "element_at(array('A','R','N'), CAST((id % 3) + 1 AS INT)) AS l_returnflag",
        "element_at(array('O','F'), CAST((id % 2) + 1 AS INT)) AS l_linestatus",
        # shipdate: 1992-01-01 + (id % 2556) days → covers through 1998-12-31
        "datediff(date_add(DATE '1992-01-01', CAST(id % 2556 AS INT)), "
        "         DATE '1970-01-01') AS l_shipdate_days",
    )


def _make_udf():
    def fn(row_id, l_returnflag, l_linestatus,
           l_quantity, l_extendedprice, l_discount, l_tax,
           l_shipdate_days, _java_ts):
        t0 = time.perf_counter_ns()
        # 1998-09-02 = 10471 epoch days
        if l_shipdate_days <= 10471:
            dp = l_extendedprice * (1.0 - l_discount)
            ch = dp * (1.0 + l_tax)
            passed = 1
        else:
            dp, ch, passed = 0.0, 0.0, 0
        elapsed = time.perf_counter_ns() - t0
        return (l_returnflag, l_linestatus, l_quantity,
                l_extendedprice, dp, ch, l_discount,
                passed, row_id, elapsed)
    return udf(fn, _schema)


def setup(spark, args):
    N = args.row_count
    li = _gen_lineitem(spark, N, args.parallelism)

    spark.udf.register("process", _make_udf())
    li.createOrReplaceTempView("src")

    sql = """
        SELECT r.l_returnflag, r.l_linestatus,
               r.disc_price, r.charge, r.passed_filter,
               r.py_duration,
               calc_overhead(r.row_id, r.py_duration) AS overhead
        FROM (
            SELECT process(
                l_rowid, l_returnflag, l_linestatus,
                l_quantity, l_extendedprice, l_discount, l_tax,
                l_shipdate_days, mark_start(l_rowid)
            ) AS r
            FROM src
        ) t
    """
    return sql, "TPC-H Q1", N, N


UDF_SPEC = {
    "name":        "tpch_q1",
    "description": "TPC-H Q1 — Pricing Summary Report",
    "setup":       setup,
}
