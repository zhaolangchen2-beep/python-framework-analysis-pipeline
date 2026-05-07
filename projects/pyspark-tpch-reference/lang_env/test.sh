#!/bin/bash
# ================================================================
# test.sh — PySpark UDF Benchmark 测试脚本
#
# 功能：
#   1. 在容器内执行 spark-submit 运行基准测试
#   2. 从输出中提取 Benchmark 指标和端到端时长
#   3. 支持单个 query 或 --all 模式跑全部
#   4. 支持 profile 模式：稳定后对 Python Worker 做 perf record
#
# 用法：
#   ./test.sh -q tpch_q1
#   ./test.sh -q tpch_q1 --profile my_profile_name
#   ./test.sh -q all -c 10000000 -v
# ================================================================

set -euo pipefail

# ────────────────────────────────────────────────────────────────
# 默认配置
# ────────────────────────────────────────────────────────────────
CONTAINER="pyspark-spark-master"
MASTER="local[*]"
JAR="/opt/spark/apps/spark-benchmark-udfs-java.jar"
RUN_PY="/opt/spark/apps/run.py"
ROW_COUNT=10000000
PARALLELISM=1
QUERY=""
TASKSET_CORES=""
EXTRA_SPARK_ARGS=""
EXTRA_RUN_ARGS=""
VERBOSE=false
PROFILE_NAME=""
PROFILE_DIR="/opt/spark/apps/profile"
PROFILE_DURATION=60
PERF_FREQ=99

ALL_QUERIES=(
    add cdf
    tpch_q1 tpch_q2 tpch_q3 tpch_q4 tpch_q5 tpch_q6
    tpch_q7 tpch_q8 tpch_q9 tpch_q10 tpch_q11 tpch_q12
    tpch_q13 tpch_q14 tpch_q15 tpch_q16 tpch_q17 tpch_q18
    tpch_q19 tpch_q20 tpch_q21 tpch_q22
)

# ────────────────────────────────────────────────────────────────
# 参数解析
# ────────────────────────────────────────────────────────────────
usage() {
    cat << 'EOF'
Usage: ./test.sh [OPTIONS]

Required:
  -q, --query <name|all>       Query to run (e.g. tpch_q1, add, all)

Benchmark options:
  -c, --row-count <N>          Number of input rows (default: 10000000)
  -a, --parallelism <N>        Spark parallelism (default: 1)

Spark options:
  --master <url>               Spark master (default: local[*])
  --jars <path>                JAR path in container
  --spark-args <args>          Extra spark-submit arguments (quoted)

Execution options:
  --container <name>           Docker container name (default: spark-master)
  --taskset <cores>            CPU cores to pin (e.g. "0-7")
  --run-args <args>            Extra run.py arguments (quoted)

Profile options:
  --profile <name>             Enable perf profiling with given name.
                               After worker stabilizes (2nd benchmark line),
                               runs perf record on pyspark.worker process
                               for 60 seconds, then converts to CSV.
  --profile-duration <secs>    Perf record duration (default: 60)
  --perf-freq <Hz>             Perf sampling frequency (default: 99)

Output options:
  -v, --verbose                Show full spark-submit output
  -h, --help                   Show this help

Examples:
  ./test.sh -q tpch_q1
  ./test.sh -q tpch_q1 --profile cpython314_q1
  ./test.sh -q tpch_q1 --profile q1_baseline --profile-duration 30
  ./test.sh -q all -c 20000000 -v
EOF
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -q|--query)            QUERY="$2";            shift 2 ;;
        -c|--row-count)        ROW_COUNT="$2";         shift 2 ;;
        -a|--parallelism)      PARALLELISM="$2";       shift 2 ;;
        --master)              MASTER="$2";            shift 2 ;;
        --jars)                JAR="$2";               shift 2 ;;
        --container)           CONTAINER="$2";         shift 2 ;;
        --taskset)             TASKSET_CORES="$2";     shift 2 ;;
        --spark-args)          EXTRA_SPARK_ARGS="$2";  shift 2 ;;
        --run-args)            EXTRA_RUN_ARGS="$2";    shift 2 ;;
        --profile)             PROFILE_NAME="$2";      shift 2 ;;
        --profile-duration)    PROFILE_DURATION="$2";  shift 2 ;;
        --perf-freq)           PERF_FREQ="$2";         shift 2 ;;
        -v|--verbose)          VERBOSE=true;           shift ;;
        -h|--help)             usage ;;
        *)
            echo "❌ Unknown option: $1"
            echo "   Run './test.sh --help' for usage."
            exit 1
            ;;
    esac
done

if [ -z "$QUERY" ]; then
    echo "❌ Missing required argument: -q <query|all>"
    echo "   Run './test.sh --help' for usage."
    exit 1
fi

# profile 模式下不支持 all（需要手动指定名称）
if [ -n "$PROFILE_NAME" ] && [ "$QUERY" = "all" ]; then
    echo "❌ --profile cannot be used with -q all"
    echo "   Profile one query at a time."
    exit 1
fi

# ────────────────────────────────────────────────────────────────
# 构建 query 列表
# ────────────────────────────────────────────────────────────────
if [ "$QUERY" = "all" ]; then
    QUERIES=("${ALL_QUERIES[@]}")
else
    QUERIES=("$QUERY")
fi

# ────────────────────────────────────────────────────────────────
# 构建执行命令
# ────────────────────────────────────────────────────────────────
build_cmd() {
    local q="$1"
    local cmd=""

    if [ -n "$TASKSET_CORES" ]; then
        cmd="taskset -c $TASKSET_CORES "
    fi

    cmd+="spark-submit"
    cmd+=" --master $MASTER"
    cmd+=" --jars $JAR"

    if [ -n "$EXTRA_SPARK_ARGS" ]; then
        cmd+=" $EXTRA_SPARK_ARGS"
    fi

    cmd+=" $RUN_PY"
    cmd+=" -q $q"
    cmd+=" -c $ROW_COUNT"
    cmd+=" -a $PARALLELISM"

    if [ -n "$EXTRA_RUN_ARGS" ]; then
        cmd+=" $EXTRA_RUN_ARGS"
    fi

    echo "$cmd"
}

# ────────────────────────────────────────────────────────────────
# 提取指标
# ────────────────────────────────────────────────────────────────
extract_metrics() {
    local output="$1"
    local query_name="$2"

    local bench_lines
    bench_lines=$(echo "$output" | grep '\[.*Benchmark\]' | grep -v 'WARMUP' || true)

    if [ -z "$bench_lines" ]; then
        printf "  %-12s | ⚠️  No benchmark data\n" "$query_name"
        return
    fi

    local total_lines
    total_lines=$(echo "$bench_lines" | wc -l)

    local target_line
    if [ "$total_lines" -ge 2 ]; then
        target_line=$(echo "$bench_lines" | tail -2 | head -1)
    else
        target_line=$(echo "$bench_lines" | tail -1)
    fi

    local avg_udf avg_overhead throughput wallclock
    avg_udf=$(echo "$target_line" | grep -oP 'Avg UDF=\K[0-9]+' || echo "N/A")
    avg_overhead=$(echo "$target_line" | grep -oP 'Avg Overhead=\K[0-9]+' || echo "N/A")
    throughput=$(echo "$target_line" | grep -oP 'Throughput=\K[0-9]+' || echo "N/A")
    wallclock=$(echo "$target_line" | grep -oP 'WallClock=\K[0-9.]+' || echo "N/A")

    local duration
    duration=$(echo "$output" | grep -oP 'Duration=\K[0-9.]+' | tail -1 || true)
    if [ -z "$duration" ]; then
        duration=$(echo "$output" | grep -oP 'Wall time\s*:\s*\K[0-9.]+' || echo "N/A")
    fi

    printf "  %-12s | UDF=%4s ns | Overhead=%5s ns | Throughput=%7s rows/s | Duration=%6s s\n" \
        "$query_name" \
        "$avg_udf" \
        "$avg_overhead" \
        "$throughput" \
        "$duration"
}

# ────────────────────────────────────────────────────────────────
# Profile 函数：等待稳定 → 找 PID → perf record → 转 CSV
# ────────────────────────────────────────────────────────────────
run_profile() {
    local log_file="$1"
    local profile_name="$2"
    local bg_pid="$3"
    local perf_data_file="${PROFILE_DIR}/${profile_name}.data"

    echo ""
    echo "  [Profile] Waiting for worker to stabilize (2nd benchmark line)..."
    echo "  [Profile] Monitoring log: $log_file"

    # ── 等待第 2 行非 WARMUP 的 Benchmark 输出 ──
    local bench_count=0
    local waited=0
    local max_wait=300

    while true; do
        sleep 2
        waited=$((waited + 2))

        # 检查后台进程是否还活着
        if ! kill -0 "$bg_pid" 2>/dev/null; then
            echo "  [Profile] ⚠️  spark-submit already exited (waited ${waited}s)"
            echo "  [Profile] Log tail:"
            tail -10 "$log_file" 2>/dev/null | sed 's/^/    /'
            return 1
        fi

        # 检查超时
        if [ "$waited" -ge "$max_wait" ]; then
            echo "  [Profile] ⚠️  Timed out after ${max_wait}s"
            return 1
        fi

        # 计算 benchmark 行数（文件可能还不存在）
        if [ -f "$log_file" ] && [ -s "$log_file" ]; then
            bench_count=$(grep -c '\[.*Benchmark\]' "$log_file" 2>/dev/null || true)
            # 去掉空白
            bench_count=$(echo "$bench_count" | tr -d '[:space:]')
            bench_count=${bench_count:-0}

            if [ "$VERBOSE" = true ]; then
                echo "  [Profile] ... waited ${waited}s, benchmark lines: $bench_count"
            fi

            # 第一行可能是 WARMUP，需要至少 2 行（含 WARMUP 就是 3 行）
            # 保守起见等 3 行
            if [ "$bench_count" -ge 3 ]; then
                break
            fi
        else
            if [ "$VERBOSE" = true ]; then
                echo "  [Profile] ... waited ${waited}s, log file not ready yet"
            fi
        fi
    done

    echo "  [Profile] Worker stabilized after ${waited}s (${bench_count} benchmark lines)"

    # ── 找到 pyspark.worker 子进程 PID ──
    echo "  [Profile] Looking for pyspark.worker process..."

    local worker_pid=""

    # 尝试多种匹配方式，取最后一个（最新启动的 worker）
    local patterns=(
        "pyspark/worker.py"
        "pyspark.*worker"
        "python.*-c.*from pyspark"
    )

    for pat in "${patterns[@]}"; do
        worker_pid=$(docker exec "$CONTAINER" bash -c \
            "ps -ef | grep '$pat' | grep -v grep | awk '{print \$2}' | tail -1" \
            2>/dev/null || true)
        #                                                         ^^^^^^
        #                                                  head -1 改为 tail -1
        worker_pid=$(echo "$worker_pid" | tr -d '[:space:]')
        if [ -n "$worker_pid" ]; then
            echo "  [Profile] Matched pattern: '$pat'"
            break
        fi
    done

    # 兜底：找 daemon 的子进程（也取最后一个）
    if [ -z "$worker_pid" ]; then
        local daemon_pid
        daemon_pid=$(docker exec "$CONTAINER" bash -c \
            "ps -ef | grep 'pyspark.*daemon' | grep -v grep | awk '{print \$2}' | head -1" \
            2>/dev/null || true)
        daemon_pid=$(echo "$daemon_pid" | tr -d '[:space:]')
        if [ -n "$daemon_pid" ]; then
            echo "  [Profile] Found daemon PID=$daemon_pid, looking for children..."
            worker_pid=$(docker exec "$CONTAINER" bash -c \
                "ps --ppid $daemon_pid -o pid= | tail -1" \
                2>/dev/null || true)
            worker_pid=$(echo "$worker_pid" | tr -d '[:space:]')
        fi
    fi

    # 打印所有 worker 供确认
    if [ -n "$worker_pid" ]; then
        echo "  [Profile] All pyspark workers:"
        docker exec "$CONTAINER" bash -c \
            "ps -ef | grep 'pyspark.*worker' | grep -v grep" 2>/dev/null \
            | while IFS= read -r line; do echo "    $line"; done || true
        echo "  [Profile] Selected PID=$worker_pid (last/newest)"
    fi

    echo "  [Profile] Found worker PID=$worker_pid"

    # ── 确保 profile 目录存在 ──
    docker exec "$CONTAINER" mkdir -p "$PROFILE_DIR"

    # ── 执行 perf record ──
    echo "  [Profile] Starting perf record (freq=${PERF_FREQ}Hz, duration=${PROFILE_DURATION}s)..."
    echo "  [Profile] Output: $perf_data_file"

    set +e
    docker exec "$CONTAINER" bash -c \
        "perf record -F $PERF_FREQ -g --call-graph dwarf \
         -p $worker_pid \
         -o $perf_data_file \
         -- sleep $PROFILE_DURATION" 2>&1 | \
    while IFS= read -r line; do
        echo "  [perf] $line"
    done
    local perf_exit=${PIPESTATUS[0]}
    set -e

    echo "  [Profile] perf record exit code: $perf_exit"

    # ── 验证文件 ──
    local file_size
    file_size=$(docker exec "$CONTAINER" stat -c%s "$perf_data_file" 2>/dev/null || echo "0")
    echo "  [Profile] Data file size: ${file_size} bytes"

    if [ "$file_size" = "0" ]; then
        echo "  [Profile] ⚠️  perf data file is empty"
        return 1
    fi

    if [ "$perf_exit" -ne 0 ]; then
        echo "  [Profile] perf record exited with code $perf_exit, but data file is present; continuing"
    fi

    echo "  [Profile] perf record completed"
    echo "  [Profile] ✅ Done: $perf_data_file"
}
# ────────────────────────────────────────────────────────────────
# 打印配置头
# ────────────────────────────────────────────────────────────────
echo "================================================================"
echo "  PySpark UDF Benchmark"
echo "================================================================"
echo "  Rows=$ROW_COUNT  Parallelism=$PARALLELISM  Master=$MASTER"
[ -n "$TASKSET_CORES" ] && echo "  Taskset=cores $TASKSET_CORES"
[ -n "$PROFILE_NAME" ] && echo "  Profile=$PROFILE_NAME (duration=${PROFILE_DURATION}s, freq=${PERF_FREQ}Hz)"
echo "----------------------------------------------------------------"

# ────────────────────────────────────────────────────────────────
# 表头
# ────────────────────────────────────────────────────────────────
echo ""
printf "  %-12s | %-10s | %-14s | %-18s | %s\n" \
    "Query" "UDF" "Overhead" "Throughput" "Duration"
echo "  ------------ | ---------- | -------------- | ------------------ | ----------"

# ────────────────────────────────────────────────────────────────
# 执行测试
# ────────────────────────────────────────────────────────────────
for q in "${QUERIES[@]}"; do
    cmd=$(build_cmd "$q")
    tmpfile=$(mktemp /tmp/bench_${q}_XXXXXX.log)

    if [ "$VERBOSE" = true ]; then
        echo ""
        echo "── Running: $q ──"
        echo "   CMD: docker exec -it $CONTAINER bash -c \"$cmd\""
        echo ""
    fi

    if [ -n "$PROFILE_NAME" ]; then
        # ── Profile 模式 ──
        # spark-submit 在后台运行，前台等待稳定后做 perf

        if [ "$VERBOSE" = true ]; then
            echo "  [Profile] Starting spark-submit in background..."
        fi

        # 后台启动 spark-submit，输出写入 tmpfile
        docker exec -i "$CONTAINER" bash -c "$cmd" > "$tmpfile" 2>&1 &
        SPARK_BG_PID=$!

        # 等待稳定并执行 perf
        run_profile "$tmpfile" "$PROFILE_NAME" "$SPARK_BG_PID"

        # 等待 spark-submit 完成
        if [ "$VERBOSE" = true ]; then
            echo "  [Profile] Waiting for spark-submit to finish..."
        fi

        set +e
        wait $SPARK_BG_PID
        exit_code=$?
        set -e

        # verbose 模式下打印完整输出
        if [ "$VERBOSE" = true ]; then
            echo ""
            echo "── Full output ──"
            cat "$tmpfile"
            echo ""
        fi

    else
        # ── 普通模式 ──
        set +e
        if [ "$VERBOSE" = true ]; then
            docker exec -i "$CONTAINER" bash -c "$cmd" 2>&1 | tee "$tmpfile"
        else
            docker exec -i "$CONTAINER" bash -c "$cmd" > "$tmpfile" 2>&1
        fi
        exit_code=${PIPESTATUS[0]}
        set -e

        if [ "$VERBOSE" = true ]; then
            echo ""
        fi
    fi

    if [ $exit_code -ne 0 ]; then
        printf "  %-12s | ❌ FAILED (exit code %d)\n" "$q" "$exit_code"
        if [ "$VERBOSE" = false ]; then
            # 失败时打印最后 5 行帮助诊断
            echo "  Last 5 lines:"
            tail -5 "$tmpfile" | sed 's/^/    /'
        fi
    else
        extract_metrics "$(cat "$tmpfile")" "$q"
    fi

    rm -f "$tmpfile"
done

# ────────────────────────────────────────────────────────────────
# 尾部
# ────────────────────────────────────────────────────────────────
echo ""
echo "================================================================"
echo "  Config: -c $ROW_COUNT -a $PARALLELISM --master $MASTER"
[ -n "$TASKSET_CORES" ] && echo "  Taskset: cores $TASKSET_CORES"
[ -n "$PROFILE_NAME" ] && echo "  Profile: ${PROFILE_DIR}/${PROFILE_NAME}.data"
echo "================================================================"