#!/usr/bin/env bash
# test3B.sh —— 并发压测 3B 的【每一条】用例（TSan 版）
#
# 旧写法：./tsan.sh 3B -count 2 串行整包跑，且 tsan.sh 每调一次都 cmake
#         重编一遍；25×4 轮里绝大多数时间花在重复编译 + 串行等待上。
# 新写法：只编译一次，然后 3B 的 8 条用例各起一个独立 kv_test 进程并发跑，
#         每条用例重复 COUNT 轮、每轮换随机种子，日志按用例名分文件。
#
# 为什么可以进程级并发：
#   labrpc 是纯内存模拟网络——无真实端口、无磁盘共享文件，每个 kv_test
#   进程自带一套独立的 Network / Raft / KVServer，进程之间零共享。
#
# 用法：
#   ./test3B.sh                  每条用例各 25 轮，最多 4 条同时跑
#   COUNT=50 ./test3B.sh         每条压 50 轮
#   PARALLEL=2 ./test3B.sh       TSan 吃 CPU/内存，机器卡就把并发调小
#   固定复现某个种子：SEED=12345 ./build-tsan/kv_test <用例名>
#
# 跑非 TSan 的日常版（快 5~15 倍）：把下面 BIN 改成 ./build/kv_test，
# 或临时 BIN=./build/kv_test COUNT=100 ./test3B.sh

set -euo pipefail
cd "$(dirname "$0")"

# 硬约束：Ctrl-C / kill / 退出时把所有后台测试子进程一起带走，不留孤儿
trap 'kill 0 2>/dev/null' SIGINT SIGTERM EXIT

COUNT="${COUNT:-25}"        # 每条用例重复多少轮
PARALLEL="${PARALLEL:-4}"   # 最多同时跑几条用例（TSan 下建议 2~4）
BIN="${BIN:-./build-tsan/kv_test}"
LOGDIR=testdir

# 3B 全部用例（名字即 kv_test 的过滤词，与 src/kvraft/test_kvraft.cpp
# 的 kTests[] 一一对应；用全名做子串过滤，互不误伤）
TESTS=(
  TestSnapshotRPC3B
  TestSnapshotSize3B
  TestSnapshotRecover3B
  TestSnapshotRecoverManyClients3B
  TestSnapshotUnreliable3B
  TestSnapshotUnreliableRecover3B
  TestSnapshotUnreliableRecoverConcurrentPartition3B
  TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B
)

echo "=== 1/2 编译 TSan 二进制（整个压测只编这一次）==="
cmake -S . -B build-tsan -DCMAKE_BUILD_TYPE=Debug -DENABLE_TSAN=ON > /dev/null
cmake --build build-tsan -j "$(sysctl -n hw.ncpu 2>/dev/null || echo 8)"

mkdir -p "$LOGDIR"

# 单条用例的压测循环：在自己的进程组里跑 COUNT 轮，日志写独立文件。
# 一轮一个进程（-count 1）：某轮 watchdog abort 不影响其余轮次继续跑。
run_one() {
  local name="$1"
  local log="$LOGDIR/3B-${name}.log"
  : > "$log"
  local ok=0 fail=0
  for ((i = 1; i <= COUNT; i++)); do
    # 每轮换种子扩大竞态覆盖面（种子会打印在日志头部，失败可拿出来固定复现）
    if SEED=$((RANDOM * RANDOM + i)) "$BIN" "$name" -count 1 \
           >> "$log" 2>&1; then
      ok=$((ok + 1))
    else
      fail=$((fail + 1))
      echo "  [FAIL] $name 第 $i/$COUNT 轮（种子与现场见 ${log}）"
    fi
  done
  echo "RESULT $name: $ok/$COUNT 通过, $fail 失败" | tee -a "$log"
}

echo "=== 2/2 并发压测：${#TESTS[@]} 条用例，并发度 ${PARALLEL}，每条 ${COUNT} 轮 ==="

# 简易 worker pool（兼容 macOS 自带 bash 3.2，不用 wait -n）：
# 起满 PARALLEL 个就轮询等任意一个结束，再补新的。
pids=()
for name in "${TESTS[@]}"; do
  run_one "$name" &
  pids+=("$!")
  while (( ${#pids[@]} >= PARALLEL )); do
    sleep 2
    alive=()
    for p in "${pids[@]}"; do
      kill -0 "$p" 2>/dev/null && alive+=("$p")
    done
    # 注意：alive 可能为空（这轮 2s 内所有任务恰好都跑完）。bash 3.2 下
    # 直接写 pids=("${alive[@]}") 在 set -u 时空数组展开会报 unbound variable，
    # 必须用 ${arr[@]+...} 守卫（为空时展开成零个词）。
    pids=("${alive[@]+"${alive[@]}"}")
  done
done
wait

# 正常跑完：撤掉 EXIT 陷阱。否则 kill 0 会给整个进程组（含调用方 shell）
# 发信号，导致脚本明明成功却以"被信号杀死"收场。
trap - INT TERM EXIT

echo
echo "=== 全部结束，汇总（详细日志在 $LOGDIR/3B-<用例名>.log）==="
for name in "${TESTS[@]}"; do
  grep "^RESULT" "$LOGDIR/3B-${name}.log" | tail -1 || true
done
