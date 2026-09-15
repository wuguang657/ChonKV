#!/usr/bin/env bash
# test_part.sh —— 并发压测某一 Lab 部分（2A/2B/2C/3A/3B/CheckQuorum/ReadIndex）
# 的【每一条】用例（TSan 版）
#
# 这是 test2A.sh / test2B.sh / test2C.sh / test3A.sh / test3B.sh 的通用核心，
# 逻辑与 test3B.sh 完全一致：只编译一次 → 每条用例各起独立测试进程并发跑
# → 每条重复 COUNT 轮、每轮换随机种子 → 日志按 "part-用例名" 分文件汇总。
#
# 为什么可以进程级并发：
#   labrpc 是纯内存模拟网络——无真实端口、无磁盘共享文件，每个测试进程自带
#   一套独立的 Network / Raft / KVServer，进程之间零共享。
#
# 用法：
#   ./test_part.sh 2A             2A 每条用例各 25 轮，最多 4 条同时跑
#   COUNT=50 ./test_part.sh 2B    每条压 50 轮
#   PARALLEL=2 ./test_part.sh 2C  TSan 吃 CPU/内存，机器卡就把并发调小
#   ./test_part.sh CheckQuorum    生产级扩展：CheckQuorum 全部 7 条
#   ./test_part.sh ReadIndex      生产级扩展：ReadIndex 全部 10 条
#
# 跑非 TSan 的日常版（快 5~15 倍）：
#   BIN=./build/raft_test COUNT=100 ./test_part.sh 2B
#   BIN=./build/kv_test   COUNT=100 ./test_part.sh 3A
#
# 固定复现某个种子：SEED=12345 ./build-tsan/raft_test <用例全名>
# （对 Ext 组务必用全名：main() 对"filter==全名"走精确匹配，
#  否则子串 "TestReadIndex" 会误伤其余 9 个 TestReadIndex* 用例）

set -euo pipefail
cd "$(dirname "$0")"

PART="${1:?用法: ./test_part.sh 2A|2B|2C|3A|3B|CheckQuorum|ReadIndex}"

# 硬约束：Ctrl-C / kill / 退出时把所有后台测试子进程一起带走，不留孤儿
trap 'kill 0 2>/dev/null' SIGINT SIGTERM EXIT

COUNT="${COUNT:-25}"        # 每条用例重复多少轮
PARALLEL="${PARALLEL:-4}"   # 最多同时跑几条用例（TSan 下建议 2~4）
LOGDIR=testdir

# 按 part 选二进制（2*→raft_test，3*→kv_test，分派规则与 tsan.sh 一致）
# 和该部分的全部用例全名（名字即测试二进制的过滤词，用全名做子串过滤互不误伤）
case "$PART" in
  2A)
    DEFAULT_BIN=./build-tsan/raft_test
    TESTS=(
      TestInitialElection2A
      TestReElection2A
    ) ;;
  2B)
    DEFAULT_BIN=./build-tsan/raft_test
    TESTS=(
      TestBasicAgree2B
      TestRPCBytes2B
      TestFailAgree2B
      TestFailNoAgree2B
      TestConcurrentStarts2B
      TestRejoin2B
      TestBackup2B
      TestCount2B
    ) ;;
  2C)
    DEFAULT_BIN=./build-tsan/raft_test
    TESTS=(
      TestPersist12C
      TestPersist22C
      TestPersist32C
      TestFigure82C
      TestUnreliableAgree2C
      TestFigure8Unreliable2C
      TestReliableChurn2C
      TestUnreliableChurn2C
    ) ;;
  3A)
    DEFAULT_BIN=./build-tsan/kv_test
    TESTS=(
      TestBasic3A
      TestConcurrent3A
      TestUnreliable3A
      TestUnreliableOneKey3A
      TestOnePartition3A
      TestManyPartitionsOneClient3A
      TestManyPartitionsManyClients3A
      TestPersistOneClient3A
      TestPersistConcurrent3A
      TestPersistConcurrentUnreliable3A
      TestPersistPartition3A
      TestPersistPartitionUnreliable3A
      TestPersistPartitionUnreliableLinearizable3A
    ) ;;
  3B)
    DEFAULT_BIN=./build-tsan/kv_test
    TESTS=(
      TestSnapshotRPC3B
      TestSnapshotSize3B
      TestSnapshotRecover3B
      TestSnapshotRecoverManyClients3B
      TestSnapshotUnreliable3B
      TestSnapshotUnreliableRecover3B
      TestSnapshotUnreliableRecoverConcurrentPartition3B
      TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B
    ) ;;
  # ---- 生产级扩展（raft_test 的 Ext 用例）----
  # 每条进程拿【全名】当过滤词，命中 main() 的精确匹配模式 —— 这些用例名
  # 互为前缀（TestReadIndex vs TestReadIndexNoStale...），若走子串匹配
  # 一条进程就会把整组都跑了，并发隔离失效。
  CheckQuorum)
    DEFAULT_BIN=./build-tsan/raft_test
    TESTS=(
      TestCheckQuorum
      TestCheckQuorumNoSpuriousDemote
      TestCheckQuorumUnreliableNoFlap
      TestCheckQuorumUnreliableIsolated
      TestCheckQuorumMinorityPartitionKeepsLeadership
      TestCheckQuorumMinorityPartitionKeepsLeadership5
      TestCheckQuorumSustainedMinorityStable
    ) ;;
  ReadIndex)
    DEFAULT_BIN=./build-tsan/raft_test
    TESTS=(
      TestReadIndex
      TestReadIndexNoStale
      TestReadIndexConcurrent
      TestReadIndexPartitionImmediate
      TestReadIndexMajorityToleratesMinorityFailure
      TestReadIndexSnapshotCatchUp
      TestReadIndexSnapshotNoDoubleCount
      TestReadIndexUnreliable
      TestReadIndexDuringReelection
      TestReadIndexSnapshotUnreliable
    ) ;;
  *)
    echo "未知 part: ${PART}（只支持 2A/2B/2C/3A/3B/CheckQuorum/ReadIndex）" >&2
    exit 2 ;;
esac

BIN="${BIN:-$DEFAULT_BIN}"

echo "=== 1/2 编译 TSan 二进制（整个压测只编这一次）==="
cmake -S . -B build-tsan -DCMAKE_BUILD_TYPE=Debug -DENABLE_TSAN=ON > /dev/null
cmake --build build-tsan -j "$(sysctl -n hw.ncpu 2>/dev/null || echo 8)"

mkdir -p "$LOGDIR"

# 单条用例的压测循环：一轮一个进程（-count 1）：某轮 watchdog abort
# 不影响其余轮次继续跑，日志写独立文件。
run_one() {
  local name="$1"
  local log="$LOGDIR/${PART}-${name}.log"
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

echo "=== 2/2 并发压测 ${PART}：${#TESTS[@]} 条用例，并发度 ${PARALLEL}，每条 ${COUNT} 轮 ==="

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
echo "=== 全部结束，汇总（详细日志在 $LOGDIR/${PART}-<用例名>.log）==="
for name in "${TESTS[@]}"; do
  grep "^RESULT" "$LOGDIR/${PART}-${name}.log" | tail -1 || true
done
