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
#   TSAN=0 ./test_part.sh 3A      日常版：用 build/（无 TSan，快 5~15 倍）
#   SAN=none ./test_part.sh 3A    同 TSAN=0（SAN 为新的统一开关，见下）
#   SAN=asan ./test_part.sh 2B    ASan 版（内存越界/UAF，build-asan/）
#   SAN=ubsan ./test_part.sh 2B   UBSan 版（未定义行为，build-ubsan/；自动 halt_on_error）
#   SAN=asan,ubsan ./test_part.sh 2B   ASan+UBSan 组合（build-asan-ubsan/）
#   RELEASE=1 ./test_part.sh 2B   Release 版（-O3 + NDEBUG，build-release*/）
#   RELEASE=1 TSAN=1 ./test_part.sh 2B   Release+TSan 组合（build-release-tsan/）
#   OPT=1 ./test_part.sh 2A       优化版：-O2 + LTO（明显更快；改变指令时序，
#                                 结果面可能与 Debug 基线不同，回归后使用；
#                                 Release 下 OPT 只贡献 LTO，-O3 照旧）
#   ./test_part.sh CheckQuorum    生产级扩展：CheckQuorum 全部 7 条
#   ./test_part.sh ReadIndex      生产级扩展：ReadIndex 全部 10 条
#   ./test_part.sh Membership     生产级扩展：Membership 全部 27 条
#   ./test_part.sh MaxMessageSize 生产级扩展：MaxMessageSize 全部 3 条
#
# 二进制分派（与 tsan.sh 一致）：2*/生产级扩展 → raft_test，3* → kv_test
#
# 也可显式指定二进制（编译配置按其所在目录名自动判断）：
#   BIN=./build/kv_test COUNT=100 ./test_part.sh 3A
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
TSAN="${TSAN:-0}"           # 旧开关：TSAN=0 → 无 TSan（build/）；TSAN=1 → TSan 版（build-tsan/）
# SAN：统一的 sanitizer 选择，优先级高于 TSAN。取值：
#   tsan（默认）| asan | ubsan | asan,ubsan（组合）| none
# TSan 与 ASan 互斥（clang 拒绝同开）；UBSan 可与任一组合。
SAN="${SAN:-}"
if [[ -z "$SAN" ]]; then
  if [[ "$TSAN" == "0" ]]; then SAN=none; else SAN=tsan; fi
fi
case "$SAN" in
  tsan)       SAN_SUBDIR=tsan;       TSAN_FLAG=ON;  ASAN_FLAG=OFF; UBSAN_FLAG=OFF ;;
  asan)       SAN_SUBDIR=asan;       TSAN_FLAG=OFF; ASAN_FLAG=ON;  UBSAN_FLAG=OFF ;;
  ubsan)      SAN_SUBDIR=ubsan;      TSAN_FLAG=OFF; ASAN_FLAG=OFF; UBSAN_FLAG=ON ;;
  asan,ubsan) SAN_SUBDIR=asan-ubsan; TSAN_FLAG=OFF; ASAN_FLAG=ON;  UBSAN_FLAG=ON ;;
  none)       SAN_SUBDIR="";         TSAN_FLAG=OFF; ASAN_FLAG=OFF; UBSAN_FLAG=OFF ;;
  *) echo "未知 SAN: $SAN（支持 tsan|asan|ubsan|asan,ubsan|none）" >&2; exit 2 ;;
esac
# UBSan 默认只打印不中断，压测必须让它炸出来才算抓到
if [[ "$UBSAN_FLAG" == "ON" ]]; then
  export UBSAN_OPTIONS="halt_on_error=1:print_stacktrace=1"
fi
RELEASE="${RELEASE:-0}"     # RELEASE=1 → Release 版（build-release*/，-O3 + NDEBUG；与 TSAN 正交）
OPT="${OPT:-0}"             # OPT=1 → LTO（Release 下只加 LTO；Debug 下再附赠 -O2；与 TSAN/RELEASE 正交）
LOGDIR=testdir

# 按 part 选二进制（2*→raft_test，3*→kv_test，分派规则与 tsan.sh 一致）
# 和该部分的全部用例全名（名字即测试二进制的过滤词，用全名做子串过滤互不误伤）
case "$PART" in
  2A)
    TESTS=(
      TestInitialElection2A
      TestReElection2A
    ) ;;
  2B)
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
    TESTS=(
      # TestKVRedirectLeaderId
      # TestKVBackpressureBusy
      # TestKVSessionsEviction
      # TestKVSessionsSnapshotRoundTrip
      # TestKVSessionsDeterminismWithSnapshots
      # TestKVSessionsFenceStopsReplay
      # TestKVSessionsClientProtocol
      # TestKVSessionsTombstoneValue
      # TestKVSessionsTombstoneSnapshot
      # TestKVSessionsFollowerRead
      # TestConcurrentFollowerReadUnreliable
      # TestFollowerReadAfterSnapshot
      # TestBasic3A
      # TestConcurrent3A
      # TestUnreliable3A
      # TestUnreliableOneKey3A
      # TestOnePartition3A
      # TestManyPartitionsOneClient3A
      # TestManyPartitionsManyClients3A
      # TestPersistOneClient3A
      # TestPersistConcurrent3A
      TestPersistConcurrentUnreliable3A
      TestPersistPartition3A
      TestPersistPartitionUnreliable3A
      TestPersistPartitionUnreliableLinearizable3A
    ) ;;
  3B)
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
      TestReadIndexIgnoresNonVoterAcks
      TestReadIndexTimesOutWithoutQuorum
      TestReadIndexFollowerConcurrent
      TestReadIndexIsolatedFollowerStable
    ) ;;
  Membership)
    TESTS=(
      TestSingleNodeConfChange
      TestNoRemovingLastVoter
      TestLearnerCatchup
      TestLearnerAvailabilityWin
      TestMembershipPersistAcrossRestart
      TestLearnerNeverLeader
      TestLearnerPersistAcrossRestart
      TestConfChangeChurn
      TestRemoveLeaderSelf
      TestInstallSnapshotRestoresMembership
      TestConcurrentConfChangeLinearizable
      TestLearnerCatchupWithChurn
      TestConfChangeMidCrash
      TestRemovedNodeExcludedFromQuorum
      TestMembershipConsistencyAtQuiescence
      TestReadIndexDuringConfChange
      TestLearnerDirectlyRemoved
      TestPromoteLaggingLearnerSafe
      TestStartRejectedForNonVoter
      TestInstallSnapshotRestoresRemovedRole
      TestRemovedNodeStopsReceivingReplication
      TestRemovedNodeStaysQuiescentAfterRemoval
      TestMembershipFuzzChurn
      TestConfChangeFromMinorityLeader
      TestLearnerReadIndexRejected
      TestVoteCountIgnoresRemovedVoters
      TestRemovedFreezeCoversBothSources
    ) ;;
  MaxMessageSize)
    TESTS=(
      TestMaxUncommittedBackpressure
      TestRpcMaxMessageBytes
      TestRemovedNodeQuiesces
    ) ;;
  *)
    echo "未知 part: ${PART}（只支持 2A/2B/2C/3A/3B/CheckQuorum/ReadIndex/Membership/MaxMessageSize）" >&2
    exit 2 ;;
esac

# 按 part 分派测试二进制：3* 用例在 kv_test 里，其余（2* 与生产级扩展）在 raft_test 里
case "$PART" in
  3A|3B) TEST_NAME=kv_test ;;
  *)     TEST_NAME=raft_test ;;
esac

# 二进制选择优先级：显式 BIN > SAN 开关。显式指定 BIN 时，sanitizer 配置按
# 其所在目录名反推（含 tsan/asan/ubsan → 对应 flag ON，可叠加，build-asan-ubsan
# 即双开）；否则按 SAN 分支的 flag 走，目录名自然与配置一致。
if [[ -n "${BIN:-}" ]]; then
  BUILD_DIR="$(dirname "$BIN")"
  TSAN_FLAG=OFF
  ASAN_FLAG=OFF
  UBSAN_FLAG=OFF
  if [[ "$BUILD_DIR" == *tsan* ]]; then TSAN_FLAG=ON; fi
  if [[ "$BUILD_DIR" == *asan* ]]; then ASAN_FLAG=ON; fi
  if [[ "$BUILD_DIR" == *ubsan* ]]; then UBSAN_FLAG=ON; fi
else
  if [[ "$RELEASE" == "1" ]]; then
    BUILD_DIR=build-release
  else
    BUILD_DIR=build
  fi
  if [[ -n "$SAN_SUBDIR" ]]; then BUILD_DIR="${BUILD_DIR}-${SAN_SUBDIR}"; fi
  BIN="$BUILD_DIR/$TEST_NAME"
fi

# 注意：全角标点紧邻的变量必须加大括号 —— bash 会把全角字符的高位字节
# 当作变量名一部分，set -u 下报 unbound variable（见下方 run_one 的教训）。
BUILD_TYPE=Debug
# 显式 if 而非 "[[ ]] &&"：set -e 下短路列表在顶层命令位置有 errexit 歧义
if [[ "$RELEASE" == "1" ]]; then
  BUILD_TYPE=Release
fi
echo "=== 1/2 编译（目录 ${BUILD_DIR}，${BUILD_TYPE}/SAN=${SAN}，二进制 ${BIN}；整个压测只编这一次）==="
cmake -S . -B "$BUILD_DIR" -DCMAKE_BUILD_TYPE="$BUILD_TYPE" \
      -DENABLE_TSAN="$TSAN_FLAG" -DENABLE_ASAN="$ASAN_FLAG" -DENABLE_UBSAN="$UBSAN_FLAG" \
      -DENABLE_OPT="$OPT" > /dev/null
cmake --build "$BUILD_DIR" -j "$(sysctl -n hw.ncpu 2>/dev/null || echo 8)"

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
