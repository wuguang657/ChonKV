#!/usr/bin/env bash
# test_part.sh —— 并发压测某一 Lab 部分（2A/2B/2C/3A/3B/生产级扩展）的【每一条】用例。
# 唯一压测入口（原 test2A.sh / test2B.sh / test2C.sh / test3A.sh / test3B.sh 已并入）：
# 只编译一次 → 每条用例各起独立测试进程并发跑 → 每条重复 COUNT 轮、每轮换随机种子
# → 日志按 "part-用例名" 分文件汇总。
#
# 为什么可以进程级并发：
#   labrpc 是纯内存模拟网络——无真实端口、无磁盘共享文件，每个测试进程自带
#   一套独立的 Network / Raft / KVServer，进程之间零共享。
#
# 用法：
#   ./test_part.sh 2A             2A 每条用例各 25 轮，最多 4 条同时跑
#   COUNT=50 ./test_part.sh 2B    每条压 50 轮
#   PARALLEL=2 ./test_part.sh 2C  TSan 吃 CPU/内存，机器卡就把并发调小
#   PARALLEL=8 ./test_part.sh 3B  file/socket 后端也可安全并发（默认 ISO=1，见下）
#   ISO=0 ./test_part.sh 3B      旧行为：file/socket 后端自动把 PARALLEL 降为 1
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
#   ./test_part.sh 3B             快照（当前处于调试态，只跑未注释的用例）
#   ./test_part.sh ChunkedSnapshotTransmit   分块快照两条
#   ./test_part.sh CheckQuorum    生产级扩展：CheckQuorum 全部 7 条
#   ./test_part.sh ReadIndex      生产级扩展：ReadIndex 全部条数
#   ./test_part.sh Membership     生产级扩展：Membership（可能处于调试态）
#   ./test_part.sh MaxMessageSize 生产级扩展：MaxMessageSize 全部条数
#
# 二进制分派（与 tsan.sh 一致）：2*/生产级扩展 → raft_test，3* → kv_test
#
# 也可显式指定二进制（编译配置按其所在目录名自动判断）：
#   BIN=./build/kv_test COUNT=100 ./test_part.sh 3A
#
# 固定复现某个种子：SEED=12345 ./build-tsan/raft_test <用例全名>
# （对 Ext 组务必用全名：main() 对"filter==全名"走精确匹配，
#  否则子串 "TestReadIndex" 会误伤其余 9 个 TestReadIndex* 用例）
#
# 可插拔后端开关（需先合入 labrpc 可插拔传输层 / 持久化层两个 patch）：
#   网络层  LABRPC_TRANSPORT = inproc（默认，纯内存模拟）| socket|tcp|net（真实 TCP，可跨机）
#           仅 socket 后端生效：LABRPC_SOCKET_HOST（默认 127.0.0.1）、LABRPC_SOCKET_BASE（默认 19090）
#   持久化层 LABRPC_PERSIST   = memory（默认，内存）| file|disk（真落盘）
#           仅 file 后端生效：LABRPC_PERSIST_DIR（默认自动生成 /tmp/cpp6raft_*）
# 脚本读取这些变量并以默认值补全后显式传给每个测试子进程，保证：
#   - 不设任何变量时行为与原来完全一致（inproc + memory，零回归）；
#   - 设了变量时透传到被测二进制；并会在 2/2 横幅里打印当前后端，便于对照日志。
# 注意：socket / file 模式下多进程会争抢端口或落盘目录。脚本默认给每个 worker
# 独立的 LABRPC_SOCKET_BASE 端口段（ISO=1，默认），从而保持 PARALLEL 并发度；
# ISO=0 退回旧行为——自动把 PARALLEL 降为 1。落盘目录的正确性在 run_one 内
# 保证：每轮必须换新目录（Persister 构造即加载已存在目录，详见那里注释）。

set -euo pipefail
cd "$(dirname "$0")"

PART="${1:?用法: ./test_part.sh 2A|2B|2C|3A|3B|ChunkedSnapshotTransmit|SegmentLog|CheckQuorum|ReadIndex|Membership|MaxMessageSize}"

# 硬约束：Ctrl-C / kill / 退出时把所有后台测试子进程一起带走，不留孤儿
children_started=0
trap 'if [[ "$children_started" == "1" ]]; then kill 0 2>/dev/null; fi' EXIT
# 交互中断（Ctrl-C）/ 被 kill：这时子进程确实在跑，必须连组带走。
# 先撤自己的陷阱再 kill 0 —— 否则 `kill 0` 发回给自己的那个信号会再次触发本
# 处理器（自递归），退出码也就不可预测了。
trap 'trap - INT TERM EXIT; kill 0 2>/dev/null; exit 130' INT TERM

# 早退（参数/配置错误）用这个：撤掉陷阱再退出。这些分支都发生在起任何子进程
# 之前，没有任何东西需要清理。
bail() {
  trap - INT TERM EXIT
  echo "$@" >&2
  exit 2
}

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
  *) bail "未知 SAN: ${SAN}（支持 tsan|asan|ubsan|asan,ubsan|none）" ;;
esac
# UBSan 默认只打印不中断，压测必须让它炸出来才算抓到
if [[ "$UBSAN_FLAG" == "ON" ]]; then
  export UBSAN_OPTIONS="halt_on_error=1:print_stacktrace=1"
fi
RELEASE="${RELEASE:-0}"     # RELEASE=1 → Release 版（build-release*/，-O3 + NDEBUG；与 TSAN 正交）
OPT="${OPT:-0}"             # OPT=1 → LTO（Release 下只加 LTO；Debug 下再附赠 -O2；与 TSAN/RELEASE 正交）

# ---- 可插拔后端：网络层 / 持久化层（默认 in-proc 模拟 + 内存持久化，与原行为一致）----
TRANSPORT="${LABRPC_TRANSPORT:-inproc}"   # inproc（默认）| socket|tcp|net（真实 TCP）
# 后端名必须在这里就校验，不能靠 MakeTransport 自己"吼一声"。
case "$TRANSPORT" in
  inproc|memory|socket|tcp|net) ;;
  *) bail "未知 LABRPC_TRANSPORT=${TRANSPORT}（支持 inproc|memory|socket|tcp|net）。
      拼错后果【不是报错而是假绿】：MakeTransport 会回退 in-proc，
      整套 net 用例静默跑在内存网络上并通过，横幅还照抄你写的名字。" ;;
esac
PERSIST="${LABRPC_PERSIST:-memory}"       # memory（默认）| file|disk（真落盘）
SOCKET_HOST="${LABRPC_SOCKET_HOST:-127.0.0.1}"
SOCKET_BASE="${LABRPC_SOCKET_BASE:-19090}"
PERSIST_DIR="${LABRPC_PERSIST_DIR:-}"
# socket / file 后端下，多个测试进程会争抢端口 / 落盘目录。
#（ISO=1，默认）：每 worker 独立端口段 → 并行不再冲突，保持 PARALLEL
# 并发度；ISO=0 退回旧的强制串行。落盘目录不按 worker 分（按轮分，见 run_one）。
# 默认 inproc + memory 两种情况都维持原并行行为，零回归。
ISO="${ISO:-1}"
PARALLEL_ISO=0
if [[ "$TRANSPORT" != "inproc" || "$PERSIST" != "memory" ]]; then
  if [[ "$ISO" == "1" ]] && (( PARALLEL > 1 )); then
    PARALLEL_ISO=1
    echo "注意：${TRANSPORT}/${PERSIST} 后端启用并行隔离（每 worker 独立落盘目录/端口段），PARALLEL=${PARALLEL}"
  elif (( PARALLEL > 1 )); then
    echo "注意：ISO=0，${TRANSPORT}/${PERSIST} 后端多进程会争抢端口/目录，已自动将 PARALLEL 降为 1"
    PARALLEL=1
  fi
fi

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

      TestFileBackedCrashRecover
      TestFileBackedCrashMinority
      TestFileBackedCrashLeader
      TestFileBackedCrashAfterSnapshot
    ) ;;
  3A)
    TESTS=(
      TestKVDiskFullBurstThrottle
      TestKVRedirectLeaderId
      TestKVBackpressureBusy
      TestKVSessionsEviction
      TestKVSessionsSnapshotRoundTrip
      TestKVSessionsDeterminismWithSnapshots
      TestKVSessionsFenceStopsReplay
      TestKVSessionsClientProtocol
      TestKVSessionsTombstoneValue
      TestKVSessionsTombstoneSnapshot
      TestKVSessionsFollowerRead
      TestConcurrentFollowerReadUnreliable
      TestFollowerReadAfterSnapshot
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
    TESTS=(
      TestSnapshotRPC3B
      TestSnapshotSize3B
      TestSnapshotRecover3B
      TestSnapshotRecoverManyClients3B
      TestSnapshotUnreliable3B
      TestSnapshotMultiChunk3B
      TestSnapshotMultiChunkUnreliable3B
      TestSnapshotUnreliableRecover3B
      TestSnapshotUnreliableRecoverConcurrentPartition3B
      TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B
    ) ;;
  ChunkedSnapshotTransmit)
    TESTS=(
      TestChunkedSnapshotTransmit
      TestChunkedSnapshotLargeStateMachine
    ) ;;
  SegmentLog)
    TESTS=(
      TestSnapshotBlobCrc
      TestSegmentLogStore
      TestSegmentLogStoreAbort
    );;
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
      TestLearnerReadIndexForwards
      TestVoteCountIgnoresRemovedVoters
      TestRemovedFreezeCoversBothSources
    ) ;;
  MaxMessageSize)
    TESTS=(
      TestMaxUncommittedBackpressure
      TestRpcMaxMessageBytes
      TestRemovedNodeQuiesces
      TestAppendEntriesMaxBytesPerRpc
      TestRaftStateDiskQuota
    ) ;;
  *)
    bail "未知 part: ${PART}（只支持 2A/2B/2C/3A/3B/ChunkedSnapshotTransmit/SegmentLog/CheckQuorum/ReadIndex/Membership/MaxMessageSize）" ;;
esac

# 按 part 分派测试二进制：3* 用例在 kv_test 里，其余（2* 与生产级扩展）在 raft_test 里
case "$PART" in
  3A|3B) TEST_NAME=kv_test ;;
  *)     TEST_NAME=raft_test ;;
esac

# ---- 端口段规划的范围守卫（只有 socket/tcp/net 后端且并行隔离时才有意义）----
# run_one 给第 slot 个 worker 的基端口是 SOCKET_BASE + slot*1000，轮内再加
# (i%40)*20（最大 +780）。用例一多，这个算式会越过 65535：
#   SOCKET_BASE=19090 时第 46 个 worker 就已经到 65090，第 47 个直接 66090。
# labrpc 现在会在端口非法时【明确拒绝】（ParsePortStrict / PortsInRange，见
# socket_transport.cpp）——于是每条用例都以"RPC 全部立即失败"收场，屏幕上只有
# 一行 "[labrpc] LABRPC_SOCKET_BASE=66090 不是合法端口"，而现象是"整个 part 齐
# 刷刷超时"，极容易被误读成"测试或实现坏了"。这种配置错误必须在开跑前就报。
if (( PARALLEL_ISO )); then
  port_top=$(( SOCKET_BASE + (${#TESTS[@]} - 1) * 1000 + 780 ))
  if (( SOCKET_BASE < 1 || SOCKET_BASE > 65535 || port_top > 65535 )); then
    bail "错误：端口段越界。SOCKET_BASE=${SOCKET_BASE}，${#TESTS[@]} 条用例；
      每个 worker 占 1000 段、段内再用 780，最高要 bind 到 ${port_top}
      （合法范围 1..65535）。请调小 LABRPC_SOCKET_BASE，或用 ISO=0 串行跑。"
  fi
fi

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
  local name="$1" slot="$2"
  local log="$LOGDIR/${PART}-${name}.log"
  : > "$log"
  local ok=0 fail=0
  for ((i = 1; i <= COUNT; i++)); do
    # 每轮换种子扩大竞态覆盖面（种子会打印在日志头部，失败可拿出来固定复现）
    # 同时把可插拔后端开关作为 env 前缀透传给被测二进制（默认 inproc + memory）。
    # 并行隔离（PARALLEL_ISO=1）：每 worker 一段独立端口，每轮再在段内错开
    # base（%40 回绕：COUNT 再大也不会溢到隔壁 worker 的端口段）——避免
    # listen 端口在多进程间互相踩。
    # 落盘目录【绝不能按 worker 复用，甚至不能跨轮复用】
    # 自动生成 /tmp/cpp6raft_<pid>_<seq>（每进程唯一，天然无污染）。
    local w_persist_dir="$PERSIST_DIR"
    local w_socket_base="$SOCKET_BASE"
    if [[ "$PERSIST" != "memory" && -n "$PERSIST_DIR" ]]; then
      w_persist_dir="${PERSIST_DIR}_s${slot}_r${i}"
      rm -rf "$w_persist_dir"
    fi
    if (( PARALLEL_ISO )); then
      if [[ "$TRANSPORT" != "inproc" ]]; then
        w_socket_base=$(( SOCKET_BASE + slot * 1000 + (i % 40) * 20 ))
      fi
    fi
    local rc=0
    SEED=$((RANDOM * RANDOM + i)) \
       LABRPC_TRANSPORT="$TRANSPORT" LABRPC_PERSIST="$PERSIST" \
       LABRPC_SOCKET_HOST="$SOCKET_HOST" LABRPC_SOCKET_BASE="$w_socket_base" \
       LABRPC_PERSIST_DIR="$w_persist_dir" \
       "$BIN" "$name" -count 1 >> "$log" 2>&1 || rc=$?
    # ------------------------------------------------------------------
    # 【必须记下退出码 / 信号】，不能只记"失败了一次"。
    # ------------------------------------------------------------------
    if (( rc == 0 )); then
      ok=$((ok + 1))
    else
      fail=$((fail + 1))
      local why="退出码 ${rc}"
      if (( rc > 128 )); then
        why="被信号 SIG$((rc - 128)) 杀掉（rc=${rc}）"
      fi
      echo "  [FAIL] $name 第 $i/$COUNT 轮：${why}（种子与现场见 ${log}）"
      echo "  [FAIL] $name 第 $i/$COUNT 轮：${why}" >> "$log"
      if (( rc > 128 )); then
        echo "         注意：被信号杀掉通常不是被测实现的 bug —— 多半是另一份"
        echo "         test_part.sh 的 pkill（见下文「残留进程清理」那段）或系统 OOM。"
      fi
    fi
  done
  echo "RESULT $name: $ok/$COUNT 通过, $fail 失败" | tee -a "$log"
}

# 清掉可能占着 socket 基端口的残留测试进程。
# 用更宽的匹配（kv_test / raft_test，不限定 build 目录），避免"上次用 build/kv_test
# 跑、这次用 build-tsan/kv_test 跑"时旧进程漏杀、继续占着 19090 → 新进程 bind
# 静默失败 → server 起不来 → 用例卡死/被看门狗 abort（这就是 net 模式间歇 FAIL 的真凶）。

echo "=== 清理残留测试进程（${TRANSPORT} 后端要独占端口；⚠️ 这也会杀掉其它正在跑的压测，别并行开两个 part）==="
pkill -f 'kv_test' 2>/dev/null || true
pkill -f 'raft_test' 2>/dev/null || true
# 等端口真正释放（最多 ~2s）：上一个进程刚被 SIGTERM，内核不一定立刻回收，
# 立刻 bind 仍可能 EADDRINUSE。先 sleep 一下兜底（实测 pkill 后加 1s 足够稳）。
sleep 1

echo "=== 2/2 并发压测 ${PART}：${#TESTS[@]} 条用例，并发度 ${PARALLEL}，每条 ${COUNT} 轮；后端 net=${TRANSPORT} persist=${PERSIST} ==="

# 简易 worker pool（兼容 macOS 自带 bash 3.2，不用 wait -n）：
# 起满 PARALLEL 个就轮询等任意一个结束，再补新的。
pids=()
slot=0
children_started=1   # 从这里开始才有"需要连组清理"的后台子进程（见上面 EXIT 陷阱）
for name in "${TESTS[@]}"; do
  run_one "$name" "$slot" &
  pids+=("$!")
  slot=$((slot + 1))
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

bad_names=""
ok_cnt=0
for name in "${TESTS[@]}"; do
  log="$LOGDIR/${PART}-${name}.log"
  line="$(grep "^RESULT" "$log" 2>/dev/null | tail -1 || true)"
  if [[ -z "$line" ]]; then
    echo "RESULT ${name}: 缺失（没有跑完或被信号杀掉，见 $log）"
    bad_names="${bad_names} ${name}"
    continue
  fi
  echo "$line"
  # 形如 "RESULT X: 25/25 通过, 0 失败"
  if [[ "$line" == *", 0 失败" ]]; then
    ok_cnt=$((ok_cnt + 1))
  else
    bad_names="${bad_names} ${name}"
  fi
done

echo
if [[ -n "$bad_names" ]]; then
  echo "错误：${PART} 有失败用例（${ok_cnt}/${#TESTS[@]} 条全绿）：${bad_names}" >&2
  echo "      日志在 $LOGDIR/${PART}-<用例名>.log；本脚本以退出码 1 结束。" >&2
  exit 1
fi
echo "OK：${PART} 全部 ${#TESTS[@]} 条用例 × ${COUNT} 轮通过（后端 net=${TRANSPORT} persist=${PERSIST}）"

