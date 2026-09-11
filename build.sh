#!/usr/bin/env bash
# build.sh —— 一键编译 + 跑测试
#
#   ./build.sh              编译，跑全部测试（Lab 2 的 Raft + Lab 3 的 KV）
#   ./build.sh 2A           只跑 2A（自动识别为 Raft 测试）
#   ./build.sh 3A           只跑 3A（自动识别为 KV 测试）
#   ./build.sh 3B -count 5  3B 跑 5 遍（抓偶发 bug 的必备姿势）
#   ./build.sh --tsan       开线程 sanitizer 查数据竞争（会慢 5~15 倍）
#   ./build.sh --clean      清空编译产物
#
# 过滤词以 2 开头 → 跑 raft_test；以 3 开头 → 跑 kv_test；不写 → 两个都跑。
#
# 本质上就是 cmake 的一层薄封装，你随时可以直接用 cmake 命令。

set -euo pipefail
cd "$(dirname "$0")"

DIR=build
EXTRA=()
ARGS=()

for a in "$@"; do
  case "$a" in
    --tsan)     EXTRA+=("-DENABLE_TSAN=ON");  DIR=build-tsan ;;
    --asan)     EXTRA+=("-DENABLE_ASAN=ON");  DIR=build-asan ;;
    --clean)    rm -rf build build-tsan build-asan; echo "已清理"; exit 0 ;;
    *)          ARGS+=("$a") ;;
  esac
done

cmake -S . -B "$DIR" -DCMAKE_BUILD_TYPE=Debug "${EXTRA[@]:-}" > /dev/null
cmake --build "$DIR" -j "$(sysctl -n hw.ncpu 2>/dev/null || echo 8)"

echo
echo "=== 先跑网络层自测 ==="
"./$DIR/labrpc_selftest" || true

# 按过滤词挑要跑哪个二进制：2* → Raft，3* → KV，都没写 → 两个都跑
FILTER="${ARGS[0]:-}"
case "$FILTER" in
  2*)  BINS=(raft_test) ;;
  3*)  BINS=(kv_test) ;;
  *)   BINS=(raft_test kv_test) ;;
esac

for b in "${BINS[@]}"; do
  case "$b" in
    raft_test) TITLE="Raft 测试 (Lab 2)" ;;
    kv_test)   TITLE="KV 测试 (Lab 3)" ;;
    *)         TITLE="$b" ;;
  esac
  echo
  echo "=== $TITLE ==="
  "./$DIR/$b" "${ARGS[@]:-}"
done
