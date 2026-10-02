#!/usr/bin/env bash
# build.sh —— 一键编译 + 跑测试
#
#   ./build.sh              编译，跑全部测试（Lab 2 的 Raft + Lab 3 的 KV）
#   ./build.sh 2A           只跑 2A（自动识别为 Raft 测试）
#   ./build.sh 3A           只跑 3A（自动识别为 KV 测试）
#   ./build.sh 3B -count 5  3B 跑 5 遍（抓偶发 bug 的必备姿势）
#   ./build.sh --tsan       开线程 sanitizer 查数据竞争（会慢 5~15 倍）
#   ./build.sh --asan       开 AddressSanitizer 查内存越界/UAF
#   ./build.sh --ubsan      开 UBSan 查未定义行为（可与 --asan/--tsan 组合）
#   ./build.sh --release    Release 版（-O3 + NDEBUG，build-release*/，可叠加其他开关）
#   ./build.sh --opt        开 LTO 优化（Debug 下附赠 -O2；可与 --tsan/--asan/--release 叠加）
#   ./build.sh --clean      清空编译产物
#
# 组合规则：--tsan 与 --asan 互斥（clang 拒绝同开）；目录 build-{tsan|asan|ubsan|asan-ubsan|tsan-ubsan}[-release]
#
# 过滤词以 2 开头 → 跑 raft_test；以 3 开头 → 跑 kv_test；不写 → 两个都跑。
#
# 本质上就是 cmake 的一层薄封装，你随时可以直接用 cmake 命令。

set -euo pipefail
cd "$(dirname "$0")"

DIR=build
EXTRA=()
ARGS=()
REL=0  # --release 标记（build-type 用；目录在循环后统一推导，支持 --release --tsan 组合）
TS=0   # 同上；set -u 下必须初始化
AS=0
UB=0

for a in "$@"; do
  case "$a" in
    --tsan)     EXTRA+=("-DENABLE_TSAN=ON");   TS=1 ;;
    --asan)     EXTRA+=("-DENABLE_ASAN=ON");   AS=1 ;;
    --ubsan)    EXTRA+=("-DENABLE_UBSAN=ON");  UB=1 ;;
    --release)  REL=1 ;;  # Release 版（-O3 + NDEBUG），目录 build-release*
    --opt)      EXTRA+=("-DENABLE_OPT=ON") ;;  # LTO（Debug 下附赠 -O2；可与上三者叠加）
    --clean)    rm -rf build build-tsan build-asan build-ubsan build-asan-ubsan \
                       build-tsan-ubsan build-release build-release-tsan \
                       build-release-asan build-release-ubsan build-release-asan-ubsan \
                       build-release-tsan-ubsan build-release-tsan; echo "已清理"; exit 0 ;;
    *)          ARGS+=("$a") ;;
  esac
done

# TSan 与 ASan 同开 clang 直接拒绝编译，提前拦下给出人话报错
if (( TS && AS )); then
  echo "错误：--tsan 与 --asan 不能同时开（clang 不允许；要两者兼顾请分开跑）" >&2
  exit 2
fi

# UBSan 默认只打印不中断（halt_on_error=0），UB 测试会"假绿"。
# 必须 halt_on_error=1 让它在遇到未定义行为时真正中断并打栈，才能抓到。
# （test_part.sh 已设，这里补齐，两条入口保持一致）
if (( UB )); then
  export UBSAN_OPTIONS="halt_on_error=1:print_stacktrace=1"
fi

# 目录推导：sanitizer 与 Release 各占独立目录（避免配置互踩整目录重编）。
# 显式 if 而非 "(( )) && cmd"：后者在 if 块尾部 + set -e 下有 errexit 歧义。
SAN_DIR=""
if (( TS )); then SAN_DIR=tsan; fi
if (( AS )); then SAN_DIR="${SAN_DIR:+${SAN_DIR}-}asan"; fi
if (( UB )); then SAN_DIR="${SAN_DIR:+${SAN_DIR}-}ubsan"; fi
if (( REL )); then
  BUILD_TYPE=Release
  if [[ -n "$SAN_DIR" ]]; then DIR="build-release-$SAN_DIR"; else DIR=build-release; fi
else
  BUILD_TYPE=Debug
  if [[ -n "$SAN_DIR" ]]; then DIR="build-$SAN_DIR"; else DIR=build; fi
fi

cmake -S . -B "$DIR" -DCMAKE_BUILD_TYPE="$BUILD_TYPE" "${EXTRA[@]:-}" > /dev/null
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
