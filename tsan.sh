#!/usr/bin/env bash
# tsan.sh —— 一键跑 ThreadSanitizer 体检（等价于 Go 版的 `go test -race`）
#
#   ./tsan.sh         默认跑 2A（快，约 1~2 分钟）
#   ./tsan.sh 2B      只跑 2B
#   ./tsan.sh 3A      只跑 3A（走 kv_test）
#   ./tsan.sh 2C -count 3
#
# 为什么需要单独一个脚本：
#   sanitizer 的二进制慢 5~15 倍，而且必须独立编一个目录（build-tsan），
#   不能跟日常用的 build/ 混。kv_test 全量在正常编译下约 8 分钟，
#   开 TSan 就是 40~120 分钟 —— 所以默认只跑一个子集，别一上来全量。
#
# 过滤词分派规则和 build.sh 一致：2* → raft_test，3* → kv_test。

set -euo pipefail
cd "$(dirname "$0")"

FILTER="${1:-2A}"
ARGS=("$@")
[ ${#ARGS[@]} -eq 0 ] && ARGS=("$FILTER")

case "$FILTER" in
  2*)  BIN=raft_test ;;
  3*)  BIN=kv_test ;;
  *)   echo "过滤词请以 2 或 3 开头（2*→raft_test，3*→kv_test），例如：./tsan.sh 2B"; exit 2 ;;
esac

echo "=== 用 -DENABLE_TSAN=ON 编译到 build-tsan/ ==="
cmake -S . -B build-tsan -DCMAKE_BUILD_TYPE=Debug -DENABLE_TSAN=ON > /dev/null
cmake --build build-tsan -j "$(sysctl -n hw.ncpu 2>/dev/null || echo 8)"

echo
echo "=== 跑 $BIN ${ARGS[*]}（TSan 下会慢很多，耐心点）==="
"./build-tsan/$BIN" "${ARGS[@]}"
