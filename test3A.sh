#!/usr/bin/env bash
# test3A.sh —— 并发压测 Lab 3A（无快照的容错 KV）的每一条用例。
# 所有逻辑（编译一次 / 进程级并发 / 每轮换种子 / 分用例日志）都在 test_part.sh。
# 可调环境变量：COUNT=25（每条用例轮数）、PARALLEL=4（并发度）、BIN=...（换非 TSan 二进制）
exec "$(dirname "$0")/test_part.sh" 3A
