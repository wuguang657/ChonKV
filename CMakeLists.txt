cmake_minimum_required(VERSION 3.14)
project(cpp6824 CXX)

# ===========================================================================
# 编译选项说明
#
#   cmake -S . -B build                 用你的 src/raft/raft.cpp（骨架）
#   cmake -S . -B build -DENABLE_TSAN=ON   开线程 sanitizer，查数据竞争
#
# 然后用 ./build.sh 或 make -C build -j
# ===========================================================================

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()

option(ENABLE_TSAN  "开启 ThreadSanitizer，用来查数据竞争（会慢 5~15 倍）" OFF)
option(ENABLE_ASAN  "开启 AddressSanitizer，用来查内存越界/Use-After-Free" OFF)
option(ENABLE_UBSAN "开启 UBSan，用来查未定义行为（有符号溢出/越界移位/null 解引用等；可与 ASan 组合）" OFF)
# -O2 + LTO 优化开关：./build.sh --opt 或 test_part.sh OPT=1 打开。
# 默认 OFF 是有意的：优化会改变指令时序，测试结果面可能变化，
# 要和 Debug 基线的"已知绿/已知挂"清单保持一致时请保持默认。
option(ENABLE_OPT  "启用 -O2 + LTO 优化（默认关，保持 Debug 测试基线）" OFF)

# TSan 与 ASan 互斥：clang 编译期直接拒绝同开。这里提前 FATAL_ERROR 给出人话，
# 避免有人绕过脚本直接 cmake -DENABLE_TSAN=ON -DENABLE_ASAN=ON 时拿到晦涩报错。
if(ENABLE_TSAN AND ENABLE_ASAN)
  message(FATAL_ERROR "TSan 与 ASan 互斥，不能同开（clang 会拒绝）。请分开跑。")
endif()

# sanitizer 分支的优化级：默认 -O1（插桩下的保守选择），ENABLE_OPT 时提到 -O2。
# TSan/ASan 官方支持 -O2 —— 插桩发生在优化管道内，优化不会漏报/误报。
set(SAN_OPT -O1)
if(ENABLE_OPT)
  # -O2：显著提速（sanitizer 插桩版的 5~15 倍慢主要来自"插桩 + 未优化代码"）。
  # Release 下跳过 —— CMAKE_CXX_FLAGS_RELEASE 自带 -O3，再叠 -O2 反而把 -O3 降级。
  if(NOT CMAKE_BUILD_TYPE STREQUAL "Release")
    add_compile_options(-O2)
  endif()
  # LTO：交给 CMAKE_INTERPROCEDURAL_OPTIMIZATION，由 CMake 同时管理编译与链接
  #      两侧（AppleClang/GCC/Clang 通吃），比手塞 -flto 到 CXX_FLAGS 更可靠
  #      （后者只保证进编译行，链接行为随生成器而异）。Release 下照常生效
  #      → --release --opt 组合 = -O3 + LTO。
  set(CMAKE_INTERPROCEDURAL_OPTIMIZATION ON)
  set(SAN_OPT -O2)
  message(STATUS ">>> 优化已开启: -O2 + LTO")
endif()

# 这两个默认 OFF 是有意的：带 sanitizer 的二进制慢 5~15 倍，日常跑测试不能忍。
# 但它也意味着【默认编出来的二进制抓不到 data race】—— Go 那边是 `go test -race`
# 运行时一键开关，C++ 必须另编一个目录。
#
# 所以：别指望"反正平时跑得好好的"，请定期跑一次 ./tsan.sh（项目根）。
# 它等价于 Go 的 `go test -race`，是唯一能自动抓竞态的手段。
# 每次跑测试时启动横幅也会打印当前 sanitizer 状态，避免忘了这件事。

# ---------------------------------------------------------------------------
# sanitizer 施加器：所有测试目标都必须走这一处。
# ---------------------------------------------------------------------------
function(cpp6824_apply_sanitizers tgt)
  if(ENABLE_TSAN)
    target_compile_options(${tgt} PRIVATE -fsanitize=thread -g ${SAN_OPT})
    target_link_options(${tgt} PRIVATE -fsanitize=thread)
  endif()
  if(ENABLE_ASAN)
    target_compile_options(${tgt} PRIVATE -fsanitize=address -fno-omit-frame-pointer -g ${SAN_OPT})
    target_link_options(${tgt} PRIVATE -fsanitize=address)
  endif()
  if(ENABLE_UBSAN)
    target_compile_options(${tgt} PRIVATE -fsanitize=undefined -fno-omit-frame-pointer -g ${SAN_OPT})
    target_link_options(${tgt} PRIVATE -fsanitize=undefined)
  endif()
endfunction()

set(RAFT_SRC ${CMAKE_CURRENT_SOURCE_DIR}/src/raft/raft.cpp)
message(STATUS ">>> 使用你的实现: src/raft/raft.cpp")

add_executable(raft_test
  ${CMAKE_CURRENT_SOURCE_DIR}/src/labrpc/labrpc.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/labrpc/socket/socket_transport.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/labrpc/epoll/poller.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/labrpc/epoll/epoll_transport.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/labrpc/inproc/network.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/raft/config.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/raft/test_raft.cpp
  ${RAFT_SRC}
  ${CMAKE_CURRENT_SOURCE_DIR}/src/persist/log/segment_log_store.cpp
)

# src/ 用来引 "common/..."、"labrpc/..."；src/raft 用来引 "raft.h"
# （这样你的实现 src/raft/raft.cpp 能 #include "raft.h"，不用改成 "raft/raft.h"）
# 持久化域在 src/persist/：log_store.h（接口）+ log/（段文件·内存两后端）+
# state/（RaftState 持久化）。labrpc 的传输后端在 src/labrpc/：inproc/（同进程
# 模拟，默认）+ socket/（真实 TCP）。跨目录一律相对路径引用，不占 include path。
target_include_directories(raft_test PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/src
  ${CMAKE_CURRENT_SOURCE_DIR}/src/raft
)

# macOS 的 AppleClang 和 Linux 的 GCC/Clang 都吃这些 flag
target_compile_options(raft_test PRIVATE -Wall -Wextra)

find_package(Threads REQUIRED)
target_link_libraries(raft_test PRIVATE Threads::Threads)

# 三个目标的插桩统一走 cpp6824_apply_sanitizers()（见文件上半部分）：
# 早先每个目标各抄三段 if()，抄漏一个就是"假绿"，而 labrpc_selftest 正是漏的那个。
# 运行期建议配 UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1（test_part.sh
# 的 SAN=ubsan 已自动设置）—— 否则 UBSan 只打印不中断，测试照样"绿"。
cpp6824_apply_sanitizers(raft_test)

# ---------------------------------------------------------------------------
# Lab 3：KV 服务（kvraft） —— 跑 ./build/kv_test 3A / 3B
#
# 注意它【复用】了 Lab 2 的 raft.cpp，没有另起一份 —— KV 服务是跑在
# Raft 之上的，两边必须共用同一个共识实现。
# ---------------------------------------------------------------------------
add_executable(kv_test
  ${CMAKE_CURRENT_SOURCE_DIR}/src/labrpc/labrpc.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/labrpc/socket/socket_transport.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/labrpc/epoll/poller.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/labrpc/epoll/epoll_transport.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/labrpc/inproc/network.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/raft/raft.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/persist/log/segment_log_store.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/kvraft/server.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/kvraft/client.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/kvraft/config.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/kvraft/test_kvraft.cpp
  # 线性一致性检查器（对应 Go 版 src/porcupine/ + src/models/kv.go）
  ${CMAKE_CURRENT_SOURCE_DIR}/src/kvraft/porcupine.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/kvraft/kv_model.cpp
  # 共享的测试失败状态（让 Config::CheckLinearizability 也能 flip g_failed）
  ${CMAKE_CURRENT_SOURCE_DIR}/src/kvraft/test_status.cpp
)

target_include_directories(kv_test PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/src
  ${CMAKE_CURRENT_SOURCE_DIR}/src/raft
)

target_compile_options(kv_test PRIVATE -Wall -Wextra)
target_link_libraries(kv_test PRIVATE Threads::Threads)

cpp6824_apply_sanitizers(kv_test)

# ---------------------------------------------------------------------------
# 一个专门跑 labrpc 自测的小程序（不依赖 Raft，先确认网络层是对的）
# ---------------------------------------------------------------------------
add_executable(labrpc_selftest
  ${CMAKE_CURRENT_SOURCE_DIR}/src/labrpc/labrpc.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/labrpc/socket/socket_transport.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/labrpc/epoll/poller.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/labrpc/epoll/epoll_transport.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/labrpc/inproc/network.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/labrpc/labrpc_selftest.cpp
)
target_include_directories(labrpc_selftest PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_compile_options(labrpc_selftest PRIVATE -Wall -Wextra)
target_link_libraries(labrpc_selftest PRIVATE Threads::Threads)
# ← 关键：与 raft_test / kv_test 同一套插桩（早先这里什么都没写，
#   于是 ./build.sh --tsan 跑的自测其实没插桩，见函数定义处的血案注释）
cpp6824_apply_sanitizers(labrpc_selftest)

# ---------------------------------------------------------------------------
# EpollNetwork 练习用自测（第三个传输后端，见 src/labrpc/epoll/）
#
# 它【不在】build.sh / test_part.sh 的用例清单里 —— 这个后端是练习骨架，
# 默认构建里它只是个"能在 macOS 上编过、但所有 RPC 立即失败"的空壳，
# 不应该出现在 80 条 raft 用例的基线里。
#
# 退出码 2 = 本平台没有 epoll（只跑了 wire 层用例）。刻意不是 0：
# 不能让"什么也没验"看起来像"通过"。
# ---------------------------------------------------------------------------
add_executable(epoll_selftest
  ${CMAKE_CURRENT_SOURCE_DIR}/src/labrpc/labrpc.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/labrpc/epoll/poller.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/labrpc/epoll/epoll_transport.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/labrpc/epoll/epoll_selftest.cpp
)
target_include_directories(epoll_selftest PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_compile_options(epoll_selftest PRIVATE -Wall -Wextra)
target_link_libraries(epoll_selftest PRIVATE Threads::Threads)
cpp6824_apply_sanitizers(epoll_selftest)

# ---------------------------------------------------------------------------
# 性能压测（bench）：raft_bench / kv_bench
#
# 与测试目标的区别：
#   * 独立 main，不参与 build.sh / test_part.sh 的任何测试编排；
#   * 计时逻辑与测试的生命周期管理隔离，互不干扰；
#   * raft_bench = 共识层开环饱和 + raft 内部埋点；kv_bench = KV 栈闭环
#     （客户端可感延迟 + 吞吐）。
#
# 埋点开关 RAFT_BENCH_METRICS（默认 OFF）：
#   开启后 raft.cpp / inproc/network.cpp 内的埋点代码才编译（metrics.h
#   整个文件被 #ifdef 包住，未开启时是空头）。默认构建【一条指令都不多】，
#   80/80 测试基线的时序不受影响 —— 这是项目铁律：影响时序的东西不进
#   默认构建。压测时才开：
#     cmake -S . -B build-bench -DRAFT_BENCH_METRICS=ON
# ---------------------------------------------------------------------------
option(RAFT_BENCH_METRICS
       "开启 raft 性能埋点（仅 bench 用；默认 OFF = 测试路径零开销）" OFF)
if(RAFT_BENCH_METRICS)
  add_compile_definitions(RAFT_BENCH_METRICS)
  message(STATUS ">>> 性能埋点已开启: RAFT_BENCH_METRICS（只用于 bench，勿用于测试基线）")
endif()

set(BENCH_COMMON_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/src/labrpc/labrpc.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/labrpc/socket/socket_transport.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/labrpc/epoll/poller.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/labrpc/epoll/epoll_transport.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/labrpc/inproc/network.cpp
)

add_executable(raft_bench
  ${BENCH_COMMON_SRC}
  ${CMAKE_CURRENT_SOURCE_DIR}/src/raft/config.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/bench/raft_bench.cpp
  ${RAFT_SRC}
  ${CMAKE_CURRENT_SOURCE_DIR}/src/persist/log/segment_log_store.cpp
)
target_include_directories(raft_bench PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/src
  ${CMAKE_CURRENT_SOURCE_DIR}/src/raft
)
target_compile_options(raft_bench PRIVATE -Wall -Wextra)
target_link_libraries(raft_bench PRIVATE Threads::Threads)
cpp6824_apply_sanitizers(raft_bench)

add_executable(kv_bench
  ${BENCH_COMMON_SRC}
  ${CMAKE_CURRENT_SOURCE_DIR}/src/raft/raft.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/persist/log/segment_log_store.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/kvraft/server.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/kvraft/client.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/kvraft/config.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/bench/kv_bench.cpp
  # 与 kv_test 相同：config.cpp 的 Fatal()/CheckLinearizability 引用这些符号
  ${CMAKE_CURRENT_SOURCE_DIR}/src/kvraft/porcupine.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/kvraft/kv_model.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/kvraft/test_status.cpp
)
target_include_directories(kv_bench PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/src
  ${CMAKE_CURRENT_SOURCE_DIR}/src/raft
)
target_compile_options(kv_bench PRIVATE -Wall -Wextra)
target_link_libraries(kv_bench PRIVATE Threads::Threads)
cpp6824_apply_sanitizers(kv_bench)
