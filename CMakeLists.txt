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

# 这两个默认 OFF 是有意的：带 sanitizer 的二进制慢 5~15 倍，日常跑测试不能忍。
# 但它也意味着【默认编出来的二进制抓不到 data race】—— Go 那边是 `go test -race`
# 运行时一键开关，C++ 必须另编一个目录。
#
# 所以：别指望"反正平时跑得好好的"，请定期跑一次 ./tsan.sh（项目根）。
# 它等价于 Go 的 `go test -race`，是唯一能自动抓竞态的手段。
# 每次跑测试时启动横幅也会打印当前 sanitizer 状态，避免忘了这件事。

set(RAFT_SRC ${CMAKE_CURRENT_SOURCE_DIR}/src/raft/raft.cpp)
message(STATUS ">>> 使用你的实现: src/raft/raft.cpp")

add_executable(raft_test
  ${CMAKE_CURRENT_SOURCE_DIR}/src/labrpc/labrpc.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/raft/config.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/raft/test_raft.cpp
  ${RAFT_SRC}
)

# src/ 用来引 "common/..."、"labrpc/..."；src/raft 用来引 "raft.h"
# （这样你的实现 src/raft/raft.cpp 能 #include "raft.h"，不用改成 "raft/raft.h"）
target_include_directories(raft_test PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/src
  ${CMAKE_CURRENT_SOURCE_DIR}/src/raft
)

# macOS 的 AppleClang 和 Linux 的 GCC/Clang 都吃这些 flag
target_compile_options(raft_test PRIVATE -Wall -Wextra)

find_package(Threads REQUIRED)
target_link_libraries(raft_test PRIVATE Threads::Threads)

if(ENABLE_TSAN)
  target_compile_options(raft_test PRIVATE -fsanitize=thread -g -O1)
  target_link_options(raft_test PRIVATE -fsanitize=thread)
  message(STATUS ">>> ThreadSanitizer 已开启")
endif()

if(ENABLE_ASAN)
  target_compile_options(raft_test PRIVATE -fsanitize=address -fno-omit-frame-pointer -g -O1)
  target_link_options(raft_test PRIVATE -fsanitize=address)
  message(STATUS ">>> AddressSanitizer 已开启")
endif()

# ---------------------------------------------------------------------------
# Lab 3：KV 服务（kvraft） —— 跑 ./build/kv_test 3A / 3B
#
# 注意它【复用】了 Lab 2 的 raft.cpp，没有另起一份 —— KV 服务是跑在
# Raft 之上的，两边必须共用同一个共识实现。
# ---------------------------------------------------------------------------
add_executable(kv_test
  ${CMAKE_CURRENT_SOURCE_DIR}/src/labrpc/labrpc.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/raft/raft.cpp
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

if(ENABLE_TSAN)
  target_compile_options(kv_test PRIVATE -fsanitize=thread -g -O1)
  target_link_options(kv_test PRIVATE -fsanitize=thread)
endif()

if(ENABLE_ASAN)
  target_compile_options(kv_test PRIVATE -fsanitize=address -fno-omit-frame-pointer -g -O1)
  target_link_options(kv_test PRIVATE -fsanitize=address)
endif()

# ---------------------------------------------------------------------------
# 一个专门跑 labrpc 自测的小程序（不依赖 Raft，先确认网络层是对的）
# ---------------------------------------------------------------------------
add_executable(labrpc_selftest
  ${CMAKE_CURRENT_SOURCE_DIR}/src/labrpc/labrpc.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/labrpc/labrpc_selftest.cpp
)
target_include_directories(labrpc_selftest PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(labrpc_selftest PRIVATE Threads::Threads)
