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
  target_compile_options(raft_test PRIVATE -fsanitize=thread -g ${SAN_OPT})
  target_link_options(raft_test PRIVATE -fsanitize=thread)
  message(STATUS ">>> ThreadSanitizer 已开启")
endif()

if(ENABLE_ASAN)
  target_compile_options(raft_test PRIVATE -fsanitize=address -fno-omit-frame-pointer -g ${SAN_OPT})
  target_link_options(raft_test PRIVATE -fsanitize=address)
  message(STATUS ">>> AddressSanitizer 已开启")
endif()

if(ENABLE_UBSAN)
  # 运行期建议配 UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1（test_part.sh
  # 的 SAN=ubsan 已自动设置）—— 否则 UBSan 只打印不中断，测试照样"绿"。
  target_compile_options(raft_test PRIVATE -fsanitize=undefined -fno-omit-frame-pointer -g ${SAN_OPT})
  target_link_options(raft_test PRIVATE -fsanitize=undefined)
  message(STATUS ">>> UBSan 已开启")
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
  target_compile_options(kv_test PRIVATE -fsanitize=thread -g ${SAN_OPT})
  target_link_options(kv_test PRIVATE -fsanitize=thread)
endif()

if(ENABLE_ASAN)
  target_compile_options(kv_test PRIVATE -fsanitize=address -fno-omit-frame-pointer -g ${SAN_OPT})
  target_link_options(kv_test PRIVATE -fsanitize=address)
endif()

if(ENABLE_UBSAN)
  target_compile_options(kv_test PRIVATE -fsanitize=undefined -fno-omit-frame-pointer -g ${SAN_OPT})
  target_link_options(kv_test PRIVATE -fsanitize=undefined)
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
