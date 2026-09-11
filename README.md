# cpp-6.824

```
cpp-6.824/
├── src/
│   ├── common/           # 工具：随机数、时间、channel、线程跟踪器
│   ├── labrpc/           # 网络仿真层（丢包/延迟/乱序/断连/字节统计）
│   ├── raft/             # Lab 2
│   │   ├── raft.h        # 【不用改】数据结构 + Raft 类声明 + 详细注释
│   │   ├── raft.cpp      # ★ 你的主战场：只有 TODO 要你填
│   │   ├── persister.h   # 【不用改】模拟磁盘
│   │   ├── config.h/.cpp # 【不用改】测试脚手架
│   │   └── test_raft.cpp # 【不用改】22 个测试用例（2A/2B/2C + 2D 快照）+ main
│   └── kvraft/           # Lab 3：跑在 Raft 之上的 KV 服务（复用同一份 raft.cpp）
│       ├── common.h      # Op / Get / PutAppend 消息 + 序列化
│       ├── server.h/.cpp # KVServer：状态机 + exactly-once + 快照
│       ├── client.h/.cpp # Clerk：客户端重试逻辑
│       ├── config.h/.cpp # 测试脚手架
│       └── test_kvraft.cpp # 19 个测试用例（3A/3B）+ main
├── doc/                  # 分阶段任务书 + 测试入门
├── CMakeLists.txt
└── build.sh              # 一键编译 + 跑测试
```

---

## 30 秒上手

```bash
cd cpp-6.824
./build.sh                # 编译 + 跑网络层自测 + 跑全部 Raft 测试
./build.sh 2A             # 只跑 2A
./build.sh 2B -count 10   # 2B 跑 10 遍（抓偶发 bug）
```

第一次跑，22 个用例**全都会失败**——这是对的，`raft.cpp` 里的
TODO 全空着。按下面的顺序一个个填。

---
## 环境要求
| 东西 | 版本要求 | 检查命令 |
|---|---|---|
| 编译器 | 支持 C++17（clang++ / g++ 都行） | `clang++ --version` |
| CMake | ≥ 3.14 | `cmake --version` |
| 线程库 | POSIX threads | macOS / Linux 自带 |

Apple Silicon Mac 自带的 `clang++`（Xcode Command Line Tools）完全够用，
不用额外装任何东西。

## 排错三板斧

```bash
# 1. 偶发失败？重复跑，把概率性问题变成必现
./build.sh 2B -count 20

# 2. 怀疑数据竞争？开 TSAN（慢 5~15 倍，但一抓一个准）
./build.sh --tsan 2B

# 3. 看不清状态机在干嘛？开 trace
RAFT_LOG=1 ./build/raft_test 2B
```

`Config::DumpState()` 会在 `one()` 失败时把每个节点的
term / state / commitIndex / logLen 打到 stderr，先看这个。

---

## 统计数字怎么读

测试输出形如：

```
  ... Passed --   3.2  3  121    1088    6
                   │   │   │       │     └─ 本次新增的已提交命令数
                   │   │   │       └─ 总字节数
                   │   │   └─ 总 RPC 次数
                   │   └─ 节点数
                   └─ 耗时（秒）
```

`TestCount2B` 和 `TestRPCBytes2B` 就是查这两列的：
- 空闲 1 秒的 RPC 增量 > 60 → 心跳发太频繁或线程在空转；
- 复制 N 条命令的字节数远超 `N × 节点数 × 命令大小` → 同一批日志被重复发送。
