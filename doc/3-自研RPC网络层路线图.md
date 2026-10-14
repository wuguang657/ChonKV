# 3 · 自研 RPC 网络层路线图（学习 + 生产）

> 目标：对照 Ceph `AsyncMessenger`，从零自研一个 RPC 网络层，用来练网络编程，
> 并争取能上生产、性能更高。最聪明的玩法——做成 `cpp-6.824` 里 Raft 的 transport。
>
> 创建：2026-10-14 ｜ 关联：cpp-6.824 / io_uring 动手方向

---

## 一、先分两档目标（别一上来追求生产级）

- [ ] **档 A · 学会网络编程**：Stage 0~4，单机能跑通、能压测，成就感拉满
- [ ] **档 B · 上生产 + 性能更高**：Stage 5~7，零拷贝 / 内存池 / io_uring / 流控 / TLS / 优雅关闭 / 监控

> 关键认知：档 B 跟 Ceph 的 `AsyncMessenger` 是同一条路，不要一开始就追求。

---

## 二、Ceph 实际怎么做的（照着学，别闭门造车）

直接学**第二代 `AsyncMessenger`**（现在的生产主力；第一代 `SimpleMessenger` 每连接两线程已废弃，性能差）。

| 组件 | 作用 | 可借鉴点 |
|------|------|---------|
| `EventCenter` | 每个 Worker 持一个 epoll 实例，注册 fd + 回调 | 你的 reactor 核心 |
| `Worker` 线程池 | 一个 Worker 绑一个 EventCenter，避免锁 | 多线程模型 |
| `AsyncConnection` | 每条连接一个状态机（CONNECT/OPEN/READ/WRITE/CLOSED） | 连接状态机 |
| `ProtocolV1/V2` | 协议协商 + 封帧 + 可选加密 | Codec / Transport 层 |
| `Dispatch` | 消息分发回调 | RPC 路由 |
| `Throttler` | 流控 / 背压，防单连接打爆内存 | 后期必做 |

**灵魂要点（记死）**：Ceph 用「1 个 epoll 实例 + N 个 Worker，连接按哈希绑到固定 Worker」，
所以同一条连接上的事件永远在同一个线程处理，**几乎不需要锁**。这是高性能 RPC 的核心。

---

## 三、技术选型

| 维度 | 学习期（够用） | 生产期（更猛） |
|------|---------------|---------------|
| IO 模型 | epoll（ET 边缘触发） | io_uring（Linux 5.1+） |
| 序列化 | protobuf | FlatBuffers / cap'n proto（零拷贝读） |
| 内存 | 裸 buffer + `std::string` | 内存池 + `iovec` 散射聚合 + bufferlist |
| 线程模型 | 单 reactor | N Worker，连接绑定线程 |
| 协议 | length-prefixed header | + 魔数 + 校验和 + 可选 TLS |
| 关联 | seq → `std::map<seq, promise>` | 同上，放 Worker 本地 |

---

## 四、分阶段路线图（每档都有可验证里程碑）

- [ ] **Stage 0 · 地基**：非阻塞 socket、ET 模式、`EPOLLIN/EPOLLOUT` 切换、`accept` 惊群
  - [ ] 里程碑：手写一个能扛 C10K 连接的 echo server
- [ ] **Stage 1 · Reactor**：单线程 epoll 事件循环 + 回调注册
  - [ ] 里程碑：`telnet` 连上能收发
- [ ] **Stage 2 · 封帧**：`[magic][len][seq][payload]` 定长头 + 变长体，解决半包/粘包
  - [ ] 里程碑：发 1MB 大包不串
- [ ] **Stage 3 · RPC 语义**：`seq` 关联请求/响应，客户端 `future`/回调；服务端方法注册表（`service.method -> handler`）
  - [ ] 里程碑：客户端 `call()` 阻塞/异步拿到结果
- [ ] **Stage 4 · 多线程**：N 个 Worker 各持 epoll，连接哈希绑定
  - [ ] 里程碑：多核压测 QPS 随核数近线性涨
- [ ] **Stage 5 · 零拷贝**：内存池 + `writev`/`readv`(iovec)，避免每次收发 `memcpy`
  - [ ] 里程碑：perf 看 `copy_user` 占比下降
- [ ] **Stage 6 · io_uring**：用 `io_uring` 替换 epoll 做提交/完成，省系统调用
  - [ ] 里程碑：同硬件 QPS 再涨一截
- [ ] **Stage 7（可选）· 内核旁路**：RDMA（ibverbs）/ DPDK（Ceph `AsyncMessenger` 也支持）
  - [ ] 里程碑：延迟降到微秒级（成本陡增，离生产落地最远）

---

## 五、新手必踩的 7 个坑（提前标记）

- [ ] **1. ET 模式必须循环读写到底**（`while (read()>0)`），否则事件丢一次就再不触发
- [ ] **2. 写不完要挂 `EPOLLOUT`**，可写时再补发，别阻塞 reactor
- [ ] **3. 半包/粘包**：TCP 是字节流，必须按 `header.len` 攒够再解
- [ ] **4. 跨线程碰连接**：没绑定 Worker 就上锁，或走线程间队列（推荐）
- [ ] **5. 背压缺失**：对端慢时本地 buffer 无限涨 → OOM，要 Throttler
- [ ] **6. 优雅关闭**：`EPOLLRDHUP` + 半关闭（`shutdown(WR)`），别直接 `close` 丢数据
- [ ] **7. 超时/心跳**：请求要有 deadline，连接要有保活，否则僵尸连接吃资源

---

## 六、与现有路径的咬合点

- [ ] **接进 cpp-6.824**：Stage 3 完成后，把此 RPC 作为 Raft 的 transport，替换现有 stub/labrpc
  - 直接得到「能跑 Raft 的高性能网络层」，最有说服力的产出
- [ ] **io_uring**：正好是当前动手方向，Stage 6 顺理成章
- [ ] **面试价值**：能讲「对照 Ceph AsyncMessenger 自己实现 reactor + 零拷贝 RPC，并拿它跑了 Raft」
  - 比背八股强十倍（XSKY 存储内核岗尤其吃这套）

---

## 七、下一步动作（待确认）

- [ ] 是否在桌面起 `rpc-learner` 最小骨架（Stage 1+2：C++17、epoll 边缘触发 reactor + length-prefixed 封帧，单文件可编译可跑，带 Makefile + 测试脚本）
  - 用户先自己读、自己加 Stage 3 的 RPC 语义，Nova 负责审查
