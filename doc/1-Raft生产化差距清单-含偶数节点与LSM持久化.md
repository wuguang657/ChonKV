A 组：性能（有实测支撑）​
1. KV 层 entry 批量提交（KV 层，不改动 raft）——82µs 中 90% 是共识往返；raft 开环 75k vs kv 闭环 c5 ~31k，差距就在于单条往返没有被摊薄。方案：把多个客户端 op 塞进一个日志条目，实现 ×N 提升。
2. 日志写路径异步化（raft 层，原 4+5 合并）——Start() 临界区内 log_store_->Append（encode+pwrite）耗时 3.84µs，占 71%（raft.cpp:1263）。方案：append 入队 → 写线程 fsync → 回调推进 commit；单节点也靠这条获得真并发。
3. Start 锁竞争（raft 层）——mu_wait_start 从 c1 的 40ns 涨到 c5 的 16µs，占 15%。方案：做完第 6 条后自然缓解大半，如果还不够，再考虑索引分片锁。

B 组：加固（GB 级状态或暴露公网才值得做）​
1. 快照断点续传（快照层）——raft.cpp:2181，offset==0 即清空从头重发。
2. 分片 CRC + 错片重发（快照层）——单块损坏需整快照重传。
3. mTLS / 整体 HMAC（网络层）——快照明文，可伪造 InstallSnapshot。
4. 内存→临时文件 + 流控 + 续传（快照层）——纯 GB 级运维需求。
