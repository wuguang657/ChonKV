// raft_util.h —— Raft 的公共设施：CRC32 / 快照 blob 编解码 / Trace 调试日志 / 计票纯函数
//
// 从 raft.h 与 raft.cpp 拆出（代码重组 L1）。这里的声明被 raft.cpp /
// persist/log/segment_log_store.cpp / test_raft.cpp 共同使用，收拢到单一入口，依赖方向
// 保持单向干净：raft.h → raft_util.h → raft_protocol.h。
//
// 注意 Trace 的 inline 化：C++17 inline 变量/函数保证多 TU include 时单实例
// （g_t0 时间基准全进程唯一）；TraceOn() 函数内 static 缓存每 TU 一份，
// 但缓存值只是「环境变量是否设置」的布尔量，各 TU 一致，行为等价。

#pragma once

#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

#include "../common/util.h"
#include "raft_protocol.h"

namespace raft {

// ---------------------------------------------------------------------------
// 调试 Trace（环境变量 RAFT_LOG=1 开启；零成本关闭——TraceOn() 静态缓存 +
// 函数首行 return，关闭时只剩一次已缓存的布尔判断）
// ---------------------------------------------------------------------------
inline raftcpp::TimePoint g_t0 = raftcpp::Now();
inline bool TraceOn() {
  static const int v = (std::getenv("RAFT_LOG") != nullptr) ? 1 : 0;
  return v == 1;
}
inline void Trace(const char* fmt, ...) {
  if (!TraceOn()) return;
  va_list ap; va_start(ap, fmt);
  std::fprintf(stderr, "[t=%lld] ", (long long)raftcpp::MillisSince(g_t0));
  std::vfprintf(stderr, fmt, ap);
  std::fprintf(stderr, "\n");
  va_end(ap);
}

// ---------------------------------------------------------------------------
// 磁盘上 snapshot blob 的自描述头（P1-1：V2 起带 CRC，防 bit rot / 半截写）
// ---------------------------------------------------------------------------
constexpr size_t kSnapHeaderSizeV1 = 8 + 4 + 4 + 4;        // V1: magic+index+term+len
constexpr size_t kSnapHeaderSizeV3 = 8 + 8 + 8 + 4 + 4;    // V3: magic+index(8B)+term(8B)+len+crc
constexpr size_t kSnapHeaderSize = kSnapHeaderSizeV1 + 4;  // V2: 再 +crc(4B)
std::string EncodeSnapshotBlob(int64_t index, int64_t term, const std::string& raw);
// corrupt 出参（默认 nullptr → 老调用点零改动）：
//   true  = blob 是本格式（magic 对得上）但内容/长度对不上 = 真损坏 → 调用方应 fail-stop；
//   false = 空 / 老格式 / 非本格式 = 视为无快照（首次启动的合法情形，绝不能 abort）。
bool DecodeSnapshotBlob(const std::string& blob, int64_t* index, int64_t* term,
                        std::string* raw, bool* corrupt = nullptr);

// ---------------------------------------------------------------------------
// §2.1 快照分块：CRC32（IEEE 802.3 多项式）工具 —— 用于分块传输完整性校验。
// 完整快照 blob 在 leader 侧算一次 CRC32，随每条 chunk 带上；follower 收到 done
// 重组出完整 blob 后重算并比对，不一致（半截写 / 分块丢失 / 损坏）则拒绝安装。
// 截断成 int 仅用于「相等比较」，双射不丢区分度。
// ---------------------------------------------------------------------------
int Crc32(const char* data, size_t n);
// 增量 CRC：在「已算好的 crc」基础上续算一段数据，返回新的 CRC。
// 用途：算 [头‖payload] 的整体 CRC 时，不必先拼出一份几百 MB 的临时串（零额外拷贝）。
// 约定：初值传 0（空串的 CRC32 就是 0）即等价于 Crc32(data, n)。
uint32_t Crc32Update(uint32_t crc, const char* data, size_t n);
// 底层 CRC32 原语（不做末态取反的「原始循环」），供 Crc32Update 与 SegmentLogStore 共用。
uint32_t Crc32Raw(uint32_t crc, const char* data, size_t n);

// ===========================================================================
// 选举计票：纯函数，便于单元测试直接构造输入验证口径（见 test_raft.cpp）
// ===========================================================================
// 只统计"当前仍是 voter"的节点投出的赞成票。
// 必须与 QuorumSizeLocked()（MemberCountLocked 只数 IsVoter）保持
// 【分子分母同口径】：否则已移除 / learner 节点的票会混进分子，却用
// "当前 voter 多数"作门槛 —— 候选者可能靠一张已作废的票凑够 quorum
// 当选，leader 并非由当前配置多数选出（选举合法性被破坏）。
//
//   votes[i]: 0 = 未投/未回, 1 = 授予, -1 = 拒绝
int CountGrantedVotes(const std::vector<MemberRole>& is_member,
                      const std::vector<int>& votes);

}  // namespace raft
