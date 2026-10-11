// segment_log_store.cpp —— SegmentLogStore 实现（append-only 段文件日志存储）
//
// 设计说明见 segment_log_store.h。本文件包含 SegmentLogStore 全部方法实现
// （Open / Append / TruncateSuffix / TruncatePrefix / Replay / Sync / ResetFromLogs，
// 及私有辅助 PutU32 / GetU32 / EncodeEntry / DecodeEntry / WriteRecord / RewriteFile /
// Rebuild / FileSizeAtOpen / NowNs）。
// 底层 CRC32 原语 Crc32Raw 定义在 raft.cpp（被 P1-1 快照校验与本文件共用），此处仅引用。
// ===========================================================================

#include "segment_log_store.h"

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>  // std::abort（坏记录 / IO 失败即 fail-stop）
#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <chrono>

namespace raft {

// ---------------------------------------------------------------------------
// SegmentLogStore
// ---------------------------------------------------------------------------

SegmentLogStore::~SegmentLogStore() {
  if (fd_ >= 0) ::close(fd_);
}

SegmentLogStore::SegmentLogStore()
    : fd_(-1), last_sync_ns_(0), dirty_(false), unsynced_(0), file_size_(0) {}

// 打开/创建 dir/raft.log.seg，回放已有记录重建内存索引。
std::unique_ptr<SegmentLogStore> SegmentLogStore::Open(const std::string& dir) {
  std::string path = dir + "/raft.log.seg";
  int fd = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);
  if (fd < 0) {
    std::fprintf(stderr, "SegmentLogStore::Open %s 失败: %s\n", path.c_str(),
                 std::strerror(errno));
    std::abort();
  }
  auto s = std::unique_ptr<SegmentLogStore>(new SegmentLogStore());
  s->fd_ = fd;
  s->Rebuild();
  return s;
}

// 追加一条（假设 index 严格连续递增：e.index == last+1）。 page cache中追加
void SegmentLogStore::Append(const LogEntry& e) {
  if (!entries_.empty() && e.index != entries_.back().index + 1) {
    std::fprintf(stderr,
                 "SegmentLogStore::Append 非连续 index=%d last=%d\n", e.index,
                 entries_.back().index);
    std::abort();
  }
  WriteRecord(e);
  entries_.push_back(e);
}

// 保留 <= index 的条目，丢弃其后（冲突覆盖）。
void SegmentLogStore::TruncateSuffix(int index) {
  size_t keep = 0;
  while (keep < entries_.size() && entries_[keep].index <= index) keep++;
  entries_.resize(keep);  // resize后cap不变，堆不释放
  RewriteFile();
  Sync(true);
}

// 丢弃 <= index 的条目（快照前缀截断）。
void SegmentLogStore::TruncatePrefix(int index) {
  size_t drop = 0;
  while (drop < entries_.size() && entries_[drop].index <= index) drop++;
  if (drop) entries_.erase(entries_.begin(), entries_.begin() + drop);
  RewriteFile();
  Sync(true);
}

// 重放：哨兵(snap_term, snap_idx) + 全部已存条目 → 还原 logs_（不含哨兵以外的历史）。
std::vector<LogEntry> SegmentLogStore::Replay(int snap_term, int snap_idx) const {
  std::vector<LogEntry> out;
  out.push_back(LogEntry{snap_term, snap_idx, Command()});
  for (const auto& e : entries_) out.push_back(e);
  return out;
}

// Group Commit：合并 fsync。force=true 忽略去抖窗口（commit 推进点用）。
void SegmentLogStore::Sync(bool force) {
  if (!dirty_) return;
  int64_t now = NowNs();
  if (!force && now - last_sync_ns_ < kGroupCommitNs &&
      unsynced_ < kMaxUnsynced)
    return;
  if (::fsync(fd_) != 0) {
    std::fprintf(stderr, "SegmentLogStore::fsync 失败: %s\n",
                 std::strerror(errno));
    std::abort();
  }
  last_sync_ns_ = now;
  dirty_ = false;
  unsynced_ = 0;
}

void SegmentLogStore::ResetFromLogs(const std::vector<LogEntry>& logs) {
  // 重启追平时，绝大多数情况下 entries_ 与 logs[1..] 已经完全一致——
  // 此时再 RewriteFile()+Sync(true) 会触发一次整文件重写 + fsync，
  // 在 file-backed crash 测试里造成 ~50% 的 liveness 超时（flaky）。
  // 先做一次 O(n) 等值校验，一致就直接跳过昂贵的重写+落盘。
  if (entries_.size() + 1 == logs.size()) {
    bool same = true;
    for (size_t i = 0; i < entries_.size(); ++i) {
      const LogEntry& e = entries_[i];
      const LogEntry& l = logs[i + 1];
      if (EncodeEntry(e) != EncodeEntry(l)) {
        same = false;
        break;
      }
    }
    if (same) return;  // 已一致：跳过整文件重写 + fsync
  }
  entries_.clear();
  for (size_t i = 1; i < logs.size(); ++i) entries_.push_back(logs[i]);
  RewriteFile();  // 仅当确实发生漂移时才用权威结构重写段文件
  Sync(true);
}

void SegmentLogStore::PutU32(uint8_t* p, uint32_t v) {
  for (int i = 0; i < 4; i++)
    p[i] = static_cast<uint8_t>((v >> (i * 8)) & 0xFF);
}
uint32_t SegmentLogStore::GetU32(const uint8_t* p) {
  uint32_t v = 0;
  for (int i = 0; i < 4; i++) v |= static_cast<uint32_t>(p[i]) << (i * 8);
  return v;
}
std::string SegmentLogStore::EncodeEntry(const LogEntry& e) {
  labrpc::Encoder en;
  en.Int(e.term)
      .Int(e.index)
      .Bytes(e.command)
      .Int(static_cast<int>(e.type))
      .Int(e.conf_server)
      .Int(e.conf_role);
  return en.Take();
}
LogEntry SegmentLogStore::DecodeEntry(const std::string& b) {
  labrpc::Decoder d(b);
  LogEntry e;
  int t = 0;
  // labrpc::Decoder::Int 返回 bool（out-param 风格），不能像 Encoder 那样链式调用，
  // 逐条调用即可（返回值忽略，靠引用写出）。
  d.Int(e.term);
  d.Int(e.index);
  d.Bytes(e.command);
  d.Int(t);
  d.Int(e.conf_server);
  d.Int(e.conf_role);
  e.type = static_cast<EntryType>(t);
  return e;
}

void SegmentLogStore::WriteRecord(const LogEntry& e) {
  std::string rec = EncodeEntry(e);
  uint32_t crc = Crc32Raw(0, rec.data(), rec.size());
  uint8_t hdr[8];
  PutU32(hdr, static_cast<uint32_t>(rec.size()));
  PutU32(hdr + 4, crc);
  // 关键：用 pwrite 显式写 file_size_ 位置，与 fd 当前偏移量解耦。
  // Rebuild() 走 pread 不会改动 fd 偏移，重启后 fd 偏移仍是 0；
  // 若这里用 ::write（顺序写，依赖 fd 偏移），第一个 Append 会从文件头
  // 覆盖掉第一条已存记录 → seg 首条错位（实测即 TestPersist12C/22C 的
  // "index 1 缺失、整体偏移一位" 发散根因）。pwrite 写 file_size_ 则恒正确。
  // pwrite先写到page cache, sync时才落到磁盘
  off_t pos = static_cast<off_t>(file_size_);
  if (::pwrite(fd_, hdr, 8, pos) != 8) {
    std::fprintf(stderr, "SegmentLogStore pwrite hdr 失败\n");
    std::abort();
  }
  if (::pwrite(fd_, rec.data(), rec.size(), pos + 8) !=
      static_cast<ssize_t>(rec.size())) {
    std::fprintf(stderr, "SegmentLogStore pwrite rec 失败\n");
    std::abort();
  }
  file_size_ += 8 + rec.size(); // 记录下次在哪里写入
  dirty_ = true;                // 有改动还没落盘
  unsynced_ += 8 + rec.size();  // 累计了多少字节还没 fsync
}

// 从内存 entries_ 整体重写段文件（截断场景用，O(条目数)，远少于每次 append 重写全日志）。
void SegmentLogStore::RewriteFile() {
  // 0就是截断直接把文件变为空文件
  if (::ftruncate(fd_, 0) != 0) {
    std::fprintf(stderr, "SegmentLogStore ftruncate 失败\n");
    std::abort();
  }
  // 相对文件开头把光标拨到0也就是文件开头
  if (::lseek(fd_, 0, SEEK_SET) != 0) {
    std::fprintf(stderr, "SegmentLogStore lseek 失败\n");
    std::abort();
  }
  file_size_ = 0;
  for (const auto& e : entries_) WriteRecord(e);
  // RewriteFile 后由调用方 Sync(true) 负责持久化
}

// 打开时回放：逐条 pread，CRC 不符即停（尾部半截安全丢弃）；中段不连续 = 真损坏 → abort。
void SegmentLogStore::Rebuild() {
  entries_.clear();
  off_t off = 0;
  // 提前拿到物理文件大小：用于区分「尾部半截」与「中段损坏」（见下方 CRC 分支）。
  off_t sz = FileSizeAtOpen();
  // 每条 entry 都有自己独立的 8 字节 [len][crc] 头 + body
  while (true) {
    uint8_t hdr[8];
    ssize_t r = ::pread(fd_, hdr, 8, off);
    if (r != 8) break;
    uint32_t len = GetU32(hdr);
    uint32_t crc = GetU32(hdr + 4);
    if (len == 0 || len > (1u << 28)) break;  // 非法长度 → 损坏/半截 32位 > 256MB概率为94%
    std::string buf(len, '\0');
    r = ::pread(fd_, &buf[0], len, off + 8);
    if (r != static_cast<ssize_t>(len)) break;
    if (Crc32Raw(0, buf.data(), len) != crc) {
      // CRC 不符：若这条记录之后【还有数据】（off+8+len < sz）= 中段真损坏，
      // 已提交条目都可能丢失 → fail-stop（abort）；若这条已到文件末尾（尾部半截）
      // → 安全丢弃。普通文件 pread 仅在 EOF 才短读，故短读支恒为尾部，无需区分。
      if (off + 8 + static_cast<off_t>(len) < sz) {
        std::fprintf(stderr,
                     "SegmentLogStore: 中段记录 CRC 损坏（非尾部），疑似磁盘损坏\n");
        std::abort();
      }
      break;  // 尾部半截
    }
    entries_.push_back(DecodeEntry(buf));
    off += 8 + static_cast<off_t>(len);
  }
  // 中段索引不连续 = 真损坏（非尾部半截）→ fail-stop
  for (size_t i = 1; i < entries_.size(); ++i) {
    if (entries_[i].index != entries_[i - 1].index + 1) {
      std::fprintf(stderr,
                   "SegmentLogStore: 中段索引不连续 index=%d prev=%d（疑似磁盘损坏）\n",
                   entries_[i].index, entries_[i - 1].index);
      std::abort();
    }
  }
  // 清掉可能存在的尾部半截
  if (off < sz) ::ftruncate(fd_, off);    // 把文件砍回 off，丢掉未提交的半截
  file_size_ = static_cast<size_t>(off);
}

off_t SegmentLogStore::FileSizeAtOpen() const {
  struct stat st;
  if (::fstat(fd_, &st) != 0) return 0;
  return st.st_size;
}

int64_t SegmentLogStore::NowNs() {
  return std::chrono::steady_clock::now().time_since_epoch().count();
}

}  // namespace raft
