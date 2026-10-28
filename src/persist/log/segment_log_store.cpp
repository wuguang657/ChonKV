// segment_log_store.cpp —— 段文件日志后端实现（SegmentLogStorage）+ 后端工厂
//
// 设计说明见 log_store.h 与 segment_log_store.h。底层 CRC32 原语 Crc32Raw 定义在 raft.cpp
// （被 P1-1 快照校验与本文件共用），此处仅引用。
// ===========================================================================

#include "segment_log_store.h"  // 本文件的类声明
#include "memory_log_store.h"   // 工厂 kMemory 分支需要其完整定义
#include "../log_store.h"       // LogStorage 抽象接口 + CreateLogStorage 声明

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>  // std::abort（坏记录 / IO 失败即 fail-stop）
#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <chrono>
#include <cassert>

#ifdef RAFT_BENCH_METRICS
#include "../../common/metrics.h"  // 仅埋点构建：raftcpp::BenchHoldGuard / Metrics
#endif

namespace raft {

// ---------------------------------------------------------------------------
// SegmentLogStorage（file_backed 段文件后端）
// ---------------------------------------------------------------------------

SegmentLogStorage::~SegmentLogStorage() {
  {
    std::lock_guard<std::mutex> lk(flush_mu_);
    flush_stop_ = true;
    flush_wake_cv_.notify_all();
  }
  if (flush_thread_.joinable()) flush_thread_.join();
  // 关闭前兜底：把可能还残留在页缓存、未及 flusher 落盘的字节一次性 fsync，防丢。
  if (fd_ >= 0) {
    if (dirty_) {
      if (::fsync(fd_) != 0) {
        std::fprintf(stderr, "SegmentLogStorage::~fsync 失败: %s\n",
                     std::strerror(errno));
        std::abort();
      }
    }
    ::close(fd_);
  }
}

SegmentLogStorage::SegmentLogStorage()
    : fd_(-1), dirty_(false), file_size_(0) {}

// 打开/创建 dir/raft.log.seg，回放已有记录重建内存索引。
std::unique_ptr<SegmentLogStorage> SegmentLogStorage::Open(const std::string& dir) {
  std::string path = dir + "/raft.log.seg";
  int fd = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);
  if (fd < 0) {
    std::fprintf(stderr, "SegmentLogStorage::Open %s 失败: %s\n", path.c_str(),
                 std::strerror(errno));
    std::abort();
  }
  auto s = std::unique_ptr<SegmentLogStorage>(new SegmentLogStorage());
  s->fd_ = fd;
  s->Rebuild();
  // 起专职刷盘线程（fd 已就绪、Rebuild 只读不写，安全）。
  s->flush_thread_ = std::thread(&SegmentLogStorage::FlushLoop, s.get());
  return s;
}

// 追加一条（假设 index 严格连续递增：e.index == last+1）。page cache 中追加。
void SegmentLogStorage::Append(const LogEntry& e) {
  // 【文件基准归零】DropPrefix 只清内存、不动文件，于是会短暂出现「entries_ 空、
  // 但文件里还留着旧前缀」的状态。若此时直接 WriteRecord，pwrite 仍从 file_size_
  // （旧末尾）起写，磁盘上就留下跨号断档（如 [49..70][73]）→ 重启 Rebuild 命中
  // 「中段索引不连续」abort。entries_ 为空 ⇒ 文件里那些记录必然都已被快照覆盖
  // （DropPrefix 由快照路径调用），ftruncate 到 0 不丢任何已提交数据。
  if (entries_.empty() && file_size_ > 0) RewriteFile();
  if (!entries_.empty() && e.index != entries_.back().index + 1) {
    std::fprintf(stderr,
                 "SegmentLogStorage::Append 非连续 index=%lld last=%lld\n",
                 static_cast<long long>(e.index),
                 static_cast<long long>(entries_.back().index));
    std::abort();
  }
  WriteRecord(e);
  entries_.push_back(e);
  assert(offsets_.size() == entries_.size());
}

// 保留 <= index 的条目，丢弃其后（冲突覆盖）。
// raft.cpp:1089 的 Sync(false) 一起在同一个 group commit 窗口内合并落盘。
void SegmentLogStorage::TruncateSuffix(int64_t index) {
  size_t keep = 0;
  while (keep < entries_.size() && entries_[keep].index <= index) keep++;
  entries_.resize(keep);   // resize 后 cap 不变，堆不释放
  offsets_.resize(keep);   // 偏移表跟着丢，保持 size() 与 entries_ 一致
  // 截到第 keep 条的末尾；keep==0 表示全丢（把文件清空，等价于旧行为）。
  const off_t new_size = keep > 0 ? offsets_[keep - 1] : 0;
  if (::ftruncate(fd_, new_size) != 0) {
    std::fprintf(stderr, "SegmentLogStorage ftruncate 失败: %s\n",
                 std::strerror(errno));
    std::abort();
  }
  if (::lseek(fd_, new_size, SEEK_SET) < 0) {
    std::fprintf(stderr, "SegmentLogStorage lseek 失败: %s\n", std::strerror(errno));
    std::abort();
  }
  file_size_ = static_cast<size_t>(new_size);
  dirty_ = true;  // 长度变了，析构前若没落盘要兜底 fsync
  Sync(false);    // 不阻塞：交给 flusher 与紧接的 Append 合并（见上方安全性论证）
  assert(offsets_.size() == entries_.size());
}

// 只丢弃内存镜像里 <= index 的条目，不重写文件、不 fsync（幂等）。
// Raft::Snapshot / InstallSnapshot 在【释放 mu_ 之前】调用它，保证并发进来的
// AppendEntries 看到的是「logs_ 与段镜像已对齐」的状态。
void SegmentLogStorage::DropPrefix(int64_t index) {
  size_t drop = 0;
  while (drop < entries_.size() && entries_[drop].index <= index) drop++;
  if (drop) {
    entries_.erase(entries_.begin(), entries_.begin() + drop);
    // 偏移表同步丢前缀。注意这里【不动文件】，剩下的 offsets_ 仍指向原文件的
    // 绝对位置，因此无需重算；真正的截短发生在之后的 RewriteFile。
    offsets_.erase(offsets_.begin(), offsets_.begin() + drop);
  }
}

// 丢弃 <= index 的条目（快照前缀截断）并落盘。
// DropPrefix 幂等：调用方若已在释放锁前做过内存截断，这里只剩「重写 + fsync」。
void SegmentLogStorage::TruncatePrefix(int64_t index) {
  DropPrefix(index);
  RewriteFile();
  Sync(true);
}

// 重放：哨兵(snap_term, snap_idx) + 全部已存条目 → 还原 logs_（不含哨兵以外的历史）。
std::vector<LogEntry> SegmentLogStorage::Replay(int64_t snap_term, int64_t snap_idx) const {
  std::vector<LogEntry> out;
  out.push_back(LogEntry{snap_term, snap_idx, Command()});
  for (const auto& e : entries_) {
    if (e.index <= snap_idx) continue;    // 有可能因为在持久化快照过程中，先持久化了state snapshot index而没真正截断日志（途中断电了）导致entries中有部分旧的日志没被截断掉
    out.push_back(e);
  }
  return out;
}

// Group Commit：把 fsync 交给后台 FlushLoop 合并。
//   force=true（提交点）：登记 epoch 并阻塞等待 flusher 把数据落盘后才返回
//     —— 调用方在落盘完成前绝不对外 ack，durability 语义与「原地 fsync」完全等价。
//   force=false（普通 append）：仅标记 pending 并唤醒 flusher（2ms 去抖窗口内合并），
//     不阻塞调用方。
void SegmentLogStorage::Sync(bool force) {
  std::unique_lock<std::mutex> lk(flush_mu_);
  if (force) {
    uint64_t my = ++flush_request_epoch_;
    flush_pending_ = true;
    flush_urgent_ = true;  // 提交点：缩短去抖窗口，尽快落盘
    flush_wake_cv_.notify_one();
    flush_done_cv_.wait(lk, [this, my] {
      return flush_stop_ || flush_done_epoch_ >= my;
    });
  } else {
    // fire-and-forget：仅标记待刷 + 唤醒 flusher，由其在去抖窗口内合并落盘。
    if (!flush_pending_) {
      flush_pending_ = true;
      flush_wake_cv_.notify_one();
    }
  }
}

// 专职刷盘线程：合并窗口内的多次 fsync 请求为一次 ::fsync(fd_)。
void SegmentLogStorage::FlushLoop() {
  while (true) {
    uint64_t target;
    {
      std::unique_lock<std::mutex> lk(flush_mu_);
      flush_wake_cv_.wait(lk, [this] { return flush_pending_ || flush_stop_; });
      if (flush_stop_) {
        // 关闭前最后刷一次（若有未落盘字节），保证不丢。
        if (dirty_) {
          lk.unlock();
          if (::fsync(fd_) != 0) {
            std::fprintf(stderr, "SegmentLogStorage::fsync 失败: %s\n",
                         std::strerror(errno));
            std::abort();
          }
          lk.lock();
        }
        flush_done_epoch_ = flush_request_epoch_;   // 把已受理的最大号宣告落盘
        flush_done_cv_.notify_all();                // 放行全部等待者
        return;
      }
      target = flush_request_epoch_;
      int64_t window = flush_urgent_ ? kFlushUrgentNs : kGroupCommitNs;
      int64_t deadline = NowNs() + window;
      // 去抖：在窗口内等待更多请求合并（group commit），到时或新请求到达即醒。
      while (NowNs() < deadline) {
        int64_t remain = deadline - NowNs();
        if (remain <= 0) break;
        std::cv_status st =
            flush_wake_cv_.wait_for(lk, std::chrono::nanoseconds(remain));
        target = std::max(target, flush_request_epoch_);
        // 被唤醒（新请求到达）而非超时：若此刻已有提交点在等（flush_urgent_），
        // 立即把剩余窗口缩短到紧急档，不再傻等满 2ms → committed 数据只付 ~kFlushUrgentNs。
        if (flush_urgent_ && window > kFlushUrgentNs) {
          window = kFlushUrgentNs;
          deadline = NowNs() + kFlushUrgentNs;
        }
        if (st == std::cv_status::timeout) break;
      }
      flush_pending_ = false;
      flush_urgent_ = false;
    }
    // 锁外做真正的 fsync：绝不持 raft mu_，心跳 / AE 回包 / 提案线程都不被堵。
#ifdef RAFT_BENCH_METRICS
    raftcpp::BenchHoldGuard _sync_hold{raftcpp::kLogSync};
#endif
    if (::fsync(fd_) != 0) {
      std::fprintf(stderr, "SegmentLogStorage::fsync 失败: %s\n",
                   std::strerror(errno));
      std::abort();
    }
#ifdef RAFT_BENCH_METRICS
    raftcpp::Metrics::Instance().Incr(raftcpp::kLogFsync);
#endif
    {
      std::lock_guard<std::mutex> lk(flush_mu_);
      flush_done_epoch_ = target;
      flush_done_cv_.notify_all();
    }
  }
}

void SegmentLogStorage::ResetFromLogs(const std::vector<LogEntry>& logs) {
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

void SegmentLogStorage::PutU32(uint8_t* p, uint32_t v) {
  for (int i = 0; i < 4; i++)
    p[i] = static_cast<uint8_t>((v >> (i * 8)) & 0xFF);
}
uint32_t SegmentLogStorage::GetU32(const uint8_t* p) {
  uint32_t v = 0;
  for (int i = 0; i < 4; i++) v |= static_cast<uint32_t>(p[i]) << (i * 8);
  return v;
}
std::string SegmentLogStorage::EncodeEntry(const LogEntry& e) {
  labrpc::Encoder en;
  en.Zigzag(e.term)
      .Zigzag(e.index)
      .Bytes(e.command)
      .Int(static_cast<int>(e.type))
      .Int(e.conf_server)
      .Int(e.conf_role);
  return en.Take();
}
LogEntry SegmentLogStorage::DecodeEntry(const std::string& b) {
  labrpc::Decoder d(b);
  LogEntry e;
  int t = 0;
  // labrpc::Decoder::Int 返回 bool（out-param 风格），不能像 Encoder 那样链式调用，
  // 逐条调用即可（返回值忽略，靠引用写出）。
  d.Zigzag(e.term);
  d.Zigzag(e.index);
  d.Bytes(e.command);
  d.Int(t);
  d.Int(e.conf_server);
  d.Int(e.conf_role);
  e.type = static_cast<EntryType>(t);
  return e;
}

void SegmentLogStorage::WriteRecord(const LogEntry& e) {
  std::string rec = EncodeEntry(e);
  const uint32_t len = static_cast<uint32_t>(rec.size());
  const uint32_t crc = Crc32Raw(0, rec.data(), rec.size());
  // 【单次 pwrite】把 [len(4)][crc(4)] 头与 body 拼进同一块连续缓冲，一次写完。
  // 原来分两次 pwrite（头 8B + body）——body 很短时两条记录仍要付两次系统调用，
  // 而饱和态下 log_append（编码 + 落 page cache）是头号热点（占每条 8.2µs 的 62%）。
  // 合并后每条少一次 pwrite；代价是多一次 memcpy(rec)，但 rec 只被拷一遍，
  // 比一次 enter/exit 内核便宜。磁盘格式【完全不变】：仍是 [len][crc][body] 连续字节。
  std::string buf;
  buf.reserve(8 + rec.size());  // 单次分配，避免 append 过程中多次增长
  buf.resize(8, '\0');          // 先占 8 字节头位（下面回填）
  buf.append(rec);              // body 紧接在头之后
  PutU32(reinterpret_cast<uint8_t*>(&buf[0]), len);
  PutU32(reinterpret_cast<uint8_t*>(&buf[4]), crc);
  // 关键：用 pwrite 显式写 file_size_ 位置，与 fd 当前偏移量解耦。
  // Rebuild() 走 pread 不会改动 fd 偏移，重启后 fd 偏移仍是 0；
  // 若这里用 ::write（顺序写，依赖 fd 偏移），第一个 Append 会从文件头
  // 覆盖掉第一条已存记录 → seg 首条错位（实测即 TestPersist12C/22C 的
  // "index 1 缺失、整体偏移一位" 发散根因）。pwrite 写 file_size_ 则恒正确。
  // pwrite 先写到 page cache, sync 时才落到磁盘。
  off_t pos = static_cast<off_t>(file_size_);
  if (::pwrite(fd_, buf.data(), buf.size(), pos) !=
      static_cast<ssize_t>(buf.size())) {
    std::fprintf(stderr, "SegmentLogStorage pwrite record 失败\n");
    std::abort();
  }
  file_size_ += buf.size(); // 记录下次在哪里写入
  // 记下这条记录的结束偏移，供 TruncateSuffix 做 O(1) 截短。
  // 不变式：进入本函数时 offsets_.size() 恰好等于「这条记录的下标」
  //   —— Append：WriteRecord 在 entries_.push_back 之前调用，两者同长；
  //   —— RewriteFile：循环前已 clear，逐条 push 自然重建。
  offsets_.push_back(static_cast<off_t>(file_size_));
  dirty_ = true;            // 有改动还没落盘（析构前兜底 fsync）
}

// 从内存 entries_ 整体重写段文件（截断场景用，O(条目数)，远少于每次 append 重写全日志）。
void SegmentLogStorage::RewriteFile() {
  // 0 就是截断直接把文件变为空文件
  if (::ftruncate(fd_, 0) != 0) {
    std::fprintf(stderr, "SegmentLogStorage ftruncate 失败\n");
    std::abort();
  }
  // 相对文件开头把光标拨到 0 也就是文件开头
  if (::lseek(fd_, 0, SEEK_SET) != 0) {
    std::fprintf(stderr, "SegmentLogStorage lseek 失败\n");
    std::abort();
  }
  file_size_ = 0;
  offsets_.clear();  // 文件已从 0 重写，偏移表整体重建（下面 WriteRecord 逐条 push）
  for (const auto& e : entries_) WriteRecord(e);
  // RewriteFile 后由调用方 Sync(true) 负责持久化
}

// 打开时回放：逐条 pread，CRC 不符即停（尾部半截安全丢弃）；中段不连续 = 真损坏 → abort。
void SegmentLogStorage::Rebuild() {
  entries_.clear();
  offsets_.clear();  // 偏移表随回放重建
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
    if (len == 0 || len > (1u << 28)) {
    // 头 8 字节已完整读到（上面的 r != 8 已排除头不完整），走到这里只剩两种可能：
    //   (a) 尾部半截  → off 之后不足一个头，交给下面 ftruncate 丢掉
    //   (b) 中段损坏  → 后面其实还有完整记录，绝不能截
      if (off + 8 < sz) {
        std::fprintf(stderr,
                     "SegmentLogStorage: 中段记录 len 字段非法 len=%u（非尾部），"
                     "疑似磁盘损坏\n",
                     static_cast<unsigned>(len));
        std::abort();
      }
      break;
    }
    std::string buf(len, '\0');
    r = ::pread(fd_, &buf[0], len, off + 8);
    if (r != static_cast<ssize_t>(len)) break;
    if (Crc32Raw(0, buf.data(), len) != crc) {
      // CRC 不符：若这条记录之后【还有数据】（off+8+len < sz）= 中段真损坏，
      // 已提交条目都可能丢失 → fail-stop（abort）；若这条已到文件末尾（尾部半截）
      // → 安全丢弃。普通文件 pread 仅在 EOF 才短读，故短读支恒为尾部，无需区分。
      if (off + 8 + static_cast<off_t>(len) < sz) {
        std::fprintf(stderr,
                     "SegmentLogStorage: 中段记录 CRC 损坏（非尾部），疑似磁盘损坏\n");
        std::abort();
      }
      break;  // 尾部半截
    }
    entries_.push_back(DecodeEntry(buf));
    off += 8 + static_cast<off_t>(len);
    offsets_.push_back(off);  // 这条记录的结束偏移（与 entries_ 同长）
  }
  // 中段索引不连续 = 真损坏（非尾部半截）→ fail-stop
  for (size_t i = 1; i < entries_.size(); ++i) {
    if (entries_[i].index != entries_[i - 1].index + 1) {
      std::fprintf(stderr,
                   "SegmentLogStorage: 中段索引不连续 index=%lld prev=%lld（疑似磁盘损坏）\n",
                   static_cast<long long>(entries_[i].index),
                   static_cast<long long>(entries_[i - 1].index));
      std::abort();
    }
  }
  // 清掉可能存在的尾部半截
  if (off < sz) ::ftruncate(fd_, off);    // 把文件砍回 off，丢掉未提交的半截
  file_size_ = static_cast<size_t>(off);
}

off_t SegmentLogStorage::FileSizeAtOpen() const {
  struct stat st;
  if (::fstat(fd_, &st) != 0) return 0;
  return st.st_size;
}

int64_t SegmentLogStorage::NowNs() {
  return std::chrono::steady_clock::now().time_since_epoch().count();
}

// ---------------------------------------------------------------------------
// 工厂（注：MemoryLogStorage / kMemory 后端已拆至 memory_log_store.h，纯头文件全内联）
// ---------------------------------------------------------------------------

std::unique_ptr<LogStorage> CreateLogStorage(LogStorageKind kind, const std::string& dir) {
  switch (kind) {
    case LogStorageKind::kFile:
      return SegmentLogStorage::Open(dir);
    case LogStorageKind::kMemory:
      return std::make_unique<MemoryLogStorage>();
    case LogStorageKind::kRemote:
      // 第三种后端（远程对象存储 / LSM 等）尚未实现。
      // fail-stop 而非返回空指针半成品：调用方据此再判空反而更易漏判。
      std::fprintf(stderr, "CreateLogStorage: kRemote backend not implemented\n");
      std::abort();
  }
  std::abort();  // 枚举未全覆盖时兜底
}

}  // namespace raft
