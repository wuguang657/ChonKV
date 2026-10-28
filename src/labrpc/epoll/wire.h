// wire.h —— 帧格式 + 【增量】帧解析器（非阻塞 I/O 的核心）
//
// 帧格式与 socket/socket_transport.cpp 里那一份【逐字节相同】，这样两个后端在
// 线上是可互换的（将来做"一半连接走 socket 后端、一半走 epoll 后端"的对照也不会
// 解析失败）。真正的差别在解析方式：
//
//   SocketNetwork（阻塞）：RecvAll(fd, buf, n) —— "读满 n 字节才返回"。
//     这个前提只在"一条连接一个线程、线程可以睡在 read 上"时成立。
//
//   EpollNetwork（非阻塞）：read 每次最多给你当前可读的那么多字节，一帧可能被
//     切成 3 段、也可能一次给你 5 帧。所以必须自己攒缓冲、自己切帧：
//        FrameReader::Append()  把新到的字节追加进去
//        FrameReader::Next()    能切出一整帧就返回它，切不出来就返回 false
//     这就是"半帧状态机"，也是 epoll 后端与阻塞后端最大的结构性差别。
//
// ⚠️ 这个文件是纯内存逻辑，不碰任何系统调用 —— 所以它能在 macOS 上编译，
//    也值得你单独写用例压它（见 epoll_selftest.cpp 的 wire 用例组）。

#pragma once

#include <atomic>
#include <cstdint>
#include <cstring>
#include <string>

namespace labrpc {
namespace wire {

// 帧头：4 字节大端长度（只算后面 payload 的长度，不含自己这 4 字节）。
static constexpr size_t kLenHeader = 4;
// 请求体最短：corr(8) + svc_len(4) + args_len(4)
static constexpr size_t kMinRequestBody = 16;
// 回包体最短：corr(8) + ok(1) + reply_len(4)
static constexpr size_t kMinReplyBody = 13;

inline void PutU8(std::string& b, uint8_t v) { b.push_back(static_cast<char>(v)); }

inline void PutU32(std::string& b, uint32_t v) {
  char tmp[4];
  tmp[0] = static_cast<char>((v >> 24) & 0xFF);
  tmp[1] = static_cast<char>((v >> 16) & 0xFF);
  tmp[2] = static_cast<char>((v >> 8) & 0xFF);
  tmp[3] = static_cast<char>(v & 0xFF);
  b.append(tmp, 4);
}

inline void PutU64(std::string& b, uint64_t v) {
  char tmp[8];
  for (int i = 7; i >= 0; i--) {
    tmp[i] = static_cast<char>(v & 0xFF);
    v >>= 8;
  }
  b.append(tmp, 8);
}

// 游标式解码：p 前进，越界即失败。返回 false 表示"这帧是坏的"。
inline bool GetU8(const char*& p, const char* end, uint8_t& v) {
  if (p + 1 > end) return false;
  v = static_cast<uint8_t>(*p++);
  return true;
}

inline bool GetU32(const char*& p, const char* end, uint32_t& v) {
  if (p + 4 > end) return false;
  v = (static_cast<uint32_t>(static_cast<uint8_t>(p[0])) << 24) |
      (static_cast<uint32_t>(static_cast<uint8_t>(p[1])) << 16) |
      (static_cast<uint32_t>(static_cast<uint8_t>(p[2])) << 8) |
      static_cast<uint32_t>(static_cast<uint8_t>(p[3]));
  p += 4;
  return true;
}

inline bool GetU64(const char*& p, const char* end, uint64_t& v) {
  if (p + 8 > end) return false;
  v = 0;
  for (int i = 0; i < 8; i++) v = (v << 8) | static_cast<uint8_t>(p[i]);
  p += 8;
  return true;
}

// ---------------------------------------------------------------------------
// 编码
// ---------------------------------------------------------------------------
// 请求帧 = [u32 体长][u64 corr][u32 svc_len][svc][u32 args_len][args]
inline std::string EncodeRequestFrame(uint64_t corr, const std::string& svc,
                                     const std::string& args) {
  std::string payload;
  payload.reserve(16 + svc.size() + args.size());
  PutU64(payload, corr);
  PutU32(payload, static_cast<uint32_t>(svc.size()));
  payload += svc;
  PutU32(payload, static_cast<uint32_t>(args.size()));
  payload += args;

  std::string frame;
  frame.reserve(kLenHeader + payload.size());
  PutU32(frame, static_cast<uint32_t>(payload.size()));
  frame += payload;
  return frame;
}

// 回包帧 = [u32 体长][u64 corr][u8 ok][u32 reply_len][reply]
inline std::string EncodeReplyFrame(uint64_t corr, bool ok,
                                    const std::string& reply) {
  std::string payload;
  payload.reserve(13 + reply.size());
  PutU64(payload, corr);
  PutU8(payload, ok ? 1 : 0);
  PutU32(payload, static_cast<uint32_t>(reply.size()));
  payload += reply;

  std::string frame;
  frame.reserve(kLenHeader + payload.size());
  PutU32(frame, static_cast<uint32_t>(payload.size()));
  frame += payload;
  return frame;
}

// ---------------------------------------------------------------------------
// 解码（输入是【已经去掉 4 字节长度头】的 body）
// ---------------------------------------------------------------------------
inline bool DecodeRequest(const std::string& body, uint64_t& corr,
                          std::string& svc, std::string& args) {
  // 先挡明显不够长的：否则下面那些 GetU32 会各自判一次越界，错误码没法区分
  // "帧太短"和"帧内部长度字段说谎"。
  if (body.size() < kMinRequestBody) return false;
  const char* p = body.data();
  const char* end = body.data() + body.size();
  uint32_t svc_len = 0, args_len = 0;
  if (!GetU64(p, end, corr)) return false;
  if (!GetU32(p, end, svc_len)) return false;
  if (static_cast<size_t>(end - p) < svc_len) return false;
  svc.assign(p, svc_len);
  p += svc_len;
  if (!GetU32(p, end, args_len)) return false;
  if (static_cast<size_t>(end - p) < args_len) return false;
  args.assign(p, args_len);
  return true;
}

inline bool DecodeReply(const std::string& body, uint64_t& corr, bool& ok,
                        std::string& reply) {
  if (body.size() < kMinReplyBody) return false;
  const char* p = body.data();
  const char* end = body.data() + body.size();
  uint8_t ok_byte = 0;
  uint32_t reply_len = 0;
  if (!GetU64(p, end, corr)) return false;
  if (!GetU8(p, end, ok_byte)) return false;
  if (!GetU32(p, end, reply_len)) return false;
  if (static_cast<size_t>(end - p) < reply_len) return false;
  reply.assign(p, reply_len);
  ok = (ok_byte != 0);
  return true;
}

// ---------------------------------------------------------------------------
// FrameReader：把"字节流"切成"一帧一帧"
// ---------------------------------------------------------------------------
// 用法（事件循环里就这两句）：
//     reader.Append(buf, n);                       // 刚 read 到的字节
//     std::string body;
//     while (reader.Next(body)) { ...解析 body... }
//
// 三个必须做对的点（也是压测最容易抓到 bug 的地方）：
//   1) 【半帧】：一次 Append 可能只到一半，必须留着等下次。别用"读不满就报错"。
//   2) 【长度闸门】：4 字节长度头是【远端】给的，可能是垃圾（甚至 0xFFFFFFFF）。
//      不在切帧阶段就挡掉，切出来那一刻就会 allocate 4GiB。max_body 就是这个闸门。
//   3) 【不搬内存】：每切一帧都 erase 前半段的话，一个 10KB 的批量帧流会退化成
//      O(n²)。这里用 off_ 游标，只在 off_ 超过一半时才压实一次。
class FrameReader {
 public:
  // max_body：单帧 body 上限。0 表示不设限（对应 SetMaxMessageBytes(0) 的语义）。
  explicit FrameReader(size_t max_body) : max_body_(max_body) {}

  void Append(const char* data, size_t n) {
    if (n > 0) buf_.append(data, n);
  }

  // 能切出一整帧就填进 body 并返回 true；否则返回 false（且不动 buf_ 的语义内容）。
  // 帧非法（长度头超过闸门）时置 broken_，调用方应据此直接关连接。
  bool Next(std::string& body) {
    const size_t avail = buf_.size() - off_;
    if (avail < kLenHeader) return false;

    const char* p = buf_.data() + off_;
    const char* end = p + kLenHeader;
    uint32_t len = 0;
    if (!GetU32(p, end, len)) return false;  // 理论上不可达，防御

    // 闸门：长度头来自远端，必须在校验通过后才 allocate。
    const size_t cap = max_body_.load(std::memory_order_relaxed);
    if (cap > 0 && static_cast<size_t>(len) > cap) {
      broken_ = true;
      return false;
    }
    if (avail < kLenHeader + static_cast<size_t>(len)) return false;  // 半帧，等下次

    body.assign(buf_.data() + off_ + kLenHeader, len);
    off_ += kLenHeader + len;
    CompactIfNeeded();
    return true;
  }

  bool broken() const { return broken_; }
  size_t buffered() const { return buf_.size() - off_; }

  // 换用新的上限（SetMaxMessageBytes 是运行期可调的，而它可能在线程 A 上被调、
  // 在事件循环线程里被读 —— 所以这里必须是原子，否则就是一次真实的数据竞争）。
  void SetMaxBody(size_t n) { max_body_.store(n, std::memory_order_relaxed); }

 private:
  // 压实策略：攒了很多已消费的前缀、且前缀确实占大头时才搬一次内存。
  void CompactIfNeeded() {
    if (off_ < 64 * 1024) return;           // 小缓冲不值得搬
    if (off_ * 2 < buf_.size()) return;     // 已消费的不占大头，搬了也不划算
    buf_.erase(0, off_);
    off_ = 0;
  }

  std::string buf_;
  size_t off_ = 0;
  std::atomic<size_t> max_body_{0};
  bool broken_ = false;
};

}  // namespace wire
}  // namespace labrpc
