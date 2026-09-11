// codec.h —— 极简二进制编解码器（替代 Go 版的 labgob）
//
// 为什么需要它？
//   1. Go 版 labrpc 用 labgob 编码 RPC 参数，目的是"确保 RPC 不传递对象引用"，
//      顺便还能数出"这条消息有多少字节"。C++ 里没有反射，我们手写一份。
//   2. TestRPCBytes2B 要统计字节数，检查"每条命令是不是只发给每个 peer 一次"。
//      所以编码必须紧凑：一个 5000 字节的字符串，编码后就应该接近 5000 字节。
//
// 编码格式（够用就行，别追求完美）：
//   * 整数：Zigzag + Varint（小整数占 1~2 字节，负数也不膨胀）
//   * bool：当成 0/1 的整数
//   * 字符串/字节串：Varint 长度 + 原始字节
//   * 数组：Varint 元素个数 + 依次编码每个元素
//
// 【C++ 知识点】
//   * 链式调用 `enc.Zigzag(1).Str("a")` 靠"返回 *this 的引用"实现。
//   * Decoder 不抛异常，出错就置 ok_=false，调用方用 Ok() 检查。

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace labrpc {

class Encoder {
 public:
  Encoder& Bool(bool v) { return Varint(v ? 1u : 0u); }

  // Varint：每字节低 7 位存数据，最高位表示"后面还有"。
  Encoder& Varint(uint64_t v) {
    while (v >= 0x80) {
      buf_.push_back(static_cast<char>((v & 0x7f) | 0x80));
      v >>= 7;
    }
    buf_.push_back(static_cast<char>(v));
    return *this;
  }

  // Zigzag：把有符号数映射成无符号数，-1→1, 1→2, -2→3, 2→4 ...
  // 这样负数也能用很少的字节表示。
  Encoder& Zigzag(int64_t v) {
    return Varint((static_cast<uint64_t>(v) << 1) ^
                  static_cast<uint64_t>(v >> 63));
  }

  Encoder& Bytes(const std::string& s) {
    Varint(static_cast<uint64_t>(s.size()));
    buf_.append(s);
    return *this;
  }

  Encoder& Int(int v) { return Zigzag(v); }

  // 取走结果（移动语义，避免拷贝大字符串）
  std::string Take() { return std::move(buf_); }
  const std::string& Data() const { return buf_; }

 private:
  std::string buf_;
};

class Decoder {
 public:
  explicit Decoder(const std::string& s) : buf_(s) {}

  bool Bool(bool& v) {
    uint64_t x = 0;
    if (!Varint(x)) return Fail();
    v = (x != 0);
    return true;
  }

  bool Varint(uint64_t& v) {
    uint64_t result = 0;
    int shift = 0;
    while (true) {
      if (pos_ >= buf_.size()) return Fail();
      unsigned char b = static_cast<unsigned char>(buf_[pos_++]);
      if (shift >= 64) return Fail();
      result |= (static_cast<uint64_t>(b & 0x7f) << shift);
      if ((b & 0x80) == 0) {
        v = result;
        return true;
      }
      shift += 7;
    }
  }

  bool Zigzag(int64_t& v) {
    uint64_t x = 0;
    if (!Varint(x)) return Fail();
    v = static_cast<int64_t>(x >> 1) ^ -static_cast<int64_t>(x & 1);
    return true;
  }

  bool Bytes(std::string& s) {
    uint64_t n = 0;
    if (!Varint(n)) return Fail();
    if (n > buf_.size() - pos_) return Fail();
    s.assign(buf_, pos_, static_cast<size_t>(n));
    pos_ += static_cast<size_t>(n);
    return true;
  }

  bool Int(int& v) {
    int64_t x = 0;
    if (!Zigzag(x)) return false;
    v = static_cast<int>(x);
    return true;
  }

  bool Ok() const { return ok_ && pos_ == buf_.size(); }

 private:
  bool Fail() {
    ok_ = false;
    return false;
  }
  const std::string& buf_;
  size_t pos_ = 0;
  bool ok_ = true;
};
}  // namespace labrpc
