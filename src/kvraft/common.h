// common.h —— KV 服务的公共数据类型（对应 Go 版 src/kvraft/common.go）
//
// 每个结构都有 Serialize()/Deserialize()，网络层靠它们把对象变成字节流。
// 顺序必须严格一致：编码写进去几个字段、什么顺序，解码就怎么读出来。
// （和 raft.h 里的 RPC 消息同一套范式）

#pragma once

#include <string>

#include "../labrpc/codec.h"

namespace kvraft {

// ---------------------------------------------------------------------------
// 错误码
// ---------------------------------------------------------------------------
enum class Err {
  kOK = 0,
  kNoKey,        // Get 的 key 不存在
  kWrongLeader,  // 找错人了，客户端该换台机器重试
  kTimeout,      // 等 raft 提交超时（本实现自定义，Go 版没有）
};

inline const char* ErrName(Err e) {
  switch (e) {
    case Err::kOK: return "OK";
    case Err::kNoKey: return "ErrNoKey";
    case Err::kWrongLeader: return "ErrWrongLeader";
    case Err::kTimeout: return "ErrTimeout";
  }
  return "???";
}

// ---------------------------------------------------------------------------
// Op：真正塞进 raft 日志里的命令
// ---------------------------------------------------------------------------
// 【为什么必须是 struct 而不是直接塞字符串】
// Raft 的 Command 就是 std::string，KV 层自己在里面编码。用固定字段的 struct
// 而不是 "Put|key|value" 这种拼字符串，是为了：
//   1) 字段里有任意字节（比如 value 含 '|'）也不会解析错
//   2) client_id / seq_id 是 exactly-once 语义的命根子，必须结构化
struct Op {
  std::string key;
  std::string value;
  std::string method;  // "Put" / "Append" / "Get"
  int client_id = 0;
  int seq_id = 0;

  std::string Serialize() const {
    labrpc::Encoder e;
    e.Bytes(key).Bytes(value).Bytes(method).Int(client_id).Int(seq_id);
    return e.Take();
  }

  bool Deserialize(const std::string& s) {
    labrpc::Decoder d(s);
    if (!d.Bytes(key)) return false;
    if (!d.Bytes(value)) return false;
    if (!d.Bytes(method)) return false;
    if (!d.Int(client_id)) return false;
    if (!d.Int(seq_id)) return false;
    return d.Ok();
  }
};

// ---------------------------------------------------------------------------
// RPC 消息：Get
// ---------------------------------------------------------------------------
struct GetArgs {
  std::string key;
  int client_id = 0;
  int seq_id = 0;

  std::string Serialize() const {
    labrpc::Encoder e;
    e.Bytes(key).Int(client_id).Int(seq_id);
    return e.Take();
  }
  bool Deserialize(const std::string& s) {
    labrpc::Decoder d(s);
    if (!d.Bytes(key)) return false;
    if (!d.Int(client_id)) return false;
    if (!d.Int(seq_id)) return false;
    return d.Ok();
  }
};

struct GetReply {
  Err err = Err::kOK;
  std::string value;

  std::string Serialize() const {
    labrpc::Encoder e;
    e.Int(static_cast<int>(err)).Bytes(value);
    return e.Take();
  }
  bool Deserialize(const std::string& s) {
    labrpc::Decoder d(s);
    int e = 0;
    if (!d.Int(e)) return false;
    err = static_cast<Err>(e);
    if (!d.Bytes(value)) return false;
    return d.Ok();
  }
};

// ---------------------------------------------------------------------------
// RPC 消息：Put / Append（共用一个 RPC）
// ---------------------------------------------------------------------------
struct PutAppendArgs {
  std::string key;
  std::string value;
  std::string op;  // "Put" or "Append"
  int client_id = 0;
  int seq_id = 0;

  std::string Serialize() const {
    labrpc::Encoder e;
    e.Bytes(key).Bytes(value).Bytes(op).Int(client_id).Int(seq_id);
    return e.Take();
  }
  bool Deserialize(const std::string& s) {
    labrpc::Decoder d(s);
    if (!d.Bytes(key)) return false;
    if (!d.Bytes(value)) return false;
    if (!d.Bytes(op)) return false;
    if (!d.Int(client_id)) return false;
    if (!d.Int(seq_id)) return false;
    return d.Ok();
  }
};

struct PutAppendReply {
  Err err = Err::kOK;

  std::string Serialize() const {
    labrpc::Encoder e;
    e.Int(static_cast<int>(err));
    return e.Take();
  }
  bool Deserialize(const std::string& s) {
    labrpc::Decoder d(s);
    int e = 0;
    if (!d.Int(e)) return false;
    err = static_cast<Err>(e);
    return d.Ok();
  }
};

}  // namespace kvraft
