// persister.h —— 模拟磁盘（Go 版 src/raft/persister.go 的移植）
//
// 在真实的 Raft 里，currentTerm / votedFor / log 必须写盘，
// 断电重启后还在。测试里用一块内存 + 一把锁来假装是磁盘。
//
// 关键点：Raft 崩了之后 Persister 里的内容必须还在，这样新起的 Raft
// 才能恢复到崩溃前的状态 —— 这就是 2C 要考的东西。
// 改造为两种模式，内存模式和文件模式。

// 内存模式：所有数据都存在内存里，不写盘。
// 文件模式：所有数据都写盘，断电重启后还能恢复。

#pragma once

#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

namespace labgob {
// inline关键字：该函数被多次.h包含编译合法
// mkdir -p：递归创建目录（忽略已存在的 EEXIST）。返回是否成功。
inline bool MkdirAll(const std::string& path) {
  if (path.empty()) return false;
  std::string p = path;
  while (p.size() > 1 && p.back() == '/') p.pop_back();
  for(size_t i=1; i<p.size(); i++){ // 64位 8字节
    if(p[i] == '/'){
      char saved = p[i];
      p[i] = '\0';
      // ::的意思是用全局的mkdir函数
      ::mkdir(p.c_str(), 0755);  // 已存在则 EEXIST，忽略
      p[i] = saved;
    }
  }
  return ::mkdir(p.c_str(), 0755) == 0 || errno == EEXIST;
}

// 原子写落盘
inline bool WriteFileSync(const std::string& path, const std::string& data){
  std::string tmp = path + ".tmp";
  // O_WRONLY：只写 O_CREAT：创建文件 O_TRUNC：文件已存在就清空
  // 0644：读写权限
  int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) return false;
  size_t off = 0;
  while (off < data.size()) {
    ssize_t w = ::write(fd, data.data() + off, data.size() - off);
    if (w < 0) {
      // errno是线程级全局变量
      if (errno == EINTR) continue;
      ::close(fd); // 关闭文件描述符
      // 删除临时文件
      ::unlink(tmp.c_str());
      return false;
    }
    off += static_cast<size_t>(w);
  }
  // 把数据从内存刷到磁盘
  if (::fsync(fd) != 0) {
    ::close(fd);
    ::unlink(tmp.c_str());
    return false;
  }
  if (::close(fd) != 0) {
    ::unlink(tmp.c_str());
    return false;
  }
  if (::rename(tmp.c_str(), path.c_str()) != 0) {
    ::unlink(tmp.c_str());
    return false;
  }
  // 刷新目录缓存，让重命名的元数据操作也就是rename也落盘
  std::string dir = path.substr(0, path.find_last_of('/'));
  int dfd = ::open(dir.c_str(), O_RDONLY);
  if (dfd < 0) return false;
  if (::fsync(dfd) != 0) {
    ::close(dfd);
    return false;
  }
  ::close(dfd);
  return true;
}

// 原子读文件
inline bool ReadFile(const std::string& path, std::string* out) {
  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    out->clear();
    return false;
  }
  std::string buf;
  char chunk[65536];
  while (true) {
    ssize_t r = ::read(fd, chunk, sizeof(chunk));
    if (r < 0) {
      if (errno == EINTR) continue;
      ::close(fd);
      out->clear();
      return false;
    }
    if (r == 0) break;
    buf.append(chunk, static_cast<size_t>(r));
  }
  ::close(fd);
  *out = std::move(buf);
  return true;
}

inline bool FileExists(const std::string& path) {
  return ::access(path.c_str(), F_OK) == 0;
}

// std::abort()：宁可当街崩，也绝不「假装拷贝成功」把节点拉起来。
inline std::string CopyDir(const std::string& src) {
  static std::atomic<uint64_t> seq{0};   // 全局自增，保证每次克隆的目录名不撞车
  std::string::size_type slash = src.find_last_of('/');
  std::string parent = (slash == std::string::npos) ? "." : src.substr(0, slash);
  // ⚠️ 扁平命名：副本名只由全局自增 seq 决定，刻意【不】沿用源目录名。
  //   旧写法 dst = parent + "/" + base + "_copy_" + seq（base 取源目录 basename），
  //   会在反复重启时逐层嵌套：node_3 → node_3_copy_54 → node_3_copy_54_copy_64 …
  //   Figure 8 等高频 crash/重启用例几十轮后目录名撑爆 NAME_MAX(255)，
  //   mkdir 失败 → MkdirAll 返 false → std::abort() 把整个测试进程打挂。
  //   改成 copy_<seq> 后，无论重启多少次，单路径分量长度恒定（"copy_" + 19 位十进制），
  //   绝不会超 255。所有副本仍落在同一 parent 下，Cleanup 的 RemoveTree(base) 一并清掉。
  std::string dst;
  do {
    dst = parent + "/copy_" + std::to_string(++seq);
  } while (::access(dst.c_str(), F_OK) == 0);  // 已存在就换下一个序号
  // 建目录失败：后面所有写都会跟着失败，必须立刻报出来
  if (!MkdirAll(dst)) {
    std::fprintf(stderr, "Persister: CopyDir MkdirAll failed: %s\n", dst.c_str());
    std::abort();
  }
  for (const char* name : {"raft_state.bin", "snapshot.bin"}) {
    std::string src_file = src + "/" + name;
    // 文件不存在 = 源目录里本来就没它（比如该节点还没落过快照），合法，跳过
    if (!FileExists(src_file)) continue;
    std::string content;
    // 读不到 = 源目录里本来就没这个文件（比如该节点还没落过快照），合法，跳过
    // 存在却读不出来 = 磁盘 I/O 错误，绝不能跳过，否则副本残缺、新节点「失忆」
    if (!ReadFile(src_file, &content)) {
      std::fprintf(stderr, "Persister: CopyDir read failed: %s\n", src_file.c_str());
      std::abort();
    }
    // 写失败 = 磁盘拒绝了这次写，是真错误，绝不能当无事发生
    if (!WriteFileSync(dst + "/" + name, content)) {
      std::fprintf(stderr, "Persister: CopyDir write failed: %s/%s\n",
                   dst.c_str(), name);
      std::abort();
    }
  }
  return dst;
}

class Persister {
 public:
  Persister() = default;

  // 这里同样必须判成败，但和 CopyDir 不同：ReadFile 返回 false 有两种含义，必须分开：
  //   1) 文件不存在 → 全新节点的空盘，合法，按空状态继续；
  //   2) 文件存在却读不出来（磁盘 I/O 错误）→ 真错误。若当空盘继续，节点就「失忆」
  //      丢掉 term + log，重启安全性当场崩塌 —— 必须 abort。
  explicit Persister(const std::string& dir) : file_backed_(true), dir_(dir) {
    if (!MkdirAll(dir_)) {
      std::fprintf(stderr, "Persister: MkdirAll failed: %s\n", dir_.c_str());
      std::abort();
    }
    LoadOrAbort(StatePath(), &raft_state_);
    LoadOrAbort(SnapPath(), &snapshot_);
  }

  // 复制一份（快照语义）：copy 出的新 Persister 与原来的互不影响。
  // config 在重启一个 server 时会先 Copy()，防止旧实例继续往里写。
  std::shared_ptr<Persister> Copy() {
    if (!file_backed_) {
      auto np = std::make_shared<Persister>();
      std::lock_guard<std::mutex> lk(mu_);
      np->raft_state_ = raft_state_;
      np->snapshot_ = snapshot_;
      return np;
    }
    // 文件模式：物理克隆整个目录，新 persister 指向副本（构造时自动加载）。
    // CopyDir 内部会判断成败，失败直接 abort，不会让新实例从残缺副本启动。
    return std::make_shared<Persister>(CopyDir(dir_));
  }

  void SaveRaftState(const std::string& state) {
    std::lock_guard<std::mutex> lk(mu_);
    raft_state_ = state;
    if (file_backed_ && !WriteFileSync(StatePath(), state)) {
      // 落盘失败：本函数签名无法上报 bool，但绝不能「假装写成了」——
      // 否则 Raft 会基于幽灵写入推进 commitIndex，违反「持久化成功前不可提交」铁律。
      // 故宁肯当街崩（fail-stop），也绝不带病继续。内存模式 file_backed_=false 不会进此分支。
      std::fprintf(stderr, "Persister: SaveRaftState write failed (disk error?)\n");
      std::abort();
    }
  }

  std::string ReadRaftState() {
    std::lock_guard<std::mutex> lk(mu_);
    return raft_state_;
  }

  int RaftStateSize() {
    std::lock_guard<std::mutex> lk(mu_);
    return static_cast<int>(raft_state_.size());
  }

  // 原子地同时保存 raft 状态和快照，避免两者对不上（Lab 3 用得上）
  void SaveStateAndSnapshot(const std::string& state, const std::string& snapshot) {
    std::lock_guard<std::mutex> lk(mu_);
    raft_state_ = state;
    snapshot_ = snapshot;
    if (file_backed_) {
      // 落盘顺序：先 blob 后 state（与 Raft::Snapshot 两阶段一致）。
      // 保证任何崩溃窗口下盘上只可能出现「blob ≥ state」（安全态，ReadPersist 走追认分支），
      // 绝不出现「state > blob」（致命态：ReadPersist 两分支都不进 → 快照数据丢失）。
      if (!WriteFileSync(SnapPath(), snapshot) ||
          !WriteFileSync(StatePath(), state)) {
        // 落盘失败：state 与 snapshot 任一写不下去，立即 fail-stop。
        // 绝不让 Raft 在「只更新了内存、盘上没跟上」的状态下继续提交。
        std::fprintf(stderr,
                     "Persister: SaveStateAndSnapshot write failed (disk error?)\n");
        std::abort();
      }
    }
  }

  // 【新增】只写快照 blob，不动 raft state。
  //
  // 存在的意义：把「大 blob 的落盘 I/O」从 raft 主锁里挪出去。
  //   - 旧路径只有 SaveStateAndSnapshot（state + blob 一起原子写），
  //     意味着 blob 的 write()+fsync() 必然发生在 raft 持锁期间。
  //     真盘上一个几百 MB 的快照 fsync 要几百 ms～几秒，会把心跳、
  //     读心跳、选举全部饿死（etcd 踩过的生产事故）。
  //   - 有了本接口后，raft 可以「锁外先落 blob → 锁内再提交 state」两阶段走。
  //
  // 真实磁盘实现里，这个函数就是 open + write + fsync + rename，
  // 是整条快照路径上唯一允许慢的地方。
  void SaveSnapshotOnly(const std::string& snapshot) {
    std::lock_guard<std::mutex> lk(mu_);
    snapshot_ = snapshot;
    if (file_backed_ && !WriteFileSync(SnapPath(), snapshot)) {
      // 落盘失败：blob 没写进去却继续推进，会造出「state 领先 / blob 落后」的致命态。
      // 立刻 fail-stop，绝不让快照路径带着残缺 blob 继续跑。
      std::fprintf(stderr, "Persister: SaveSnapshotOnly write failed (disk error?)\n");
      std::abort();
    }
  }

  std::string ReadSnapshot() {
    std::lock_guard<std::mutex> lk(mu_);
    return snapshot_;
  }

  // 仅文件模式有意义：返回本节点落盘目录（调试用）。
  const std::string& Dir() const { return dir_; }
  bool FileBacked() const { return file_backed_; }

  int SnapshotSize() {
    std::lock_guard<std::mutex> lk(mu_);
    return static_cast<int>(snapshot_.size());
  }

 private:
  std::string StatePath() const { return dir_ + "/raft_state.bin"; }
  std::string SnapPath() const { return dir_ + "/snapshot.bin"; }

  // 读一个持久化文件：不存在 → 全新节点空盘，合法置空后返回；
  // 存在却读失败 → 打日志后 abort：绝不让节点「失忆」启动。
  static void LoadOrAbort(const std::string& path, std::string* out) {
    if (!FileExists(path)) {
      out->clear();
      return;
    }
    if (!ReadFile(path, out)) {
      std::fprintf(stderr, "Persister: read failed (disk error?): %s\n", path.c_str());
      std::abort();
    }
  }

  std::mutex mu_;
  std::string raft_state_;
  std::string snapshot_;
  bool file_backed_ = false;
  std::string dir_;
};

inline std::shared_ptr<Persister> MakePersister() {
  return std::make_shared<Persister>();
}

// 文件模式工厂：dir 为每节点独立目录。
inline std::shared_ptr<Persister> MakePersister(const std::string& dir) {
  return std::make_shared<Persister>(dir);
}

}  // namespace labgob
