// persister.h —— RaftState 持久化器（Go 版 src/labgob/persister.go 的移植）
//
// 归属 persist/state/（raft state blob 与快照 blob 的持久化域）。
// 日志持久化在 persist/log/（LogStorage 接口 + 段文件/内存两后端），互不相干。
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

#include <atomic>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cerrno>
#include <memory>
#include <mutex>
#include <string>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>

namespace persist {
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

// rm -rf 的安全版：递归删除目录树。与 MkdirAll 对称（一建一清）。
inline void RemoveTree(const std::string& path) {
  DIR* d = ::opendir(path.c_str());
  if (!d) return;  // 目录不存在就当没东西可删
  struct dirent* e;
  while ((e = ::readdir(d)) != nullptr) {
    std::string name = e->d_name;
    if (name == "." || name == "..") continue;
    std::string full = path + "/" + name;
    struct stat st;
    if (::stat(full.c_str(), &st) != 0) {
      // stat 抓瞎（极罕见）：先 unlink 再 rmdir，只有一个会成功，覆盖文件/目录两种情况，不漏删
      ::unlink(full.c_str());
      ::rmdir(full.c_str());
      continue;
    }
    if (S_ISDIR(st.st_mode)) {
      RemoveTree(full);
    } else if (::unlink(full.c_str()) != 0 && errno != ENOENT) {
      std::fprintf(stderr, "persist::RemoveTree: unlink %s failed (%s)\n", full.c_str(),
                   std::strerror(errno));
    }
  }
  ::closedir(d);
  if (::rmdir(path.c_str()) != 0 && errno != ENOENT) {
    std::fprintf(stderr, "persist::RemoveTree: rmdir %s failed (%s)\n", path.c_str(),
                 std::strerror(errno));
  }
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
  static std::atomic<uint64_t> seq{0};   // 进程内自增，配合 pid 保证克隆目录名不撞车
  std::string::size_type slash = src.find_last_of('/');
  std::string parent = (slash == std::string::npos) ? "." : src.substr(0, slash);
  std::string dst;
  // 名字里必须带 pid：access() 只查不占，两个【并发】进程可能同时选中 copy_1
  // （各自的 seq 都从 0 开始）→ 两边把各自的克隆写进同一目录 → 互相覆盖，
  // 造出撕裂的 raft_state/seg → "state diverged" 家族（极具误导性）。
  // pid 在任一时刻全局唯一，同进程内再由 seq 保证；pid 复用留下的残留
  // 由下面的 access 循环兜底（换下一个序号）。
  do {
    dst = parent + "/copy_" + std::to_string(::getpid()) + "_" +
          std::to_string(++seq);
  } while (::access(dst.c_str(), F_OK) == 0);  // 已存在就换下一个序号
  // 建目录失败：后面所有写都会跟着失败，必须立刻报出来
  if (!MkdirAll(dst)) {
    std::fprintf(stderr, "Persister: CopyDir MkdirAll failed: %s\n", dst.c_str());
    std::abort();
  }
  // 克隆目录下【所有】常规落盘文件（raft_state.bin / snapshot.bin /
  // raft.log.seg 等），不再写死文件名。
  DIR* d = ::opendir(src.c_str());
  if (d == nullptr) {
    std::fprintf(stderr, "Persister: CopyDir opendir failed: %s\n", src.c_str());
    std::abort();
  }
  for (struct dirent* ent = ::readdir(d); ent != nullptr; ent = ::readdir(d)) {
    std::string name = ent->d_name;
    if (name == "." || name == "..") continue;
    if (name.size() >= 4 && name.compare(name.size() - 4, 4, ".tmp") == 0) continue;
    std::string src_file = src + "/" + name;
    struct stat st;
    if (::stat(src_file.c_str(), &st) != 0) continue;
    if (!S_ISREG(st.st_mode)) continue;  // 只拷常规文件，跳过子目录
    std::string content;
    if (!ReadFile(src_file, &content)) {
      std::fprintf(stderr, "Persister: CopyDir read failed: %s\n", src_file.c_str());
      std::abort();
    }
    if (!WriteFileSync(dst + "/" + name, content)) {
      std::fprintf(stderr, "Persister: CopyDir write failed: %s/%s\n",
                   dst.c_str(), name.c_str());
      std::abort();
    }
  }
  ::closedir(d);
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

// 文件模式工厂：dir 为每节点独立目录。
// 注意：无参 MakePersister() 的"按环境变量选后端"版本已迁到 persister_factory.h
// （对标 transport_factory.h 的 MakeTransport），这里只保留显式指定目录的文件模式。
inline std::shared_ptr<Persister> MakePersister(const std::string& dir) {
  return std::make_shared<Persister>(dir);
}

}  // namespace persist
