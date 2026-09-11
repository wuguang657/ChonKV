// util.h —— 公共小工具
//
// 对应 Go 版里随手就用的 rand.Intn() / time.Sleep() / randstring()。
// C++ 没有内置这些，所以集中放在这里，全项目共用。
//
// 【C++ 知识点】
//   * `inline` 函数放在头文件里：头文件会被多个 .cpp include，
//     不加 inline 会在链接时报 "duplicate symbol"。
//   * 函数内的 `static` 局部变量：C++11 保证它只初始化一次，且线程安全
//     （"magic statics"），所以可以安全地做懒加载的随机数引擎。
//   * `thread_local`：每个线程一份，避免多线程抢同一把锁。

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>

namespace raftcpp {

// ---------------------------------------------------------------------------
// 随机数
// ---------------------------------------------------------------------------

// 随机种子 —— 整场测试的"随机序列总开关"。
//
// 命令值、分区切断谁、选举超时多少毫秒、网络丢不丢包/延迟多久，
// 全都是从这一个种子派生出来的。
//   想换一条序列： `SEED=12345 ./raft_test 2C`
//   不设就固定用 1。
//
// 【为什么默认固定，而不用 random_device】
//   原来用 std::random_device 播种，每跑一次序列都不同，
//   时序类 / race 类的偶发失败根本没法复现，只能靠 -count N 碰运气。
//   固定下来之后，至少"剧本"部分能原样重放。
//
// 【坑1：SEED=0 无效】strtoull 解析失败也返回 0，所以这里把 0 当成
//   "没设"处理，静默回落到 1 —— 别设 SEED=0，它不会生效。
// 【坑2：只在首次调用时读一次环境变量】seed 是函数内 static 局部变量，
//   进程起来之后再 setenv("SEED", ...) 已经来不及了。
//
// 【与 Go 版的差异（旧注释这里写错了，已更正）】
//   Go 版 6.824 **不是**固定种子：src/raft/config.go:69 在 ncpu_once 里
//   明确调了 rand.Seed(makeSeed())，而 makeSeed() 走 crypto/rand（真随机）。
//   所以 Go 每次运行序列都不同，官方本来就不可复现。
//   这里固定成 1 是本项目的刻意选择：牺牲一点随机覆盖率，换失败能重放。
inline uint64_t RandSeed() {
  static const uint64_t seed = [] {
    const char* s = std::getenv("SEED");
    if (s != nullptr && *s != '\0') {
      const unsigned long long v = std::strtoull(s, nullptr, 10);
      if (v != 0) return static_cast<uint64_t>(v);
    }
    return static_cast<uint64_t>(1);  // 与 Go 的默认 seed 1 对齐
  }();
  return seed;
}

// 把 (基础种子, 流编号) 打散成一个种子 —— splitmix64 的标准一轮。
// 相邻编号会得到天差地别的种子，保证各条流之间互不相关。
inline uint64_t MixSeed(uint64_t base, uint64_t stream) {
  uint64_t z = base + 0x9E3779B97F4A7C15ULL * stream;
  z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
  z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
  return z ^ (z >> 31);
}

// 给每个线程发一个【稳定的】流编号：0, 1, 2, ...
// 每个线程首次取随机数时领一个号，之后一直用它（thread_local 缓存）。
//
// 【为什么不用 std::hash<std::thread::id>】（这就是本函数存在的原因）
//   实测（/tmp/tid_demo.cpp，同一程序连跑 3 次）：
//     main id = 0x1e472d300 / 0x1e472d300 / 0x1e472d300   ← 不变
//     t1   id = 0x16b977000 / 0x16db57000 / 0x16fddf000   ← 每次都变
//   子线程的 thread::id 是【线程栈地址】，受系统 ASLR 随机化影响，
//   每次运行都不同 → 派生出的种子不同 → 子线程序列完全不可复现。
//   换成原子序号后，编号集合固定为 {0,1,2,...}，流的内容也就固定了。
inline uint64_t NextThreadStreamIndex() {
  static std::atomic<uint64_t> next{0};
  static thread_local uint64_t idx =
      next.fetch_add(1, std::memory_order_relaxed);
  return idx;
}

// 每个线程一个随机引擎，避免加锁。
//
// ⚠️ 关键：每线程必须用【不同的】种子派生独立子流。
//    原先所有线程都用同一个 RandSeed()（默认 1）播种，导致 5 个选举定时器
//    抽到完全同步的随机序列 → 每轮同时超时 → 永久 split vote → term 暴增、
//    选不出稳定 leader → 活性死锁（TestSnapshotUnreliableRecoverConcurrentPartition3B
//    偶发 "no client made progress"）。
//
// 【可复现到什么程度（诚实说明，别高估）】
//   * 主线程：严格可复现 —— 它总是第一个领号，编号固定为 0。
//   * 子线程：只要"各线程首次取随机数的先后顺序"一致就完全可复现。
//     这个顺序仍受线程调度影响，所以是"大概率"，不是 100%。
//   * 网络丢包 / 延迟这类【按 RPC 取数】的随机源，本质受 RPC 到达顺序影响，
//     Go 版同样如此（Go 是单条全局流，goroutine 取数顺序由调度决定）——
//     这是分布式测试的固有属性，改不掉。
//   * 想要【严格】可复现，用下面的 RandEngineFor(id) / RandIntFor(id, n)：
//     把流绑到逻辑实体（如节点编号），而不是绑到 OS 线程。
inline std::mt19937_64& RandEngine() {
  static thread_local std::mt19937_64 engine(
      MixSeed(RandSeed(), NextThreadStreamIndex()));
  return engine;
}

// 返回 [0, n) 之间的随机整数，n 必须 > 0。
//
// 【注意】不能写成 `engine() % n`！随机数引擎返回的是原始 64 位整数，
// 直接取模会让低位的规律性暴露出来，分布不均匀。
// 用 std::uniform_int_distribution 才是正确做法。
inline int RandInt(int n) {
  if (n <= 0) return 0;
  std::uniform_int_distribution<int> dist(0, n - 1);
  return dist(RandEngine());
}

// 返回 [lo, hi) 之间的随机整数。
inline int RandInt(int lo, int hi) {
  if (hi <= lo) return lo;
  return lo + RandInt(hi - lo);
}

// 按【逻辑实体编号】取一条固定流，而不是按 OS 线程。
//
// 同一个 id，无论落在哪个线程、哪次运行，序列都完全相同 ——
// 这是唯一能做到"严格可复现"的用法。
// 例：3 号节点的选举超时改用 RandIntFor(3, min, max) 之后，
//     "3 号节点第 1/2/3 次超时各是多少毫秒"每次运行都一样，
//     不再受"哪个线程先跑起来"影响。
//
// 【线程安全】内部加了锁，比 RandEngine() 慢。
//   选举超时这种几百毫秒才取一次的场景完全无所谓，
//   但别拿它去跑每秒几万次的高频循环。
inline std::mt19937_64& RandEngineFor(uint64_t stream_id) {
  static std::mutex mu;
  static std::unordered_map<uint64_t, std::mt19937_64> engines;
  std::lock_guard<std::mutex> lk(mu);
  auto it = engines.find(stream_id);
  if (it == engines.end()) {
    it = engines
             .emplace(stream_id,
                      std::mt19937_64(MixSeed(RandSeed(), stream_id + 1)))
             .first;
  }
  return it->second;
}

inline int RandIntFor(uint64_t stream_id, int n) {
  if (n <= 0) return 0;
  std::uniform_int_distribution<int> dist(0, n - 1);
  return dist(RandEngineFor(stream_id));
}

inline int RandIntFor(uint64_t stream_id, int lo, int hi) {
  if (hi <= lo) return lo;
  return lo + RandIntFor(stream_id, hi - lo);
}

// 生成长度为 n 的随机字符串（对应 Go 版 config.go 的 randstring）。
inline std::string RandString(int n) {
  static const char kAlphabet[] =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  std::string s;
  s.resize(static_cast<size_t>(n));
  for (int i = 0; i < n; i++) {
    s[static_cast<size_t>(i)] = kAlphabet[RandInt(0, 62)];
  }
  return s;
}

// ---------------------------------------------------------------------------
// 时间
// ---------------------------------------------------------------------------

using Clock = std::chrono::steady_clock;      // 单调时钟，不受系统时间调整影响，steady_clock 的"单调递增"是硬件保证的
using TimePoint = Clock::time_point;

inline TimePoint Now() { return Clock::now(); }

inline int64_t MillisSince(const TimePoint& t0) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(Now() - t0)
      .count();
}

inline double SecondsSince(const TimePoint& t0) {
  return std::chrono::duration<double>(Now() - t0).count();
}

inline void SleepMs(int64_t ms) {
  if (ms <= 0) return;
  std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

// 当前二进制是用哪个 sanitizer 编的。
//
// 对应 Go 的 `go test -race`：Go 是运行时一键开关，C++ 得在 cmake 配
// -DENABLE_TSAN=ON 重新编一个独立目录（见项目根的 tsan.sh）。
// 默认编出来的二进制抓不到 data race —— 所以启动时把这一行打出来，
// 免得"没开 TSan"这件事被忽略。
inline const char* SanitizerName() {
// ⚠️ 两套检测宏必须都覆盖，只写一边都会在另一个编译器上【静默失效】：
//    * __SANITIZE_THREAD__ / __SANITIZE_ADDRESS__ —— GCC 定义
//    * __has_feature(thread_sanitizer)            —— Clang / AppleClang 定义
//
//   macOS 的 AppleClang 不定义 __SANITIZE_THREAD__，所以修复前即使真的用
//   -fsanitize=thread 编出来，横幅也永远打印 "none" —— 而横幅是判断"这轮
//   到底有没有抓 data race"的唯一自检手段，它恒为 none 意味着将来 cmake
//   真失效时（换目录 / 缓存污染）也发现不了。
//
//   ⚠️ __has_feature 不能直接写进 `&&` 表达式：GCC 不认识它，未定义标识符
//      在 #if 里会被替换成 0，展开成 `0(thread_sanitizer)` 直接编译报错。
//      只能用嵌套 #if 的形式。
#if defined(__SANITIZE_THREAD__)
  return "TSan(thread)";
#elif defined(__SANITIZE_ADDRESS__)
  return "ASan(address)";
#elif defined(__has_feature)
#if __has_feature(thread_sanitizer)
  return "TSan(thread)";
#elif __has_feature(address_sanitizer)
  return "ASan(address)";
#else
  return "none（抓不到 data race；要查竞态请 ./tsan.sh）";
#endif
#else
  return "none（抓不到 data race；要查竞态请 ./tsan.sh）";
#endif
}

}  // namespace raftcpp
