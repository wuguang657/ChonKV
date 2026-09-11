// porcupine.cpp —— 线性一致性检查器实现（移植自 MIT 6.824 src/porcupine/）
//
// ⚠️ 一处重要修改：Node 自带 Input/Output 副本（Node<Input, Output> 模板）。
// 原 Go 版用 entry.value 指向 Operation（生命周期由 history 持有），
// C++ 这边如果照搬会出现 use-after-free —— MakeEntries 返回的 Entry 里的
// void* 指向已析构的局部 vector。改成 Node 自带值是最稳的做法。

#include "porcupine.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <future>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "kv_model.h"  // 显式实例化需要完整的 KvInput/KvOutput/KvState

#include <unordered_map>

namespace porcupine {
namespace {

// ---------------------------------------------------------------------------
// Bitset
// ---------------------------------------------------------------------------
class Bitset {
 public:
  explicit Bitset(uint64_t n_bits) : words_((n_bits + 63) / 64, 0) {}

  // 拷贝 / 移动都要 public —— std::vector 扩容时需要 copy/move 元素
  Bitset(const Bitset&) = default;
  Bitset& operator=(const Bitset&) = default;
  Bitset(Bitset&&) = default;
  Bitset& operator=(Bitset&&) = default;

  Bitset Clone() const { return Bitset(*this); }

  bool Get(uint64_t pos) const {
    auto [maj, min] = Index(pos);
    return (words_[maj] >> min) & 1ULL;
  }

  void Set(uint64_t pos) {
    auto [maj, min] = Index(pos);
    words_[maj] |= (1ULL << min);
  }

  void Clear(uint64_t pos) {
    auto [maj, min] = Index(pos);
    words_[maj] &= ~(1ULL << min);
  }

  uint64_t Hash() const {
    // Go 版：hash = popcnt() ^ data[0] ^ data[1] ^ ...
    uint64_t h = 0;
    for (auto w : words_) {
      h ^= w;
    }
    int pc = 0;
    for (auto w : words_) pc += __builtin_popcountll(w);
    h ^= static_cast<uint64_t>(pc);
    return h;
  }

  bool Equals(const Bitset& other) const { return words_ == other.words_; }

 private:
  static std::pair<uint64_t, uint64_t> Index(uint64_t pos) {
    return {pos / 64, pos % 64};
  }
  std::vector<uint64_t> words_;
};

// ---------------------------------------------------------------------------
// Node —— 双向链表节点（自带 input/output 副本）
//
// call 节点：match 指向它的 return 节点；value 存 Input
// return 节点：match == nullptr；value 存 Output
// ---------------------------------------------------------------------------
template <typename Input, typename Output>
struct Node {
  // 同时持有两种值，多用一点内存但避免了 variant 的运行时开销
  Input input{};
  Output output{};
  bool is_input = true;   // true=Call/Input, false=Return/Output
  Node* match = nullptr;  // Call→Return, Return→nullptr
  int id = 0;
  Node* prev = nullptr;
  Node* next = nullptr;
};

template <typename I, typename O>
Node<I, O>* InsertBefore(Node<I, O>* n, Node<I, O>* mark) {
  if (mark != nullptr) {
    auto* before_mark = mark->prev;
    mark->prev = n;
    n->next = mark;
    if (before_mark != nullptr) {
      n->prev = before_mark;
      before_mark->next = n;
    }
  }
  return n;
}

template <typename I, typename O>
int Length(Node<I, O>* n) {
  int l = 0;
  for (; n != nullptr; n = n->next) ++l;
  return l;
}

// ---------------------------------------------------------------------------
// Lift / Unlift
// ---------------------------------------------------------------------------
template <typename I, typename O>
void Lift(Node<I, O>* entry) {
  entry->prev->next = entry->next;
  entry->next->prev = entry->prev;
  auto* match = entry->match;
  if (match->next != nullptr) match->next->prev = match->prev;
  match->prev->next = match->next;
}

template <typename I, typename O>
void Unlift(Node<I, O>* entry) {
  auto* match = entry->match;
  if (match->next != nullptr) match->next->prev = match;
  match->prev->next = match;
  entry->next->prev = entry;
  entry->prev->next = entry;
}

// ---------------------------------------------------------------------------
// MakeLinkedEntries —— 把 entry 数组串成双向链表
// ---------------------------------------------------------------------------
//
// 输入格式：每个 op 有两条 entry（call 在前，return 在后）。
//
// ⚠️ 关键修复（对比 Go 版 src/porcupine/checker.go）：
// 必须把每个 op 拆成 (call, return) 两条**独立** entry 后，**按 time 整体排序**
// 再串链表 —— Go 版 `makeEntries` 就是 `sort.Sort(byTime(entries))`。
// 修复前 C++ 版直接按 history 顺序串链表，导致链表里每个 call 紧跟自己的 ret
// （call→ret 在一起），算法 POP 之后 entry = call.next = 自己 ret → 触发 RETURN
// → 又 POP → 直接弹光栈 → 永远判 Illegal。
// 修复后链表按 (call_time, ret_time) 整体排序，call 后面是别的 op 的 call，
// POP 之后 entry 跳到下一个 call，DFS 能正常推进。
//
// 反向遍历：root 始终是"最后一个 entry"。
// ⚠️ 关键顺序：必须【先插入 return、再插入 call】。原因是 insertBefore(n, root)
// 把 n 放到 root 之前。所以 Go 的原版先 insertBefore(return, root)，再
// insertBefore(call, root) —— 这样 call 最终跑到 return 前面，时间顺序才对。
template <typename Input, typename Output>
Node<Input, Output>* MakeLinkedEntries(
    std::vector<Operation<Input, Output>> history) {
  using NodeT = Node<Input, Output>;

  // 1. 拆 (call, return) 成独立条目，按 time 整体排序
  struct TimeEntry {
    bool is_call;       // true=call, false=return
    int op_index;       // history 中的下标（call.match = &returns[op_index]）
    Input input;        // call 用
    Output output;      // return 用
    int64_t time_ns;
  };
  std::vector<TimeEntry> entries;
  entries.reserve(history.size() * 2);
  for (size_t i = 0; i < history.size(); ++i) {
    entries.push_back({true, static_cast<int>(i), history[i].input, {},
                       history[i].call_time_ns});
    entries.push_back({false, static_cast<int>(i), {}, history[i].output,
                       history[i].return_time_ns});
  }
  std::sort(entries.begin(), entries.end(),
            [](const TimeEntry& a, const TimeEntry& b) {
              return a.time_ns < b.time_ns;
            });

  // 2. 先建所有 return node，按 op_index 索引（call 才能 match）
  std::vector<NodeT*> ret_nodes(history.size(), nullptr);
  for (auto& e : entries) {
    if (!e.is_call) {
      auto* ret = new NodeT{};
      ret->is_input = false;
      ret->match = nullptr;
      ret->id = e.op_index;
      ret->output = e.output;
      ret_nodes[e.op_index] = ret;
    }
  }

  // 3. 反向遍历串链表：InsertBefore(n, root) 把 n 放到 root 之前；
  //    从时间最晚的开始往前 Insert，最终链表就是正向时间序。
  NodeT* root = nullptr;
  for (int i = static_cast<int>(entries.size()) - 1; i >= 0; --i) {
    auto& e = entries[i];
    if (e.is_call) {
      auto* call = new NodeT{};
      call->is_input = true;
      call->match = ret_nodes[e.op_index];
      call->id = e.op_index;
      call->input = e.input;
      root = InsertBefore(call, root);
    } else {
      auto* ret = ret_nodes[e.op_index];
      root = InsertBefore(ret, root);
    }
  }
  return root;
}

// ---------------------------------------------------------------------------
// CacheEntry —— 缓存条目（key = (linearized Hash ^ state Hash)）
//
// 之前用 vector<vector> 按 linearized.Hash() 单级索引，但 Hash() 最大值
// 只到 64（popcnt ^ xor），64 个桶装几万 entries → 单次 cache query 退化为
// O(几万) 链表扫描。改用 unordered_map，按真实 hash 散列。
// 同时给 cache 加容量上限，避免长 partition（n>50）DFS 展开时 cache 无限
// 增长直到 OOM。
// ---------------------------------------------------------------------------
template <typename State>
struct CacheEntry {
  Bitset linearized;
  State state;
};

// state 的 hash —— std::hash<std::string> 已经够用
template <typename State>
inline uint64_t StateHash(const State& s) {
  return std::hash<State>{}(s);
}

// 缓存 key = (linearized Hash ^ state hash)。combine 两个 hash 比只用
// linearized Hash 更分散，同 linearized 不同 state 也能命中不同桶。
template <typename State>
inline uint64_t CacheKey(const Bitset& lin, const State& s) {
  return lin.Hash() ^ StateHash(s);
}

template <typename Input, typename Output, typename State>
struct CallsEntry {
  Node<Input, Output>* entry;
  State state;
};

// ---------------------------------------------------------------------------
// CheckSingle —— 核心 DFS（带回溯 + 缓存剪枝）
// ---------------------------------------------------------------------------
template <typename Input, typename Output, typename State>
bool CheckSingle(const Model<Input, Output, State>& model,
                 Node<Input, Output>* head, std::atomic<int>* kill) {
  using NodeT = Node<Input, Output>;

  int n = Length<Input, Output>(head) / 2;
  if (n == 0) return true;  // 空 history 显然是线性一致的

  Bitset linearized(n);
  // cache: 按 (linearized Hash ^ state hash) 散列到 unordered_map。
  // 容量上限：原来是 n * 1024（每个 op 最多缓存 1024 个 state），
  // 但每条 entry 都存一份 n-bit 的 Bitset clone → 总内存 O(n × 1024 × n/8)
  // = O(n² × 128B)。n=2000 时约 512MB，n=10000 时约 12.8GB → 直接 OOM。
  // 这里再加一道绝对内存预算（默认 256MB），换算成条目上限取两者较小值。
  // 正常的 n（几十，15 个 key 分摊后 n≈60）远达不到上限，行为完全不变；
  // 只有 history 大到快 OOM 时才开始丢缓存（宁可慢，不要崩）。
  // 之前用 vector<vector> 桶大小被 Hash() 范围限制在 64，单 partition 长
  // history 时 64 桶装几万 entries → query O(几万) → OOM。
  const size_t kMaxCacheBytes = 256ull * 1024 * 1024;
  const size_t per_entry_bytes = static_cast<size_t>(n) / 8 + 64;  // 粗估
  const size_t cache_cap =
      std::min<size_t>(static_cast<size_t>(n) * 1024,
                       kMaxCacheBytes / (per_entry_bytes > 0 ? per_entry_bytes : 1));
  std::unordered_map<uint64_t, std::vector<CacheEntry<State>>> cache;
  size_t cache_size = 0;

  std::vector<CallsEntry<Input, Output, State>> calls;

  State state = model.Init();

  // sentinel 节点 —— 让"链表空"和"撞到头"统一成 next == nullptr
  NodeT* head_sentinel = new NodeT{};
  head_sentinel->id = -1;
  if (head != nullptr) head->prev = head_sentinel;
  head_sentinel->next = head;

  NodeT* entry = head_sentinel->next;

  while (entry != nullptr) {
    if (kill->load() != 0) {
      // 别的分片已判 Illegal，提前退出
      // 先把链表清掉
      NodeT* p = head_sentinel->next;
      while (p != nullptr) {
        NodeT* nx = p->next;
        delete p;
        p = nx;
      }
      delete head_sentinel;
      return false;
    }

    if (entry->match != nullptr) {
      // ---- Call 节点 ----
      auto [ok, new_state] = model.Step(state, entry->input, entry->match->output);
      if (ok) {
        Bitset new_lin = linearized.Clone();
        new_lin.Set(entry->id);

        bool seen = false;
        uint64_t k = CacheKey(new_lin, new_state);
        auto it = cache.find(k);
        if (it != cache.end()) {
          for (const auto& ce : it->second) {
            if (ce.linearized.Equals(new_lin) && model.Equal(ce.state, new_state)) {
              seen = true;
              break;
            }
          }
        }
        if (!seen) {
          // 入栈、推进（cache 未满才记录，避免长 partition 时 cache 无限增长）
          if (cache_size < cache_cap) {
            cache[k].push_back({new_lin.Clone(), new_state});
            ++cache_size;
          }
          calls.push_back({entry, state});
          state = new_state;
          linearized.Set(entry->id);
          Lift(entry);
          entry = head_sentinel->next;
        } else {
          // 剪枝：跳过这条 call
          entry = entry->next;
        }
      } else {
        // 这条 op 在当前状态下产生不了这个 output → 跳过
        entry = entry->next;
      }
    } else {
      // ---- Return 节点：撞到栈顶 call 的 return → 回溯 ----
      if (calls.empty()) {
        // 没可回溯的候选了，但链表还有 entry → 这条历史找不到合法线性化
        NodeT* p = head_sentinel->next;
        while (p != nullptr) {
          NodeT* nx = p->next;
          delete p;
          p = nx;
        }
        delete head_sentinel;
        return false;
      }
      CallsEntry<Input, Output, State> top = calls.back();
      entry = top.entry;
      state = top.state;
      linearized.Clear(entry->id);
      calls.pop_back();
      Unlift(entry);
      entry = entry->next;
    }
  }

  // 链表空了 → 找到完整线性化。清理链表
  NodeT* p = head_sentinel->next;
  while (p != nullptr) {
    NodeT* nx = p->next;
    delete p;
    p = nx;
  }
  delete head_sentinel;
  return true;
}

// ---------------------------------------------------------------------------
// CheckParallel —— 每个 partition 一个线程跑 DFS
// ---------------------------------------------------------------------------
template <typename Input, typename Output, typename State>
CheckResult CheckParallel(const Model<Input, Output, State>& model,
                          const std::vector<std::vector<Operation<Input, Output>>>& partitions,
                          std::chrono::milliseconds timeout) {
  std::atomic<int> kill(0);
  std::vector<std::thread> threads;
  std::vector<std::promise<bool>> promises(partitions.size());
  std::vector<std::future<bool>> futs;
  futs.reserve(partitions.size());

  for (size_t i = 0; i < partitions.size(); ++i) {
    futs.emplace_back(promises[i].get_future());
    threads.emplace_back([&model, &partitions, i, &kill, &promises] {
      bool ok = false;
      try {
        auto* head = MakeLinkedEntries(partitions[i]);
        ok = CheckSingle(model, head, &kill);
      } catch (const std::exception& e) {
        // 任何异常都判 Illegal（保守策略：宁可误报不要漏报）
        std::fprintf(stderr,
                     "porcupine: CheckSingle 异常 (partition %zu): %s\n", i,
                     e.what());
        ok = false;
      } catch (...) {
        std::fprintf(stderr, "porcupine: CheckSingle 未知异常 (partition %zu)\n",
                     i);
        ok = false;
      }
      promises[i].set_value(ok);
    });
  }

  bool ok = true;
  bool timed_out = false;

  // deadline 必须在循环外算一次：所有 partition 共享同一份 timeout 预算
  // （对齐 Go 版的 context.WithTimeout）。
  // 之前 deadline 在循环内每次重算 → 每个 partition 都拿到满额 timeout →
  // 实际总预算变成 partitions.size() × timeout，最后被看门狗 abort，
  // 而真正的"某个分区搜不完"这一真因永远打不出来。
  const auto deadline = std::chrono::steady_clock::now() + timeout;

  // 收集结果
  for (size_t i = 0; i < futs.size(); ++i) {
    if (timeout > std::chrono::milliseconds::zero()) {
      auto remaining = deadline - std::chrono::steady_clock::now();
      if (remaining < std::chrono::milliseconds::zero()) {
        timed_out = true;
        kill.store(1);
        break;
      }
      auto status = futs[i].wait_for(remaining);
      if (status != std::future_status::ready) {
        timed_out = true;
        kill.store(1);
        break;
      }
    }
    bool r = futs[i].get();
    ok = ok && r;
    if (!ok) {
      // 已经确定违规：立刻让其余 partition 收手，并且不再消耗剩余预算。
      // 否则后面某个分片一旦超时，Illegal 就被掩盖成 Unknown，排查方向
      // 会被带偏成"history 太大搜不完"。
      kill.store(1);
      break;
    }
  }

  for (auto& t : threads) t.join();

  // Illegal 优先于 Unknown：只要确实查出了违规，就别报"超时没搜完"。
  if (!ok) return CheckResult::Illegal;
  if (timed_out) return CheckResult::Unknown;
  return CheckResult::Ok;
}

// ---------------------------------------------------------------------------
// PartitionAndMakeEntries：按 Partition 切片
// ---------------------------------------------------------------------------
template <typename Input, typename Output, typename State>
std::vector<std::vector<Operation<Input, Output>>> PartitionHistory(
    const Model<Input, Output, State>& model,
    const std::vector<Operation<Input, Output>>& history) {
  std::vector<std::vector<int>> indices;
  if (model.Partition) {
    indices = model.Partition(history);
  } else {
    indices.resize(1);
    indices[0].reserve(history.size());
    for (size_t i = 0; i < history.size(); ++i)
      indices[0].push_back(static_cast<int>(i));
  }

  std::vector<std::vector<Operation<Input, Output>>> result;
  result.reserve(indices.size());
  for (const auto& idx : indices) {
    std::vector<Operation<Input, Output>> sub(idx.size());
    // 用 resize+索引赋值，避免 push_back 扩容导致的 Node 悬挂指针
    for (size_t k = 0; k < idx.size(); ++k) sub[k] = history[idx[k]];
    result.push_back(std::move(sub));
  }
  return result;
}

}  // namespace

// ---------------------------------------------------------------------------
// 公开 API
// ---------------------------------------------------------------------------
template <typename Input, typename Output, typename State>
CheckResult CheckOperations(const Model<Input, Output, State>& model,
                            const std::vector<Operation<Input, Output>>& history,
                            std::chrono::milliseconds timeout) {
  if (history.empty()) return CheckResult::Ok;
  auto partitions = PartitionHistory(model, history);
  return CheckParallel(model, partitions, timeout);
}

// 显式实例化：让链接器只生成一份机器码（在 namespace porcupine 内，省掉前缀）
template CheckResult
CheckOperations<::kvraft::KvInput, ::kvraft::KvOutput, std::string>(
    const Model<::kvraft::KvInput, ::kvraft::KvOutput, std::string>&,
    const std::vector<Operation<::kvraft::KvInput, ::kvraft::KvOutput>>&,
    std::chrono::milliseconds);

}  // namespace porcupine