// 最小 porcupine 自测：用最简单的 IntOp / IntOut / IntState 模型验证算法本身
//
// 状态机语义：Step(state, inp, out) 把 state 加上 inp.x；要求 out.v == 新 state。
//   Put(x) → inp.x=x, out.v=新值  （覆盖语义通过 "加" 实现，简化为 add）
//   Get     → inp.x=0, out.v=当前 state
//
// 历史排序：按 (call_time, return_time) 排好进 porcupine：
//   每个 op 拆成 call + return 两条 entry，call 在前 return 在后。
//
// 三组用例：
//   1. 单线程顺序，三条 op 都自洽        → 期望 Ok
//   2. 两条 Put 并发交错，但 Get 拿到合法值 → 期望 Ok（线性化：A→B→Get）
//   3. 两条 Put 都返回后 Get 触发，Get 却读到 Put 之前的状态 → 期望 Illegal
//
// 编译：
//   g++ -std=c++17 -I src/kvraft \
//       -o /tmp/porc_selftest \
//       src/kvraft/porcupine.cpp \
//       src/kvraft/test_porcupine_selftest.cpp

#include <chrono>
#include <cstdio>
#include "porcupine.h"

// porcupine.cpp 的实现直接在 selftest 里 #include，这样编译器在同一个 TU 里
// 看到 CheckOperations 的定义 + 三个 IntOp 测试调用，能为 IntOp/IntOut/IntState
// 自动发出机器码。否则只 #include 头文件，链接器找不到定义。
//
// 编译时不要再单列 porcupine.cpp，否则会重复定义符号。
#include "porcupine.cpp"

// 简单的 int 状态机
struct IntState {
  int v = 0;
};

struct IntOp {
  int x = 0;  // 加到 state 上的增量
};

struct IntOut {
  int v = 0;  // 期望的新 state
};

int main() {
  using namespace porcupine;
  using Op = Operation<IntOp, IntOut>;
  using M = Model<IntOp, IntOut, IntState>;

  M m;
  m.Init = [] { return IntState{0}; };
  m.Step = [](const IntState& s, const IntOp& inp, const IntOut& out) {
    IntState ns{s.v + inp.x};
    if (out.v != ns.v) return std::make_pair(false, ns);
    return std::make_pair(true, ns);
  };
  m.Equal = [](const IntState& a, const IntState& b) { return a.v == b.v; };
  m.Partition = [](const std::vector<Op>&) {
    return std::vector<std::vector<int>>{{}};
  };
  // 默认 partition 给所有 idx 进一个桶
  m.Partition = [](const std::vector<Op>& hist) {
    std::vector<std::vector<int>> idx(1);
    idx[0].reserve(hist.size());
    for (size_t i = 0; i < hist.size(); ++i) idx[0].push_back(static_cast<int>(i));
    return idx;
  };

  // ---------- 历史 1：单线程顺序，全部自洽 ----------
  //   Put 1: 0→1, out=1
  //   Put 2: 1→3, out=3
  //   Get  : state=3, out=3
  //   期望：Ok
  {
    std::vector<Op> hist = {
        {0, 0, IntOp{1}, IntOut{1}, 100, 200},  // Put 1
        {0, 0, IntOp{2}, IntOut{3}, 300, 400},  // Put 2
        {0, 0, IntOp{0}, IntOut{3}, 500, 600},  // Get → state=3
    };
    auto r = CheckOperations(m, hist, std::chrono::milliseconds(1000));
    std::printf("[test 1] 单线程顺序: result=%d  (期望 0=Ok)\n", (int)r);
  }

  // ---------- 历史 2：交错但 Get 拿到合法值 ----------
  //   Put 1: 0→1, call=100 return=400  (长)
  //   Put 2: 1→3, call=200 return=300  (短)
  //   Get  : state=3, call=500 return=600
  //   线性化：Put1→Put2→Get，state 序列 0→1→3→3，Get 看到 3 ✓
  //   期望：Ok
  {
    std::vector<Op> hist = {
        {0, 0, IntOp{1}, IntOut{1}, 100, 400},
        {0, 0, IntOp{2}, IntOut{3}, 200, 300},
        {0, 0, IntOp{0}, IntOut{3}, 500, 600},
    };
    auto r = CheckOperations(m, hist, std::chrono::milliseconds(1000));
    std::printf("[test 2] 交错但合法: result=%d  (期望 0=Ok)\n", (int)r);
  }

  // ---------- 历史 3：Get 拿到陈旧值 ----------
  //   Put 1: 0→1, call=100 return=200  (先完成)
  //   Put 2: 1→3, call=300 return=400  (后完成)
  //   Get  : out=1, call=500 return=600  (在两 Put 之后开始，却拿到 1)
  //   Get 必须在两条 Put 都被线性化之后才开始（call=500 都晚于 return），
  //   此时 state=3，而 Get 期望 1 —— 任何全序排列都不满足 → Illegal
  {
    std::vector<Op> hist = {
        {0, 0, IntOp{1}, IntOut{1}, 100, 200},
        {0, 0, IntOp{2}, IntOut{3}, 300, 400},
        {0, 0, IntOp{0}, IntOut{1}, 500, 600},
    };
    auto r = CheckOperations(m, hist, std::chrono::milliseconds(1000));
    std::printf("[test 3] Get 陈旧: result=%d  (期望 1=Illegal)\n", (int)r);
  }

  return 0;
}
