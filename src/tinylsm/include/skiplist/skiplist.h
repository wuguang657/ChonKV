#include <string>
#include <vector>

// 层级3   HEAD ───────────────────────────────→ 30
//               │                               │
// 层级2   HEAD ───────→ 10 ──────────────────→ 30
//               │         │                     │
// 层级1   HEAD ──→ 5 ──→ 10 ───────→ 20 ─────→ 30
//               │      │    │        │         │
// 层级0   HEAD ─→ 5 ─→ 10 ─→ 15 ─→ 20 ─→ 25 ─→ 30    （底层是完整有序链表）

// 节点 20（它在 L0、L1 两层）的 forward_ 数组：
// forward_[0] → 25    // L0 上的右边节点
// forward_[1] → 30    // L1 上的右边节点（跳过了 25，因为 25 只有 L0）
namespace tinylsm {
    // ************************ SkipListNode ************************
    struct SkipListNode {
        std::string key; // 节点存储的键
        std::string value; // 节点存储的值
        uint64_t tranc_id; // 事务 id
        // tranc_id 给每个 key 的每个版本盖了一个"事务时间戳"，读取时靠它判断哪些版本对当前事务可见，从而实现无锁快照读
        // A 被 B 的 backward 指着 → A 的计数 = 1 （改成 weak_ptr 后，计数 = 0）
        // B 被 A 的 forward  指着 → B 的计数 = 1
        std::vector<std::shared_ptr<SkipListNode>> forward_; // 指向不同层级的下一个节点的指针数组
        std::vector<std::weak_ptr<SkipListNode>> backward_; // 指向不同层级的上一个节点的指针数组
        
        // level 表示这个节点被分配到的跳表层数
        // level 是随机生成的节点层高，构造函数用它决定给这个节点开几个前向/后向指针槽位。
        SkipListNode(const std::string &k, const std::string &v, int level,
               uint64_t tranc_id):
            key(k), value(v), tranc_id(tranc_id), forward_(level), backward_(level) {}

        void set_backward(int level, std::shared_ptr<SkipListNode> node) {
            backward_[level] = std::weak_ptr<SkipListNode>(node); // weak_ptr不增加计数，函数结束计数-1
        }

        bool operator==(const SkipListNode &other) const {
            return key == other.key && value == other.value &&
                tranc_id == other.tranc_id;
        }

        bool operator!=(const SkipListNode &other) const {
            // 直接复用 == 再取反，保证 != 与 == 严格互补，不重复写比较逻辑
            // key != other.key || value != other.value || tranc_id != other.tranc_id;
            return !(*this == other);
        }

        bool operator<(const SkipListNode &other) const {
            if (key == other.key) {
            // key 相等时，trans_id 更大的优先级更高
            return tranc_id > other.tranc_id;
            }
            return key < other.key;
        }
        bool operator>(const SkipListNode &other) const {
            if (key == other.key) {
            // key 相等时，trans_id 更大的优先级更高
            return tranc_id < other.tranc_id;
            }
            return key > other.key;
        }
    };
}
