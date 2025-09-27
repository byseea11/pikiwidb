#pragma once
#include <chrono>
#include <deque>
#include <memory>
#include <optional>
#include <string>
#include <vector>

// 前置声明（来自 hiredis）
struct redisContext;
struct redisReply;

namespace iagent {

// 逐条回复结果
struct BurstResult {
  bool ok{true};
  std::string err; // 失败原因
  std::string tag; // 业务标识（比如 manifest key）
};

// 连接参数
struct Endpoint {
  std::string host;
  int port{0};
  int connect_timeout_ms{3000}; // 连接超时
  int rw_timeout_ms{5000};      // 读写超时（hiredis层）
};

// 一条待发记录
struct BurstItem {
  std::string payload; // 命令参数（比如文件名或JSON），按 %b 发送
  std::string tag;     // 业务标识
};

class PipelinedBurst {
public:
  // connections: 建议 4~16；max_inflight_per_conn=0 表示不限制
  explicit PipelinedBurst(const Endpoint &ep, size_t connections = 4,
                          size_t max_inflight_per_conn = 0);
  ~PipelinedBurst();

  PipelinedBurst(const PipelinedBurst &) = delete;
  PipelinedBurst &operator=(const PipelinedBurst &) = delete;

  // 追加一条（不等待回复）
  bool append(const BurstItem &item);

  // 返回成功 append 的条数；若 rejected 非空指针，会把未追加的项放进去
  size_t appendAll(const std::vector<BurstItem> &items,
                   std::vector<BurstItem> *rejected = nullptr);

  // 读取已追加的全部回复（或直到超时）
  // force_quick_exit=true 时，如果 inflight==0，立即返回，不等 max_wait
  std::vector<BurstResult> drainReplies(std::chrono::milliseconds max_wait,
                                        bool force_quick_exit = true);


  // 便捷：appendAll + drainReplies
  std::vector<BurstResult> sendAndDrain(const std::vector<BurstItem> &items,
                                        std::chrono::milliseconds max_wait);

  // 当前仍未收完的条数
  size_t inflight() const;

  size_t connectionCount() const { return conns_.size(); }

private:
  struct Conn {
    redisContext *ctx{nullptr};
    size_t inflight{0}; // 已 append 尚未读取回复的数量
    size_t id{0};

    // 与 inflight 对齐的标签 FIFO 队列
    std::deque<std::string> tags;

    // 断链时把余下未返回的请求生成失败，先堆到这里；drainReplies() 每轮优先吐出
    std::deque<BurstResult> pending_failures;
  };

  bool ensureConnected_(Conn &c);
  bool appendOne_(Conn &c, const BurstItem &item);
  std::optional<BurstResult> getOneReply_(Conn &c);

  // 从 conn.pending_failures 倾倒到 out
  static bool drainPendingFailures_(Conn &c, std::vector<BurstResult> &out);

  // 选择一个可用连接（考虑 max_inflight_per_conn_），若全部不可用返回 nullptr
  Conn *pickConnForAppend_();

  // 仅声明：实现放 .cpp（避免在 .h 里包含 hiredis）
  static void closeConn_(Conn &c);

private:
  Endpoint ep_;
  std::vector<std::unique_ptr<Conn>> conns_;
  size_t rr_{0}; // 轮转下标
  size_t max_inflight_per_conn_{0};
};

} // namespace iagent
