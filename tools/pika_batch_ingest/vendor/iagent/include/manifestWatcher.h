#pragma once
#include <deque>
#include <mutex>
#include <queue>
#include <string>
#include <unordered_set>

namespace iagent {

class ManifestWatcher {
public:
  ManifestWatcher(const std::string &queueFilePath,
                  const std::string &offsetFilePath);

  // 入队（去重 + 立即追加到磁盘队列文件）
  void enqueue(const std::string &content);

  // 是否有待消费的任务（就绪队列）
  bool hasPending();

  // 仅 peek 队头（为了兼容旧接口/调试；业务上不建议用它驱动发送）
  std::string next();

  // 新增：消费一条，把它移入 staged（待确认区）
  std::string popNext();

  // 兼容旧接口名：确认一条（从 staged 头部确认，并前移 offset）
  void ack();

  // 批量确认 n 条（FIFO）
  void ack(size_t n);

private:
  void loadOffset();
  void loadQueueFromDisk();
  void persistEnqueue(const std::string &content);
  void persistOffset();

private:
  std::string queueFilePath_;
  std::string offsetFilePath_;

  // 当前已经提交（确认）的偏移
  size_t currentOffset_{0};

  // 就绪队列（尚未消费）
  std::deque<std::string> ready_;

  // 已消费但尚未确认（等待 ack）
  std::deque<std::string> staged_;

  // 去重集合（防止同一 key 重复入队）
  std::unordered_set<std::string> seen_;

  std::mutex mutex_;
};

} // namespace iagent
