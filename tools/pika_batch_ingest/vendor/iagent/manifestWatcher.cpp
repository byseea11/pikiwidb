#include "manifestWatcher.h"
#include "utils/klog.h"
#include <fstream>
#include <sstream>

namespace iagent {

ManifestWatcher::ManifestWatcher(const std::string &queueFilePath,
                                 const std::string &offsetFilePath)
    : queueFilePath_(queueFilePath), offsetFilePath_(offsetFilePath) {
  loadOffset();
  loadQueueFromDisk();
}

void ManifestWatcher::loadOffset() {
  std::ifstream in(offsetFilePath_);
  if (in) {
    in >> currentOffset_;
  } else {
    currentOffset_ = 0;
  }
}

void ManifestWatcher::loadQueueFromDisk() {
  // 队列文件按 “每行一个条目” 存，前面的 currentOffset_ 行已确认
  std::ifstream in(queueFilePath_);
  if (!in)
    return;

  std::string line;
  size_t lineIdx = 0;

  while (std::getline(in, line)) {
    if (line.empty())
      continue;
    seen_.insert(line);
    if (lineIdx++ >= currentOffset_) {
      ready_.push_back(line);
    }
  }
}

void ManifestWatcher::persistEnqueue(const std::string &content) {
  std::ofstream out(queueFilePath_, std::ios::app);
  out << content << "\n";
  out.flush();
}

void ManifestWatcher::persistOffset() {
  std::ofstream out(offsetFilePath_, std::ios::trunc);
  out << currentOffset_ << "\n";
  out.flush();
}

void ManifestWatcher::enqueue(const std::string &content) {
  std::lock_guard<std::mutex> lk(mutex_);
  if (content.empty())
    return;

  if (seen_.insert(content).second) {
    // 新条目
    ready_.push_back(content);
    persistEnqueue(content);
    LOG_DEBUG("[ManifestWatcher] Enqueueing content: " + content);
  } else {
    // 已存在，忽略
    LOG_DEBUG("[ManifestWatcher] Duplicate ignored: " + content);
  }
}

bool ManifestWatcher::hasPending() {
  std::lock_guard<std::mutex> lk(mutex_);
  return !ready_.empty();
}

std::string ManifestWatcher::next() {
  // 仅 peek：不前移，不触碰 staged_/offset
  std::lock_guard<std::mutex> lk(mutex_);
  if (ready_.empty())
    return {};
  return ready_.front();
}

std::string ManifestWatcher::popNext() {
  // 真实消费：从 ready_ 弹出 -> 放入 staged_
  std::lock_guard<std::mutex> lk(mutex_);
  if (ready_.empty())
    return {};
  std::string k = std::move(ready_.front());
  ready_.pop_front();
  staged_.push_back(k);
  return k;
}

void ManifestWatcher::ack() {
  // 兼容旧接口：确认 staged 的一条（FIFO）
  std::lock_guard<std::mutex> lk(mutex_);
  if (staged_.empty())
    return;

  // 按顺序提交偏移
  staged_.pop_front();
  ++currentOffset_;
  persistOffset();
}

void ManifestWatcher::ack(size_t n) {
  std::lock_guard<std::mutex> lk(mutex_);
  n = std::min(n, staged_.size());
  if (n == 0)
    return;

  for (size_t i = 0; i < n; ++i) {
    staged_.pop_front();
    ++currentOffset_;
  }
  persistOffset();
}

} // namespace iagent
