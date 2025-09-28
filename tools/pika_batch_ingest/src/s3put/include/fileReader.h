#include "utils/klog.h"
#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <filesystem>
#include <functional>
#include <iostream>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace s3put {
namespace fs = std::filesystem;

class FileReader {
public:
  FileReader(const std::string &root_dir, size_t num_threads)
      : root_dir_(root_dir), num_threads_(num_threads ? num_threads : 1),
        done_(false) {}

  // 并发扫描
  void scan() {
    LOG_DEBUG("Starting concurrent scan in: " + root_dir_);

    // 清空旧数据
    {
      std::lock_guard<std::mutex> lk(files_mutex_);
      files_.clear();
    }
    total_files_.store(0);

    // 1) 启动消费者线程
    std::vector<std::thread> workers;
    workers.reserve(num_threads_);
    for (size_t i = 0; i < num_threads_; ++i) {
      workers.emplace_back([this] { this->consumer_loop(); });
    }

    // 2) 生产者遍历目录并入队
    producer_walk();

    // 3) 通知消费者：生产者结束
    {
      std::lock_guard<std::mutex> lk(q_mutex_);
      done_ = true;
    }
    q_cv_.notify_all();

    // 4) 等待消费者结束
    for (auto &t : workers)
      t.join();
  }

  std::vector<std::string> get_files() const {
    std::lock_guard<std::mutex> lk(files_mutex_);
    return files_;
  }

private:
  // ========== 队列 ==========
  void enqueue(std::string path) {
    {
      std::lock_guard<std::mutex> lk(q_mutex_);
      queue_.push(std::move(path));
    }
    q_cv_.notify_one();
  }

  bool dequeue(std::string &out) {
    std::unique_lock<std::mutex> lk(q_mutex_);
    q_cv_.wait(lk, [this] { return !queue_.empty() || done_; });
    if (queue_.empty())
      return false; // done_ == true & queue empty → 退出
    out = std::move(queue_.front());
    queue_.pop();
    return true;
  }

  // ========== 生产者：递归遍历 ==========
  void producer_walk() {
    size_t discovered = 0;
    try {
      for (auto it = fs::recursive_directory_iterator(
               root_dir_, fs::directory_options::skip_permission_denied);
           it != fs::recursive_directory_iterator(); ++it) {
        // 打印检查日志（可按需关闭）
        // std::cout << "[DEBUG] Checking: " << it->path() << std::endl;

        // 只处理常规文件
        std::error_code ec;
        if (!it->is_regular_file(ec))
          continue;

        auto ext = it->path().extension().string();
        std::transform(ext.begin(), ext.end(), ext.begin(), ::tolower);
        if (ext == ".sst") {
          enqueue(it->path().string());
          ++discovered;
        }
      }
    } catch (const std::exception &e) {
      std::cerr << "[WARN] producer_walk exception: " << e.what() << std::endl;
    }
    std::cout << "[DEBUG] Total .sst files discovered: " << discovered
              << std::endl;
  }

  // ========== 消费者：出队处理 ==========
  void consumer_loop() {
    // 可选：本地缓存，减少 files_mutex_ 争用
    std::vector<std::string> local_files;
    local_files.reserve(256);

    std::string path;
    while (true) {
      if (!dequeue(path))
        break; // 队列空且 done -> 退出

      local_files.push_back(std::move(path));
      total_files_.fetch_add(1, std::memory_order_relaxed);

      // 打印
      // std::cout << "[DEBUG] Added file: " << local_files.back() << std::endl;

      // 批量汇入共享数组，减少锁竞争
      if (local_files.size() >= 256) {
        flush_local(local_files);
      }
    }
    // 收尾：把剩余的缓存写回
    flush_local(local_files);
  }

  void flush_local(std::vector<std::string> &local) {
    if (local.empty())
      return;
    std::lock_guard<std::mutex> lk(files_mutex_);
    files_.insert(files_.end(), std::make_move_iterator(local.begin()),
                  std::make_move_iterator(local.end()));
    local.clear();
  }

private:
  std::string root_dir_;
  size_t num_threads_;

  // 共享结果
  mutable std::mutex files_mutex_;
  std::vector<std::string> files_;

  // 统计
  std::atomic<int> total_files_{0};

  // 队列与同步
  std::queue<std::string> queue_;
  std::mutex q_mutex_;
  std::condition_variable q_cv_;
  bool done_;
};

} // namespace s3put
