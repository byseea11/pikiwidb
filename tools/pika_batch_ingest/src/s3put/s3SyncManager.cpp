#include "s3SyncManager.h"
#include "configManager.h"
#include "manifestBuilder.h"
#include "utils/kconfig.h"
#include "utils/klog.h"
#include <aws/core/Aws.h>
#include <filesystem>

namespace s3put {
namespace fs = std::filesystem;

S3SyncManager::S3SyncManager() = default;

S3SyncManager::~S3SyncManager() {
  if (watcher_)
    watcher_->Stop();
}

// ====== 保持不变：读取配置并解析 SST 根目录 ======
std::string S3SyncManager::GetSstRoot(const std::string &config_path) {
  // 获取配置中的 "dict" 路径部分
  std::string dict =
      ConfigManager::getInstance().getConfigValue<std::string>("dict");
  if (dict.empty()) {
    throw std::runtime_error("The 'dict' configuration value is empty");
  }

  // 拼接路径
  std::filesystem::path full_path = fs::path(PROJECT_DIR) / dict;

  // 检查路径是否存在
  if (!fs::exists(full_path)) {
    throw std::runtime_error("The path does not exist: " + full_path.string());
  }

  // 路径存在，返回路径字符串
  return full_path.string();
}

bool S3SyncManager::Init(const std::string &s3_config_path,
                         std::unique_ptr<SstTracker> tracker,
                         std::unique_ptr<S3Uploader> uploader,
                         std::unique_ptr<SstWatcher> watcher,
                         std::unique_ptr<SstTracker> /*builder_unused*/)
{
  // 1) 读取配置和初始化
  sst_root_ = GetSstRoot(s3_config_path);
  if (sst_root_.empty())
  {
    LOG_ERROR("Failed to get SST root directory from config.");
    return false;
  }

  const std::string key_prefix = ConfigManager::getInstance().getConfigValue<std::string>("dict");
  const std::string state_path = fs::path(STATUSTDIC) / ConfigManager::getInstance().getConfigValue<std::string>("tracker_state_path");
  const size_t files_per_manifest = ConfigManager::getInstance().getConfigValue<size_t>("files_per_manifest");
  const std::string manifest_dir = MANIFESTDIC;
  const std::string latest_manifest = LASTMANIFEST;

  // 确保 latest_manifest 的目录存在

  fs::path latest_manifest_dir = fs::path(latest_manifest).parent_path();
  if (!fs::exists(latest_manifest_dir))
  {
    std::error_code ec;
    if (!fs::create_directories(latest_manifest_dir, ec) && ec)
    {
      LOG_ERROR("Failed to create directory for latest_manifest: " + latest_manifest_dir.string() +
                " error: " + ec.message());
      return false;
    }
  }

  // 确保 state_path 的目录存在
  fs::path state_path_dir = fs::path(state_path).parent_path();
  if (!fs::exists(state_path_dir))
  {
    std::error_code ec;
    if (!fs::create_directories(state_path_dir, ec) && ec)
    {
      LOG_ERROR("Failed to create directory for state_path: " + state_path_dir.string() +
                " error: " + ec.message());
      return false;
    }
  }

  // 配置检查
  if (key_prefix.empty() || state_path.empty())
  {
    LOG_ERROR("Config missing: key_prefix or state_path must be set.");
    return false;
  }

  // 2) 初始化 tracker
  tracker_ = tracker ? std::move(tracker) : std::make_unique<SstTracker>();
  tracker_->SetSstRoot(sst_root_);
  tracker_->SetKeyPrefix(key_prefix);

  // 尝试加载状态文件
  if (!tracker_->LoadState(state_path))
  {
    LOG_WARN("SstTracker state not found; cold start.");
  }

  // 3) 初始化 uploader
  uploader_ = uploader ? std::move(uploader) : std::make_unique<S3Uploader>(s3_config_path);

  // 4) 初始化 watcher 和回调
  // Note: We're not using the watcher for periodic scans anymore, but we'll trigger it once
  const size_t watch_interval_sec = 0; // Disable periodic scanning
  watcher_ = watcher ? std::move(watcher) : std::make_unique<SstWatcher>(*tracker_, sst_root_, pool_, watch_interval_sec);

  watcher_->SetCallback([this, files_per_manifest, manifest_dir, latest_manifest, state_path](const std::vector<std::string> &changed)
                        {
  if (changed.empty()) 
  {
    LOG_INFO("Watcher callback: no changed files.");
    return;
  }

  // 0) 生成并设置本轮 version_id
  const std::string version_id = ManifestBuilder::GenerateVersionId();
  tracker_->SetCurrentVersionId(version_id);
  LOG_INFO("Starting build/upload for version: " + version_id);

  // 1) 并发上传
  size_t upload_concurrency = std::max<size_t>(1, std::thread::hardware_concurrency());
  std::atomic<size_t> idx{0};
  std::mutex ok_mu;
  std::vector<std::string> uploaded_ok;
  uploaded_ok.reserve(changed.size());

  std::vector<std::future<void>> workers;
  workers.reserve(upload_concurrency);

  for (size_t t = 0; t < upload_concurrency; ++t)
  {
    workers.emplace_back(std::async(std::launch::async, [&, t]
    {
      for (;;)
      {
        size_t i = idx.fetch_add(1, std::memory_order_relaxed);
        if (i >= changed.size()) break;

        const std::string &file = changed[i];
        std::string key = tracker_->GenerateSstUploadKey(file);
        if (key.empty())
        {
          LOG_ERROR("Abort upload: empty S3 key for " + file);
          continue;
        }

        // 添加重试机制
        const int max_retries = 3;
        bool uploaded = false;
        for (int attempt = 0; attempt <= max_retries; ++attempt) {
          auto res = uploader_->UploadFile(file, key, "");
          
          if (!res.isError())
          {
            std::lock_guard<std::mutex> lk(ok_mu);
            uploaded_ok.push_back(file);
            tracker_->SetStatus(file, 1);
            uploaded = true;
            break;
          }
          else
          {
            std::string error_msg = "Upload failed: " + file + " -- " + res.message();
            if (attempt < max_retries) {
              LOG_WARN("Upload attempt " + std::to_string(attempt + 1) + " failed: " + error_msg + ". Retrying...");
              std::this_thread::sleep_for(std::chrono::milliseconds(100 * (attempt + 1))); // Exponential backoff
            } else {
              LOG_ERROR(error_msg);
            }
          }
        }
      }
    }));
  }

  for (auto &f : workers)
    f.get();

  LOG_INFO("Upload finished: ok=" + std::to_string(uploaded_ok.size()) + " / total=" + std::to_string(changed.size()));

  // 2) 仅使用上传成功的文件来构建 manifest
  tracker_->ReplaceChanged(uploaded_ok);
  
  const std::string key_prefix = ConfigManager::getInstance().getConfigValue<std::string>("dict");
  const std::string state_path = fs::path(STATUSTDIC) / ConfigManager::getInstance().getConfigValue<std::string>("tracker_state_path");
  const size_t files_per_manifest = ConfigManager::getInstance().getConfigValue<size_t>("files_per_manifest");
  const std::string manifest_dir = MANIFESTDIC;
  const std::string latest_manifest = LASTMANIFEST;
  
  std::vector<std::string> parts;
  bool ok = builder_->BuildAndWrite(
    *tracker_,
    std::max<size_t>(1, std::thread::hardware_concurrency()),
    manifest_dir,
    latest_manifest,
    std::max<size_t>(1, files_per_manifest),
    version_id,
    &parts);

  if (!ok)
  {
    LOG_ERROR("BuildAndWrite failed: latest.manifest not updated.");
    return;
  }
  if (parts.empty())
  {
    // 被别的进程占用锁，本次跳过
    LOG_INFO("Manifest build skipped (lock held by another process).");
    return;
  }

  // 3) 上传 manifest parts
  LOG_INFO("Uploading manifest parts ...");
  for (const auto &part : parts)
  {
    std::ifstream in(part, std::ios::binary);
    if (!in.is_open())
    {
      LOG_WARN("Create to open manifest part: " + part);
    }
    else
    {
      const std::string body((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
      auto rc = uploader_->UploadText(body, "manifest/" + fs::path(part).filename().string());
      if (rc.isError())
      {
        LOG_ERROR(std::string("Upload manifest part failed: ") + rc.message());
        return; // Exit on manifest upload failure
      }
    }
  }

  // 4) 上传 latest.manifest
  LOG_INFO("Uploading latest.manifest ...");
  {
    std::ifstream in(latest_manifest, std::ios::binary);
    if (!in.is_open())
    {
      LOG_WARN("Create to open latest.manifest: " + latest_manifest);

    }
    else
    {
      const std::string body((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
      auto rc = uploader_->UploadText(body, "manifest/latest.manifest");
      if (rc.isError())
      {
        LOG_ERROR(std::string("Upload latest.manifest failed: ") + rc.message());
        return; // Exit on failure to open latest.manifest
      }
      else
      {
        LOG_INFO("Uploaded manifest/latest.manifest");
      }
    }
  }

  // 5) 保存 tracker 状态
  if (tracker_->SaveState(state_path))
  {
    LOG_INFO("SstTracker state saved: " + state_path);
  }
  else
  {
    LOG_WARN("Failed to save SstTracker state: " + state_path);
  }

  // 6) 清空变更
  tracker_->ClearChanged();
  LOG_INFO("Watcher callback finished. version=" + version_id + " parts=" + std::to_string(parts.size()) + " files=" + std::to_string(changed.size())); });

  return true;
}

void S3SyncManager::Run() {
  // Instead of starting a continuous watcher, just trigger one scan
  if (watcher_) {
    watcher_->ScheduledScan(); // Perform a single scan and upload
  }
  LOG_INFO("S3SyncManager finished.");
}

} // namespace s3put
