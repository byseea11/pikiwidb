// sst_downloader_minio_it.cc
#include <gtest/gtest.h>
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/core/utils/logging/LogLevel.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/HeadBucketRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/transfer/TransferManager.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/core/utils/threading/PooledThreadExecutor.h>

#include <filesystem>
#include <fstream>
#include <random>
#include <string>
#include <vector>

#include "manifest.pb.h"
#include "sst_downloader.h"

namespace fs = std::filesystem;
using namespace Aws;

// ================== 你的 MinIO 配置（来自你发的 JSON） ==================
static const char* CFG_ENDPOINT = "http://127.0.0.1:9000";
static const char* CFG_REGION   = "ap-northeast-3";
static const char* CFG_BUCKET   = "pika-sst";
static const char* CFG_AK       = "minioadmin";
static const char* CFG_SK       = "minioadmin";
static const bool  CFG_IS_MINIO = true;          // path-style

static const int    CFG_TRANSFER_THREADS = 32;
static const size_t CFG_TRANSFER_BUF     = 8 * 1024 * 1024; // 8MiB

// （这些重试参数 SDK 里无法一一逐项设置，这里只设置最大尝试次数）
static const int CFG_RETRY_MAX_ATTEMPTS = 3;

// ================== 工具函数 ==================
static std::string RandSuffix(size_t n = 8) {
  static const char* k = "abcdefghijklmnopqrstuvwxyz0123456789";
  std::mt19937_64 rng{std::random_device{}()};
  std::uniform_int_distribution<size_t> d(0, 35);
  std::string s; s.reserve(n);
  for (size_t i=0;i<n;++i) s.push_back(k[d(rng)]);
  return s;
}

static std::string BuildManifestBytes(const std::vector<std::string>& sst_paths,
                                      const std::string& version = "v1",
                                      int64_t ts = 123456789) {
  ManifestIngest::Manifest m;
  m.set_version_id(version);
  m.set_timestamp(ts);
  for (const auto& p : sst_paths) {
    auto* f = m.add_sst_files();
    f->set_sst_path(p);
    f->set_hash("deadbeef");
    f->set_file_size(1234);
  }
  std::string out;
  if (!m.SerializeToString(&out)) throw std::runtime_error("SerializeToString failed");
  return out;
}

// ================== 集成测试夹具：真实 MinIO S3 + 你的 TransferManager ==================
class SSTDownloaderTest : public ::testing::Test {
protected:
  static void SetUpTestSuite() {
    SDKOptions o;
    o.loggingOptions.logLevel = Utils::Logging::LogLevel::Off;
    options_ = o;
    // 关闭 IMDS 避免阻塞
    setenv("AWS_EC2_METADATA_DISABLED", "true", 1);
    Aws::InitAPI(options_);
  }
  static void TearDownTestSuite() {
    Aws::ShutdownAPI(options_);
  }

  void SetUp() override {
    // 1) S3 客户端配置（MinIO 推荐 path-style）
    Client::ClientConfiguration cfg;
    cfg.region = CFG_REGION;
    cfg.endpointOverride = CFG_ENDPOINT;
    cfg.scheme = Http::Scheme::HTTP;
    cfg.verifySSL = false;
    cfg.useDualStack = false;
    cfg.connectTimeoutMs = 5000;
    cfg.requestTimeoutMs = 120000;
    cfg.maxConnections = 64;
    cfg.retryStrategy = Aws::MakeShared<Client::DefaultRetryStrategy>("Retry", CFG_RETRY_MAX_ATTEMPTS);

    Aws::Auth::AWSCredentials creds(CFG_AK, CFG_SK);

    s3_ = std::make_shared<S3::S3Client>(
      creds, cfg,
      Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
      /*useVirtualAddressing*/ !CFG_IS_MINIO ? true : false // MinIO: false
    );

    bucket_ = CFG_BUCKET;

    // 给每次运行一个唯一前缀，避免污染：manifest/<run>/、sst/<run>/
    run_tag_ = RandSuffix();
    manifest_prefix_ = std::string("manifest/") + run_tag_ + "/";
    sst_prefix_      = std::string("sst/")      + run_tag_ + "/";

    // 2) 确保 bucket 存在
    {
      S3::Model::HeadBucketRequest hbr; hbr.SetBucket(bucket_.c_str());
      auto hbo = s3_->HeadBucket(hbr);
      if (!hbo.IsSuccess()) {
        S3::Model::CreateBucketRequest cbr; cbr.SetBucket(bucket_.c_str());
        auto cbro = s3_->CreateBucket(cbr);
        ASSERT_TRUE(cbro.IsSuccess()) << "CreateBucket failed: " << cbro.GetError().GetMessage();
      }
    }

    // 3) TransferManager
  auto pool = Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>("ExecPool", 1); // 单线程，测试更稳
  executor_ = pool;  // 保存以延长生命周期
  Aws::Transfer::TransferManagerConfiguration tcfg(pool.get());
  tcfg.s3Client = s3_;
  tcfg.bufferSize = CFG_TRANSFER_BUF;
  tcfg.transferBufferMaxHeapSize = CFG_TRANSFER_BUF * 4;
  xfer_mgr_ = Aws::Transfer::TransferManager::Create(tcfg);

    // 清空本地 data 目录
    fs::remove_all("data");
  }

  void TearDown() override {
    // 清理本地
    fs::remove_all("data");

    // 删除当前 run 的对象（不删 bucket）
    for (auto& k : uploaded_keys_) {
      S3::Model::DeleteObjectRequest req;
      req.SetBucket(bucket_.c_str());
      req.SetKey(k.c_str());
      s3_->DeleteObject(req);
    }

    xfer_mgr_.reset();
    s3_.reset();
  }

  // ====== 测试上传工具 ======
  void PutObjectString(const std::string& key, const std::string& payload) {
    S3::Model::PutObjectRequest req;
    req.SetBucket(bucket_.c_str());
    req.SetKey(key.c_str());
    auto body = Aws::MakeShared<Aws::StringStream>("Body");
    (*body) << payload;
    req.SetBody(body);
    req.SetContentLength(static_cast<long long>(payload.size()));
    auto out = s3_->PutObject(req);
    ASSERT_TRUE(out.IsSuccess()) << "PutObject failed key=" << key << " err=" << out.GetError().GetMessage();
    uploaded_keys_.push_back(key);
  }

  // 便捷：注册一组 SST 与 Manifest
  void PutManifestAndSsts(const std::string& manifest_name,
                          const std::vector<std::string>& sst_paths,
                          const std::vector<std::string>& sst_bodies) {
    ASSERT_EQ(sst_paths.size(), sst_bodies.size());
    // 上传 manifest（protobuf）
    const std::string manifest_key = manifest_prefix_ + manifest_name;
    PutObjectString(manifest_key, BuildManifestBytes(sst_paths));
    // 上传 sst 对象（key 使用 sst_paths 的值）
    for (size_t i = 0; i < sst_paths.size(); ++i) {
      PutObjectString(sst_paths[i], sst_bodies[i]);
    }
  }

protected:
  static Aws::SDKOptions options_;
  std::shared_ptr<S3::S3Client>                    s3_;
  std::shared_ptr<Aws::Transfer::TransferManager>  xfer_mgr_;
  std::shared_ptr<Aws::Utils::Threading::Executor> executor_;

  std::string bucket_, manifest_prefix_, sst_prefix_, run_tag_;
  std::vector<std::string> uploaded_keys_; // 用于 TearDown 清理
};

Aws::SDKOptions SSTDownloaderTest::options_;

// ================== 用例 1：正常下载两份 SST（端到端） ==================
TEST_F(SSTDownloaderTest, DownloadAllFiles_OK) {
  const std::string manifest_name = "job001.manifest";
  std::vector<std::string> sst_paths  = { sst_prefix_ + "a.sst", sst_prefix_ + "b.sst" };
  std::vector<std::string> sst_bodies = { "AAAA", "BBBB" };

  PutManifestAndSsts(manifest_name, sst_paths, sst_bodies);

  SstDownloader dl(s3_, xfer_mgr_, bucket_, manifest_prefix_, ""); // sst_dict_ 为空：我们已提供完整 sst/.. key
  std::vector<std::string> out;
  auto st = dl.DownloadAllFiles(manifest_name, out);
  EXPECT_TRUE(st.ok()) << st.ToString();
  ASSERT_EQ(out.size(), sst_paths.size());

  for (size_t i = 0; i < out.size(); ++i) {
    EXPECT_TRUE(fs::exists(out[i])) << "missing " << out[i];
    std::ifstream ifs(out[i], std::ios::binary);
    std::string got((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
    EXPECT_EQ(got, sst_bodies[i]);
    EXPECT_NE(out[i].find("data/"), std::string::npos);
  }
}

// ================== 用例 2：空 manifest（不含任何 sst） ==================
TEST_F(SSTDownloaderTest, EmptyManifest_OkNoFiles) {
  const std::string manifest_name = "empty.manifest";
  const std::string manifest_key = manifest_prefix_ + manifest_name;
  PutObjectString(manifest_key, BuildManifestBytes({}));

  SstDownloader dl(s3_, xfer_mgr_, bucket_, manifest_prefix_, "");
  std::vector<std::string> out;
  auto st = dl.DownloadAllFiles(manifest_name, out);
  EXPECT_TRUE(st.ok());
  EXPECT_TRUE(out.empty());
}

// ================== 用例 3：manifest 不存在 ==================
TEST_F(SSTDownloaderTest, ManifestMissing_IOError) {
  const std::string manifest_name = "notfound.manifest";
  // 不上传 manifest

  SstDownloader dl(s3_, xfer_mgr_, bucket_, manifest_prefix_, "");
  std::vector<std::string> out;
  auto st = dl.DownloadAllFiles(manifest_name, out);
  EXPECT_TRUE(st.IsIOError());
}

// ================== 用例 4：manifest 含非法路径（.. / 绝对路径） ==================
TEST_F(SSTDownloaderTest, ManifestHasIllegalPath_Corruption) {
  const std::string manifest_name = "evil.manifest";
  const std::string manifest_key = manifest_prefix_ + manifest_name;
  // 直接写进 manifest：非法路径
  PutObjectString(manifest_key, BuildManifestBytes({"../escape.sst", "/abs/xx.sst"}));

  SstDownloader dl(s3_, xfer_mgr_, bucket_, manifest_prefix_, "");
  std::vector<std::string> out;
  auto st = dl.DownloadAllFiles(manifest_name, out);
  EXPECT_TRUE(st.IsCorruption());
}

// ================== 用例 5：清单列出两份，但只上传其中一份 ==================
TEST_F(SSTDownloaderTest, OneSstMissing_IOError) {
  const std::string manifest_name = "partial.manifest";
  const std::vector<std::string> sst_paths = { sst_prefix_ + "user/a.sst", sst_prefix_ + "user/b.sst" };

  // 上传 manifest，列出两份 SST
  const std::string manifest_key = manifest_prefix_ + manifest_name;
  PutObjectString(manifest_key, BuildManifestBytes(sst_paths));
  // 只上传第一份
  PutObjectString(sst_paths[0], "AAAA");
  // 第二份故意不传

  SstDownloader dl(s3_, xfer_mgr_, bucket_, manifest_prefix_, "");
  std::vector<std::string> out;
  auto st = dl.DownloadAllFiles(manifest_name, out);
  EXPECT_TRUE(st.IsIOError());
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
