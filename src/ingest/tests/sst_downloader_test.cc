#include <gtest/gtest.h>
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/transfer/TransferManager.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <filesystem>
#include <fstream>
#include <unordered_map>
#include <unordered_set>
#include <memory>
#include <string>
#include <vector>

#include "sst_downloader.h"  // SstDownloader（按你工程的包含路径调整）

// 如果你的 Manifest protobuf 在 sst_downloader 内部定义/解析，不需要这行；
// 若需要自行构造清单（下面用到了），请确保生成了对应的头：
// #include "manifest.pb.h"

using Aws::S3::Model::GetObjectOutcome;
using Aws::S3::Model::GetObjectRequest;
using Aws::S3::S3Client;
using Aws::Client::AWSError;
using Aws::S3::S3Errors;

namespace fs = std::filesystem;

// ============== 简易 Manifest 构造（如果 SstDownloader::ParseManifest 只接收纯文本列表，改这里） ==============
// 若你的清单是 protobuf，请在工程里包含 manifest.pb.h，然后把这里改成真正构造 protobuf。
static std::string BuildManifestBytes(const std::vector<std::string>& sst_paths) {
  // 方案 A：每行一个相对路径（若你的实现支持纯文本）
  // 方案 B：protobuf：把这里替换为 message Manifest { repeated string sst_path = 1; } 的序列化
  std::string out;
  for (const auto& p : sst_paths) {
    out.append(p);
    out.push_back('\n');
  }
  return out;
}

// ============== Fake S3Client：从内存字典里返回对象，或按注册的错误码报错 ==============
class FakeS3Client : public S3Client {
 public:
  using S3Client::S3Client;

  void PutObjectBody(const std::string& key, const std::string& body) {
    key_to_body_[key] = body;
    error_keys_.erase(key);
  }

  void PutErrorKey(const std::string& key, S3Errors code = S3Errors::NO_SUCH_KEY,
                   const char* msg = "no such key") {
    error_keys_.insert(key);
    err_code_[key] = code;
    err_msg_[key] = msg;
    key_to_body_.erase(key);
  }

  // --- FakeS3Client::GetObject ---
GetObjectOutcome GetObject(const GetObjectRequest& request) const override {
  const std::string key = request.GetKey().c_str();

  if (error_keys_.count(key)) {
    AWSError<S3Errors> err(err_code_.at(key), false);
    err.SetMessage(err_msg_.at(key).c_str());
    return GetObjectOutcome(err);
  }

  auto it = key_to_body_.find(key);
  if (it == key_to_body_.end()) {
    AWSError<S3Errors> err(S3Errors::NO_SUCH_KEY, false);
    err.SetMessage("not found");
    return GetObjectOutcome(err);
  }

  Aws::S3::Model::GetObjectResult result;
  auto* stream = Aws::New<Aws::StringStream>("FakeS3GetObjectBody");
  (*stream) << it->second;
  result.ReplaceBody(stream); 
  return GetObjectOutcome(std::move(result));
}


 private:
    std::unordered_map<std::string, std::string> key_to_body_;
    std::unordered_set<std::string> error_keys_;
    std::unordered_map<std::string, S3Errors> err_code_;
    std::unordered_map<std::string, std::string> err_msg_;
};

// ============== 测试夹具：初始化/关闭 AWS SDK；每个用例创建独立 TransferManager ==============
class SSTDownloaderTest : public ::testing::Test {
protected:
  static void SetUpTestSuite() {
    Aws::InitAPI(options_);
  }
  static void TearDownTestSuite() {
    Aws::ShutdownAPI(options_);
  }

  // --- Fixture::SetUp() ---
    void SetUp() override {
    fake_client_ = std::make_shared<FakeS3Client>(Aws::Client::ClientConfiguration{});

    executor_ = Aws::MakeShared<Aws::Utils::Threading::DefaultExecutor>("TestExec");
    Aws::Transfer::TransferManagerConfiguration cfg(executor_.get()); 
    cfg.s3Client = fake_client_;
    cfg.transferBufferMaxHeapSize = 8 * 1024 * 1024;
    xfer_mgr_ = Aws::Transfer::TransferManager::Create(cfg);

    bucket_ = "test-bucket";
    manifest_prefix_ = "manifest/";
    sst_prefix_ = "sst/";
    fs::remove_all("data");
    }

    std::shared_ptr<Aws::Utils::Threading::Executor> executor_; 


  // 便捷：注册一组 SST 与 Manifest
  void PutManifestAndSsts(const std::string& manifest_name,
                          const std::vector<std::string>& sst_paths,
                          const std::vector<std::string>& sst_bodies) {
    ASSERT_EQ(sst_paths.size(), sst_bodies.size());
    const std::string manifest_key = manifest_prefix_ + manifest_name;
    fake_client_->PutObjectBody(manifest_key, BuildManifestBytes(sst_paths));
    for (size_t i = 0; i < sst_paths.size(); ++i) {
      fake_client_->PutObjectBody(sst_prefix_ + sst_paths[i], sst_bodies[i]);
    }
  }

protected:
  static Aws::SDKOptions options_;
  std::shared_ptr<FakeS3Client> fake_client_;
  std::shared_ptr<Aws::Transfer::TransferManager> xfer_mgr_;
  std::string bucket_;
  std::string manifest_prefix_;
  std::string sst_prefix_;
};

Aws::SDKOptions SSTDownloaderTest::options_;

// ============== 用例 1：Manifest 正常，下载两份 SST 成功 ==============
TEST_F(SSTDownloaderTest, DownloadAllFiles_OK) {
  const std::string manifest_name = "job001.manifest";
  std::vector<std::string> sst_paths = {"user/a.sst", "user/b.sst"};
  std::vector<std::string> sst_bodies = {"AAAA", "BBBB"};

  PutManifestAndSsts(manifest_name, sst_paths, sst_bodies);

  SstDownloader dl(fake_client_, xfer_mgr_, bucket_, manifest_prefix_, sst_prefix_);
  std::vector<std::string> out;
  auto st = dl.DownloadAllFiles(manifest_name, out);
  EXPECT_TRUE(st.ok()) << st.ToString();
  ASSERT_EQ(out.size(), sst_paths.size());

  for (const auto& p : out) {
    EXPECT_TRUE(fs::exists(p)) << "missing " << p;
    EXPECT_NE(p.find("data/"), std::string::npos) << "path not under data/: " << p;
  }
}

// ============== 用例 2：空 Manifest（无 sst）=> OK 且结果为空 ==============
TEST_F(SSTDownloaderTest, EmptyManifest_OkNoFiles) {
  const std::string manifest_name = "empty.manifest";
  const std::string manifest_key = manifest_prefix_ + manifest_name;
  fake_client_->PutObjectBody(manifest_key, BuildManifestBytes({}));

  SstDownloader dl(fake_client_, xfer_mgr_, bucket_, manifest_prefix_, sst_prefix_);
  std::vector<std::string> out;
  auto st = dl.DownloadAllFiles(manifest_name, out);
  EXPECT_TRUE(st.ok());
  EXPECT_TRUE(out.empty());
}

// ============== 用例 3：Manifest 不存在 => IOError ==============
TEST_F(SSTDownloaderTest, ManifestMissing_IOError) {
  const std::string manifest_name = "notfound.manifest";
  const std::string manifest_key = manifest_prefix_ + manifest_name;
  fake_client_->PutErrorKey(manifest_key, S3Errors::NO_SUCH_KEY, "missing");

  SstDownloader dl(fake_client_, xfer_mgr_, bucket_, manifest_prefix_, sst_prefix_);
  std::vector<std::string> out;
  auto st = dl.DownloadAllFiles(manifest_name, out);
  EXPECT_TRUE(st.IsIOError());
}

// ============== 用例 4：Manifest 含非法路径（.. / 绝对路径）=> Corruption ==============
TEST_F(SSTDownloaderTest, ManifestHasIllegalPath_Corruption) {
  const std::string manifest_name = "evil.manifest";
  const std::string manifest_key = manifest_prefix_ + manifest_name;
  // 若你的实现对路径有更严格的规则（例如必须以某个前缀开头），可在这里叠加样例
  fake_client_->PutObjectBody(manifest_key, BuildManifestBytes({"../escape.sst", "/abs/xx.sst"}));

  SstDownloader dl(fake_client_, xfer_mgr_, bucket_, manifest_prefix_, sst_prefix_);
  std::vector<std::string> out;
  auto st = dl.DownloadAllFiles(manifest_name, out);
  EXPECT_TRUE(st.IsCorruption());
}

// ============== 用例 5：中途某个 sst 对象缺失 => IOError ==============
TEST_F(SSTDownloaderTest, OneSstMissing_IOError) {
  const std::string manifest_name = "partial.manifest";
  const std::string manifest_key = manifest_prefix_ + manifest_name;

  // 清单列出两份，但只提供其中一份
  std::vector<std::string> sst_paths = {"user/a.sst", "user/b.sst"};
  fake_client_->PutObjectBody(manifest_key, BuildManifestBytes(sst_paths));
  fake_client_->PutObjectBody(sst_prefix_ + "user/a.sst", "AAAA");
  // b.sst 故意不放，或注册错误
  fake_client_->PutErrorKey(sst_prefix_ + "user/b.sst", S3Errors::NO_SUCH_KEY, "missing b");

  SstDownloader dl(fake_client_, xfer_mgr_, bucket_, manifest_prefix_, sst_prefix_);
  std::vector<std::string> out;
  auto st = dl.DownloadAllFiles(manifest_name, out);
  EXPECT_TRUE(st.IsIOError());
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}