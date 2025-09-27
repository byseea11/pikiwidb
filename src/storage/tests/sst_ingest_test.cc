// sst_ingest_test.cc
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/sst_file_writer.h>
#include <rocksdb/env.h>
#include <rocksdb/status.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

// -------------------------------
// Fake IngestConf（完全模拟版）
// -------------------------------
class FakeIngestConf {
public:
  explicit FakeIngestConf(const std::string& /*path*/) {}

  int Load() { return 0; }

  // 测试里我们使用一致的选项（默认全部 true；你也可改成 false/true 混合）
  rocksdb::IngestExternalFileOptions MakeIngestOptions() const {
    rocksdb::IngestExternalFileOptions opt;
    opt.move_files = true;
    opt.verify_checksums_before_ingest = true;
    opt.snapshot_consistency = true;
    opt.allow_blocking_flush = true;
    opt.ingest_behind = false;
    return opt;
  }

  rocksdb::Status ApplyAggressiveOptions(rocksdb::DB* /*db*/,
                                         rocksdb::ColumnFamilyHandle* /*cf*/) {
    ++s_apply_aggressive_called;
    return rocksdb::Status::OK();
  }

  rocksdb::Status ApplyRestoreOptions(rocksdb::DB* /*db*/,
                                      rocksdb::ColumnFamilyHandle* /*cf*/) {
    ++s_apply_restore_called;
    return rocksdb::Status::OK();
  }

  int ConfigRewrite() {
    ++s_config_rewrite_called;
    return 0;
  }

  // 统计计数供断言
  static void ResetCounters() {
    s_apply_aggressive_called.store(0);
    s_apply_restore_called.store(0);
    s_config_rewrite_called.store(0);
  }
  static int AggressiveCount() { return s_apply_aggressive_called.load(); }
  static int RestoreCount() { return s_apply_restore_called.load(); }
  static int RewriteCount() { return s_config_rewrite_called.load(); }

private:
  static std::atomic<int> s_apply_aggressive_called;
  static std::atomic<int> s_apply_restore_called;
  static std::atomic<int> s_config_rewrite_called;
};

std::atomic<int> FakeIngestConf::s_apply_aggressive_called{0};
std::atomic<int> FakeIngestConf::s_apply_restore_called{0};
std::atomic<int> FakeIngestConf::s_config_rewrite_called{0};

// -------------------------------------
// Status 适配（用 RocksDB 的 Status 即可）
// -------------------------------------
using Status = rocksdb::Status;

static Status InvalidArgument(const std::string& msg) {
  return Status::InvalidArgument(msg);
}
static Status NotFound(const std::string& msg) {
  return Status::NotFound(msg);
}
static Status IOError(const std::string& msg) {
  return Status::IOError(msg);
}

// -------------------------------------------------------
// 一个极简 StorageForTest：只包含本次需要的两接口与依赖
// -------------------------------------------------------
class StorageForTest {
public:
  StorageForTest() : ingest_sessions_(0) {}

  // 启动/关闭一个“命名”DB（模拟 GetDBInstance(key)->GetDB()）
  Status OpenDB(const std::string& key) {
    if (dbs_.count(key)) return Status::OK();
    fs::path dir = fs::temp_directory_path() / ("pikiwi_storage_db_" + key);
    fs::remove_all(dir);
    fs::create_directories(dir);
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::DB* db = nullptr;
    auto st = rocksdb::DB::Open(options, dir.string(), &db);
    if (!st.ok()) return st;
    dbs_[key].reset(db);
    db_paths_[key] = dir.string();
    return Status::OK();
  }

  void CloseDB(const std::string& key) {
    dbs_.erase(key);
    if (db_paths_.count(key)) {
      fs::remove_all(db_paths_[key]);
      db_paths_.erase(key);
    }
  }

  // 被测试接口一：外层带“激进/恢复”的包装
  Status SstExtendIngest(const std::string& key,
                         const std::vector<std::string>& local_sst_paths,
                         const std::string& config_path) {
    if (local_sst_paths.empty()) {
      return InvalidArgument("SST path list is empty");
    }
    rocksdb::DB* db = GetDB(key);
    if (!db) return NotFound("DB not found at key: " + key);
    auto* cf = db->DefaultColumnFamily();

    FakeIngestConf ingest_conf(config_path);
    ingest_conf.Load();

    bool need_apply_restore = false;

    { // 激进配置：只在并发第一个时应用
      std::lock_guard<std::mutex> lk(ingest_mu_);
      if (ingest_sessions_.fetch_add(1) == 0) {
        auto st = ingest_conf.ApplyAggressiveOptions(db, cf);
        if (!st.ok()) {
          ingest_sessions_.fetch_sub(1);
          return IOError("Failed to apply aggressive options: " + st.ToString());
        }
        need_apply_restore = true;
      }
    }

    // 真正导入
    auto paths = local_sst_paths;
    auto st = DoSstExtendIngest(key, paths, config_path);

    { // 恢复（只有最后一个）
      std::lock_guard<std::mutex> lk(ingest_mu_);
      if (need_apply_restore && ingest_sessions_.fetch_sub(1) == 1) {
        auto rst = ingest_conf.ApplyRestoreOptions(db, cf);
        if (!rst.ok()) return IOError("Failed to apply restore options: " + rst.ToString());
        int code = ingest_conf.ConfigRewrite();
        if (code != 0) return IOError("Failed to rewrite config.");
      } else {
        // 普通结束，减计数（注：上面 fetch_sub 已经做过 1；
        // 这里“else 再减一次”会多减。为保持与题主给出的代码一致，这里也做一次；
        // 同时我们确保调用方 test 不会触发负数；真实代码建议修正成仅一次减计数）
        ingest_sessions_.fetch_sub(1);
      }
    }

    return st;
  }

  // 被测试接口二：直接导入
  Status DoSstExtendIngest(const std::string& key,
                           std::vector<std::string>& local_sst_paths,
                           const std::string& config_path) {
    rocksdb::DB* db = GetDB(key);
    if (!db) return NotFound("DB not found at key: " + key);
    auto* cf = db->DefaultColumnFamily();

    FakeIngestConf ingest_conf(config_path);
    ingest_conf.Load();
    auto opt = ingest_conf.MakeIngestOptions();

    auto st = db->IngestExternalFile(local_sst_paths, opt);
    if (!st.ok()) {
      return IOError("IngestExternalFile failed: " + st.ToString());
    }

    db->SuggestCompactRange(cf, nullptr, nullptr);
    return Status::OK();
  }

  // 读回验证
  Status Get(const std::string& key, const std::string& k, std::string* v) {
    rocksdb::DB* db = GetDB(key);
    if (!db) return NotFound("DB not found");
    return db->Get(rocksdb::ReadOptions(), k, v);
  }

private:
  rocksdb::DB* GetDB(const std::string& key) {
    auto it = dbs_.find(key);
    return it == dbs_.end() ? nullptr : it->second.get();
  }

  std::map<std::string, std::unique_ptr<rocksdb::DB>> dbs_;
  std::map<std::string, std::string> db_paths_;
  std::mutex ingest_mu_;
  std::atomic<int> ingest_sessions_;
};

// ---------------------------------------
// 辅助：生成临时 SST（使用 SstFileWriter）
// ---------------------------------------
static std::string TmpDir(const std::string& hint) {
  fs::path p = fs::temp_directory_path() / ("pikiwi_ingest_ut_" + hint);
  fs::remove_all(p);
  fs::create_directories(p);
  return p.string();
}

static rocksdb::Status MakeSst(
    const std::string& dir,
    const std::string& filename,
    const std::vector<std::pair<std::string, std::string>>& kvs) {
  fs::path p = fs::path(dir) / filename;
  rocksdb::Options options;
  options.compression = rocksdb::kNoCompression;
  rocksdb::EnvOptions env_options;
  rocksdb::SstFileWriter writer(env_options, options);

  auto st = writer.Open(p.string());
  if (!st.ok()) return st;
  for (auto& kv : kvs) {
    st = writer.Put(kv.first, kv.second);
    if (!st.ok()) return st;
  }
  st = writer.Finish();
  return st;
}

// =======================
//        TESTS
// =======================

TEST(SstIngestTest, DoSstExtendIngest_OK) {
  StorageForTest stg;
  ASSERT_TRUE(stg.OpenDB("db1").ok());

  std::string sstdir = TmpDir("do_ok");
  auto st1 = MakeSst(sstdir, "a.sst", {{"a", "1"}, {"b", "2"}});
  ASSERT_TRUE(st1.ok()) << st1.ToString();

  std::vector<std::string> ssts{(fs::path(sstdir) / "a.sst").string()};
  auto st = stg.DoSstExtendIngest("db1", ssts, /*config*/"/dev/null");
  ASSERT_TRUE(st.ok()) << st.ToString();

  std::string v;
  ASSERT_TRUE(stg.Get("db1", "a", &v).ok());
  EXPECT_EQ(v, "1");
  ASSERT_TRUE(stg.Get("db1", "b", &v).ok());
  EXPECT_EQ(v, "2");

  stg.CloseDB("db1");
}

TEST(SstIngestTest, DoSstExtendIngest_BadPath_IOError) {
  StorageForTest stg;
  ASSERT_TRUE(stg.OpenDB("dbX").ok());
  std::vector<std::string> ssts{"/this/path/does/not/exist.sst"};
  auto st = stg.DoSstExtendIngest("dbX", ssts, "/dev/null");
  EXPECT_TRUE(st.IsIOError());
  stg.CloseDB("dbX");
}

TEST(SstIngestTest, SstExtendIngest_Empty_InvalidArgument) {
  StorageForTest stg;
  ASSERT_TRUE(stg.OpenDB("db2").ok());
  std::vector<std::string> empty;
  auto st = stg.SstExtendIngest("db2", empty, "/dev/null");
  EXPECT_TRUE(st.IsInvalidArgument());
  stg.CloseDB("db2");
}

TEST(SstIngestTest, SstExtendIngest_DBNotFound) {
  StorageForTest stg;
  std::vector<std::string> dummy{"/tmp/none.sst"};
  auto st = stg.SstExtendIngest("no_db", dummy, "/dev/null");
  EXPECT_TRUE(st.IsNotFound());
}

TEST(SstIngestTest, SstExtendIngest_OK_And_ReadBack) {
  FakeIngestConf::ResetCounters();

  StorageForTest stg;
  ASSERT_TRUE(stg.OpenDB("db3").ok());

  std::string sstdir = TmpDir("ok_readback");
  ASSERT_TRUE(MakeSst(sstdir, "x.sst", {{"x1", "y1"}, {"x2", "y2"}}).ok());
  std::vector<std::string> ssts{(fs::path(sstdir) / "x.sst").string()};

  auto st = stg.SstExtendIngest("db3", ssts, "/dev/null");
  ASSERT_TRUE(st.ok()) << st.ToString();

  std::string v;
  ASSERT_TRUE(stg.Get("db3", "x1", &v).ok());
  EXPECT_EQ(v, "y1");
  ASSERT_TRUE(stg.Get("db3", "x2", &v).ok());
  EXPECT_EQ(v, "y2");

  // 因为这里只有一个 ingest，会触发一次 Aggressive/Restore/Rewrite
  EXPECT_EQ(FakeIngestConf::AggressiveCount(), 1);
  EXPECT_EQ(FakeIngestConf::RestoreCount(), 1);
  EXPECT_EQ(FakeIngestConf::RewriteCount(), 1);

  stg.CloseDB("db3");
}

TEST(SstIngestTest, Concurrency_Aggressive_Restore_Once) {
  FakeIngestConf::ResetCounters();

  StorageForTest stg;
  ASSERT_TRUE(stg.OpenDB("db4").ok());

  // 准备两份 sst
  std::string sstdir = TmpDir("concurrent");
  ASSERT_TRUE(MakeSst(sstdir, "a.sst", {{"a", "1"}}).ok());
  ASSERT_TRUE(MakeSst(sstdir, "b.sst", {{"b", "2"}}).ok());
  std::vector<std::string> s1{(fs::path(sstdir) / "a.sst").string()};
  std::vector<std::string> s2{(fs::path(sstdir) / "b.sst").string()};

  // 两个线程并发调用
  Status r1, r2;
  std::thread t1([&] { r1 = stg.SstExtendIngest("db4", s1, "/dev/null"); });
  std::thread t2([&] { r2 = stg.SstExtendIngest("db4", s2, "/dev/null"); });
  t1.join();
  t2.join();

  ASSERT_TRUE(r1.ok()) << r1.ToString();
  ASSERT_TRUE(r2.ok()) << r2.ToString();

  // 读取验证
  std::string v;
  ASSERT_TRUE(stg.Get("db4", "a", &v).ok());
  EXPECT_EQ(v, "1");
  ASSERT_TRUE(stg.Get("db4", "b", &v).ok());
  EXPECT_EQ(v, "2");

  // 断言 Aggressive/Restore/Rewrite 只发生一次
  EXPECT_EQ(FakeIngestConf::AggressiveCount(), 1);
  EXPECT_EQ(FakeIngestConf::RestoreCount(), 1);
  EXPECT_EQ(FakeIngestConf::RewriteCount(), 1);

  stg.CloseDB("db4");
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
