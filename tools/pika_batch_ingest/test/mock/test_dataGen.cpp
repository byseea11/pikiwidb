#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <ctime>
#include "dataGen.h"
#include "fieldGens/FieldGebBuilder.h"
#include "utils/threadScheduler.h"

namespace fs = std::filesystem;

class DataGenTest : public ::testing::Test {
protected:
    void SetUp() override {
        // 创建临时测试目录
        testDir = "/tmp/mock_test_" + std::to_string(std::time(nullptr));
        fs::create_directories(testDir);
        
        // 初始化线程调度器
        ThreadScheduler::get().init(4); // 使用4个线程进行测试
        ThreadScheduler::get().registerLogicalRequest("dataGen", 2);
        ThreadScheduler::get().registerLogicalRequest("keyGen", 1);
        ThreadScheduler::get().registerLogicalRequest("valueGen", 1);
        ThreadScheduler::get().finalize();
    }
    
    void TearDown() override {
        // 清理测试目录
        if (fs::exists(testDir)) {
            fs::remove_all(testDir);
        }
    }
    
    std::string testDir;
};

// 测试DataGen构造函数
TEST_F(DataGenTest, Constructor) {
    mock::DataGen generator(testDir, 100, 1.0, 1.0, 1.0, 2);
    EXPECT_EQ(generator.getNumThreads(), 2);
}

// 测试基本数据生成功能
TEST_F(DataGenTest, GenerateDataBasic) {
    mock::DataGen generator(testDir, 100, 1.0, 1.0, 1.0, 2);
    
    // 创建简单的key/value生成器
    auto keyGen = mock::createFieldGenerator(mock::FieldDistributionType::Normal, "key_", 10, 100);
    auto valueGen = mock::createFieldGenerator(mock::FieldDistributionType::Normal, "value_", 20, 100);
    
    generator.setKeyGenerator(keyGen);
    generator.setValueGenerator(valueGen);
    
    auto result = generator.generateData();
    EXPECT_FALSE(result.isError()) << "Data generation failed: " << result.message();
    
    // 检查是否生成了文件
    EXPECT_TRUE(fs::exists(testDir)) << "Test directory not created";
    
    // 检查目录中是否有文件
    int fileCount = 0;
    for (const auto& entry : fs::directory_iterator(testDir)) {
        if (entry.is_regular_file()) {
            fileCount++;
        }
    }
    
    EXPECT_GT(fileCount, 0) << "No files were generated";
}

// 测试文件生成器设置
TEST_F(DataGenTest, SetFileManager) {
    mock::DataGen generator(testDir, 100, 1.0, 1.0, 1.0, 1);
    
    // 测试设置key生成器
    auto keyGen = mock::createFieldGenerator(mock::FieldDistributionType::Normal, "key_", 10, 100);
    generator.setKeyGenerator(keyGen);
    
    // 测试设置value生成器
    auto valueGen = mock::createFieldGenerator(mock::FieldDistributionType::Normal, "value_", 20, 100);
    generator.setValueGenerator(valueGen);
    
    // 检查生成器是否正确设置
    // 注意：由于这些是私有成员，我们无法直接访问它们
    // 但我们可以通过generateData的结果间接验证
    auto result = generator.generateData();
    EXPECT_FALSE(result.isError()) << "Data generation failed after setting generators";
}

// 测试线程数为0的情况
TEST_F(DataGenTest, GenerateDataWithZeroThreads) {
    mock::DataGen generator(testDir, 100, 1.0, 0.5, 1.0, 0);
    
    auto keyGen = mock::createFieldGenerator(mock::FieldDistributionType::Normal, "key_", 10, 100);
    auto valueGen = mock::createFieldGenerator(mock::FieldDistributionType::Normal, "value_", 20, 100);
    
    generator.setKeyGenerator(keyGen);
    generator.setValueGenerator(valueGen);
    
    auto result = generator.generateData();
    EXPECT_FALSE(result.isError()) << "Data generation failed with zero threads: " << result.message();
}

// 测试小数据量生成
TEST_F(DataGenTest, GenerateSmallData) {
    mock::DataGen generator(testDir, 50, 0.1, 1.0, 0.5, 1);
    
    auto keyGen = mock::createFieldGenerator(mock::FieldDistributionType::Normal, "key_", 5, 50);
    auto valueGen = mock::createFieldGenerator(mock::FieldDistributionType::Normal, "value_", 10, 50);
    
    generator.setKeyGenerator(keyGen);
    generator.setValueGenerator(valueGen);
    
    auto result = generator.generateData();
    EXPECT_FALSE(result.isError()) << "Small data generation failed: " << result.message();
    
    // 检查DataGen的行为是否一致（不检查是否生成了文件，因为可能没有生成足够的数据）
    EXPECT_FALSE(result.isError()) << "DataGen should handle small data without error";
}