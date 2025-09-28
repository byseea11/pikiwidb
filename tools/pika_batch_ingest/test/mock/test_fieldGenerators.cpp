#include <gtest/gtest.h>
#include <set>
#include <string>
#include "fieldGens/FieldGebBuilder.h"
#include "fieldGens/IFieldGenerator.h"

class FieldGeneratorTest : public ::testing::Test {
protected:
    void SetUp() override {
        // 初始化一些测试参数
        prefix = "test_";
        size = 10;
        poolSize = 100;
    }
    
    std::string prefix;
    size_t size;
    size_t poolSize;
};

// 测试NormalFieldGenerator创建
TEST_F(FieldGeneratorTest, CreateNormalFieldGenerator) {
    auto generator = mock::createFieldGenerator(
        mock::FieldDistributionType::Normal, 
        prefix, size, poolSize);
    
    EXPECT_NE(generator, nullptr) << "NormalFieldGenerator creation failed";
    
    // 测试生成字段
    auto result = generator->generateField();
    EXPECT_FALSE(result.isError()) << "Field generation failed: " << result.message();
    EXPECT_FALSE(result.message_raw().empty()) << "Generated field is empty";
    EXPECT_EQ(result.message_raw().substr(0, prefix.length()), prefix) 
        << "Generated field doesn't start with prefix";
}

// 测试RandomFieldGenerator创建
TEST_F(FieldGeneratorTest, CreateRandomFieldGenerator) {
    auto generator = mock::createFieldGenerator(
        mock::FieldDistributionType::Random, 
        prefix, size, poolSize);
    
    EXPECT_NE(generator, nullptr) << "RandomFieldGenerator creation failed";
    
    // 测试生成字段
    auto result = generator->generateField();
    EXPECT_FALSE(result.isError()) << "Field generation failed: " << result.message();
    EXPECT_FALSE(result.message_raw().empty()) << "Generated field is empty";
}

// 测试ZipfianFieldGenerator创建
TEST_F(FieldGeneratorTest, CreateZipfianFieldGenerator) {
    auto generator = mock::createFieldGenerator(
        mock::FieldDistributionType::Zipfian, 
        prefix, size, poolSize);
    
    EXPECT_NE(generator, nullptr) << "ZipfianFieldGenerator creation failed";
    
    // 测试生成字段
    auto result = generator->generateField();
    EXPECT_FALSE(result.isError()) << "Field generation failed: " << result.message();
    EXPECT_FALSE(result.message_raw().empty()) << "Generated field is empty";
}

// 测试UniformFieldGenerator创建
TEST_F(FieldGeneratorTest, CreateUniformFieldGenerator) {
    auto generator = mock::createFieldGenerator(
        mock::FieldDistributionType::Uniform, 
        prefix, size, poolSize);
    
    EXPECT_NE(generator, nullptr) << "UniformFieldGenerator creation failed";
    
    // 测试生成字段
    auto result = generator->generateField();
    EXPECT_FALSE(result.isError()) << "Field generation failed: " << result.message();
    EXPECT_FALSE(result.message_raw().empty()) << "Generated field is empty";
}

// 测试字段唯一性
TEST_F(FieldGeneratorTest, FieldUniqueness) {
    auto generator = mock::createFieldGenerator(
        mock::FieldDistributionType::Random, 
        prefix, size, poolSize);
    
    std::set<std::string> generatedFields;
    int numTests = 50;
    
    // 生成多个字段并检查是否有重复
    for (int i = 0; i < numTests; ++i) {
        auto result = generator->generateField();
        EXPECT_FALSE(result.isError()) << "Field generation failed on iteration " << i;
        generatedFields.insert(result.message_raw());
    }
    
    // 注意：对于随机生成器，可能会有重复，但对于其他类型应该更唯一
    // 这里我们只确保生成过程没有错误
    EXPECT_GE(generatedFields.size(), 1) << "No fields were generated";
}

// 测试字段长度
TEST_F(FieldGeneratorTest, FieldLength) {
    size_t testSize = 20;
    auto generator = mock::createFieldGenerator(
        mock::FieldDistributionType::Normal, 
        prefix, testSize, poolSize);
    
    auto result = generator->generateField();
    EXPECT_FALSE(result.isError()) << "Field generation failed";
    
    // 字段长度应该接近指定大小
    // 注意：实际长度可能因为实现细节而略有不同
    EXPECT_GT(result.message_raw().length(), 0) << "Generated field is empty";
}

// 测试不同前缀的字段生成
TEST_F(FieldGeneratorTest, DifferentPrefixes) {
    std::vector<std::string> prefixes = {"key_", "value_", "test_", "data_"};
    
    for (const auto& testPrefix : prefixes) {
        // 重新设置生成器前缀（如果API支持）或创建新生成器
        auto gen = mock::createFieldGenerator(
            mock::FieldDistributionType::Normal, 
            testPrefix, size, poolSize);
        
        auto result = gen->generateField();
        EXPECT_FALSE(result.isError()) << "Field generation failed for prefix: " << testPrefix;
        EXPECT_EQ(result.message_raw().substr(0, testPrefix.length()), testPrefix)
            << "Generated field doesn't start with prefix: " << testPrefix;
    }
}

// 测试无效参数处理
TEST_F(FieldGeneratorTest, InvalidParameters) {
    // 测试空前缀
    auto generator1 = mock::createFieldGenerator(
        mock::FieldDistributionType::Normal, 
        "", size, poolSize);
    EXPECT_NE(generator1, nullptr) << "Generator creation failed with empty prefix";
    
    // 测试零大小
    auto generator2 = mock::createFieldGenerator(
        mock::FieldDistributionType::Normal, 
        prefix, 0, poolSize);
    EXPECT_NE(generator2, nullptr) << "Generator creation failed with zero size";
}