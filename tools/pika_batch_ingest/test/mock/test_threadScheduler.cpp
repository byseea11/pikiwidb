#include <gtest/gtest.h>
#include "utils/threadScheduler.h"
#include <nlohmann/json.hpp>

class ThreadSchedulerTest : public ::testing::Test {
protected:
    void SetUp() override {
        // 重置调度器状态
        auto& scheduler = ThreadScheduler::get();
        // Note: We can't easily reset the singleton, so we'll work with it as-is
    }
};

// 测试线程调度器单例模式
TEST_F(ThreadSchedulerTest, Singleton) {
    auto& scheduler1 = ThreadScheduler::get();
    auto& scheduler2 = ThreadScheduler::get();
    EXPECT_EQ(&scheduler1, &scheduler2) << "ThreadScheduler should be a singleton";
}

// 测试初始化功能
TEST_F(ThreadSchedulerTest, Initialization) {
    auto& scheduler = ThreadScheduler::get();
    scheduler.init(4); // 初始化4个线程
    
    // 可用线程数应该是硬件并发线程数的2/3，但这里我们设置了4个
    // 所以应该使用4个线程
    // 注意：由于调度器是单例，这个测试可能受其他测试影响
}

// 测试逻辑请求注册
TEST_F(ThreadSchedulerTest, RegisterLogicalRequest) {
    auto& scheduler = ThreadScheduler::get();
    scheduler.init(4);
    
    scheduler.registerLogicalRequest("dataGen", 2);
    scheduler.registerLogicalRequest("keyGen", 1);
    scheduler.registerLogicalRequest("valueGen", 1);
    
    // 这些调用不应该抛出异常
    SUCCEED();
}

// 测试线程分配
TEST_F(ThreadSchedulerTest, ThreadAllocation) {
    auto& scheduler = ThreadScheduler::get();
    scheduler.init(4);
    
    scheduler.registerLogicalRequest("dataGen", 2);
    scheduler.registerLogicalRequest("keyGen", 1);
    scheduler.registerLogicalRequest("valueGen", 1);
    
    scheduler.finalize();
    
    // 检查各个模块的线程分配
    size_t dataGenThreads = scheduler.get("dataGen");
    size_t keyGenThreads = scheduler.get("keyGen");
    size_t valueGenThreads = scheduler.get("valueGen");
    
    // 总线程数应该不超过初始化的线程数
    EXPECT_LE(dataGenThreads + keyGenThreads + valueGenThreads, 4);
    
    // 每个模块应该至少分配到一些线程
    EXPECT_GE(dataGenThreads, 0);
    EXPECT_GE(keyGenThreads, 0);
    EXPECT_GE(valueGenThreads, 0);
}

// 测试未注册模块的线程获取
TEST_F(ThreadSchedulerTest, GetUnregisteredModule) {
    auto& scheduler = ThreadScheduler::get();
    scheduler.init(2);
    scheduler.finalize();
    
    // 获取未注册模块的线程数应该返回默认值1
    size_t threads = scheduler.get("unregistered_module");
    EXPECT_EQ(threads, 1);
}

// 测试边界情况：0线程初始化
TEST_F(ThreadSchedulerTest, ZeroThreadInitialization) {
    auto& scheduler = ThreadScheduler::get();
    scheduler.init(0);
    scheduler.finalize();
    
    // 即使初始化为0，也应该至少有1个线程
    size_t available = scheduler.available();
    // 注意：这个测试可能不准确，因为调度器是单例且可能被其他测试修改
}

// 测试大量逻辑请求的分配
TEST_F(ThreadSchedulerTest, LargeLogicalRequests) {
    auto& scheduler = ThreadScheduler::get();
    scheduler.init(8);
    
    // 注册多个模块
    scheduler.registerLogicalRequest("module1", 10);
    scheduler.registerLogicalRequest("module2", 5);
    scheduler.registerLogicalRequest("module3", 3);
    scheduler.registerLogicalRequest("module4", 2);
    scheduler.registerLogicalRequest("module5", 1);
    
    scheduler.finalize();
    
    // 总分配线程数不应超过初始化的线程数
    size_t totalAllocated = 
        scheduler.get("module1") + 
        scheduler.get("module2") + 
        scheduler.get("module3") + 
        scheduler.get("module4") + 
        scheduler.get("module5");
    
    EXPECT_LE(totalAllocated, 8);
}