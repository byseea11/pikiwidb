#include <chrono>
#include <iostream>
#include <fstream>
#include <string>
#include "utils/kconfig.h"

namespace fs = std::filesystem;

// 用于记录时间并写入 CSV 文件
class TimeTracker
{
public:
    // 启动时间记录
    static void Start(const std::string &actionName)
    {
        action_ = actionName;
        start_time_ = std::chrono::steady_clock::now();
        std::string log_message = action_ + "[TIME] Starting " + " at " + GetCurrentTime();
        std::cout << log_message << std::endl;
        WriteToCSV(log_message); // 写入 CSV
    }

    // 结束时间记录并将结果写入 CSV 文件
    static void End()
    {
        auto end_time = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time_);
        std::string log_message = action_ + "[TIME] completed in " + std::to_string(duration.count()) + " ms.";
        std::cout << log_message << std::endl;
        WriteToCSV(log_message); // 写入 CSV
    }

private:
    static std::chrono::steady_clock::time_point start_time_;
    static std::string action_;

    // 获取当前时间的字符串格式
    static std::string GetCurrentTime()
    {
        auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        std::string time_str = std::ctime(&now);
        time_str.pop_back(); // 去掉末尾的换行符
        return time_str;
    }

    // 写入 CSV 文件
    static void WriteToCSV(const std::string &message)
    {
        // 获取文件夹路径
        fs::path parentDir = fs::path(TIEMRECORDPATH).parent_path();
        if (!parentDir.empty() && !fs::exists(parentDir))
        {
            fs::create_directories(parentDir);
        }

        std::ofstream csv_file(TIEMRECORDPATH, std::ios::app); // 以追加模式打开文件
        if (csv_file.is_open())
        {
            // 如果是第一次写入，写入 CSV 表头
            if (csv_file.tellp() == 0)
            {
                csv_file << "Log Message\n"; // 写入表头
            }

            // 写入记录
            csv_file << message << "\n";
            csv_file.close();
        }
        else
        {
            std::cerr << "Failed to open CSV file for writing." << std::endl;
        }
    }
};

// 静态成员变量初始化
std::chrono::steady_clock::time_point TimeTracker::start_time_;
std::string TimeTracker::action_;
