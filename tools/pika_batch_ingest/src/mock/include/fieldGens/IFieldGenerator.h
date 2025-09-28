#ifndef IFIELDGENERATOR_H
#define IFIELDGENERATOR_H
#include <string>
#include <vector>
#include "utils/result.h"
#include <thread>
#include <random>

namespace mock
{
    /**
     * @brief 逻辑字段池，只保存前缀和池大小，无需存储实际字段内容。
     */
    struct LogicalFieldPool
    {
        std::string prefix;
        size_t size;
    };

    class IFieldGenerator
    {
    public:
        virtual ~IFieldGenerator() = default;
        IFieldGenerator() = default;

        virtual size_t getFieldPoolSize() = 0;
        virtual size_t getFieldSize() = 0;

        /**
         * @brief 子类需要实现此接口，用于生成一个 index（如随机/Zipfian 分布等）。
         */
        virtual size_t generateIndex() = 0;

        /**
         * @brief 字段生成逻辑：拼接直到刚好满足 fieldSize_，中途截断。
         */
        virtual Result generateField()
        {
            std::string full = logicalPool_.prefix; // 前缀只加一次

            while (full.size() < fieldSize_)
            {
                size_t index = generateIndex();
                std::string indexStr = std::to_string(index);

                if (full.size() + indexStr.size() >= fieldSize_)
                {
                    size_t remain = fieldSize_ - full.size();
                    full += indexStr.substr(0, remain); // 仅拼接剩余的部分
                    break;
                }

                full += indexStr;
            }

            return Result(Result::kOk, full);
        }

        /**
         * @brief 估算生成满足字段长度 fieldSize_ 所需拼接次数。
         *        假设每次拼接 prefix + index，估算平均长度。
         */
        virtual size_t estimateRepeatCount() const
        {
            size_t indexDigits = std::to_string(logicalPool_.size).size();
            size_t avgSegmentLength = logicalPool_.prefix.size() + indexDigits;
            return (fieldSize_ + avgSegmentLength - 1) / avgSegmentLength;
        }

        virtual Result setContext(const std::string &fieldPrefix, size_t poolSize, size_t fieldSize = 16)
        {
            if (fieldPrefix.empty())
            {
                return Result(Result::Ret::kError, "Field prefix cannot be empty.");
            }
            if (poolSize == 0)
            {
                return Result(Result::Ret::kError, "Pool size must be greater than 0.");
            }
            if (fieldSize == 0 || fieldSize > 1024)
            {
                return Result(Result::Ret::kError, "Field size must be in range [1, 1024].");
            }
            logicalPool_ = LogicalFieldPool{fieldPrefix, poolSize};
            fieldSize_ = fieldSize;
            return Result(Result::Ret::kOk, "Context set successfully.");
        }

    protected:
        LogicalFieldPool logicalPool_{}; // 新结构，包含前缀和 size
        size_t fieldSize_ = 16;          // 表示 key 或 value size
    };

} // namespace mock

#endif // IFIELDGENERATOR_H
