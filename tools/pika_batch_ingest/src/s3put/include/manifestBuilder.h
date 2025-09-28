#ifndef S3PUT_MANIFEST_BUILDER_H
#define S3PUT_MANIFEST_BUILDER_H

#include <string>
#include <vector>
#include <future>
#include <cstdint>
#include "sstTracker.h"
#include "proto/manifest.pb.h" // Manifest / SSTFile
#include <nlohmann/json.hpp>   // 写 latest.manifest (JSON)
#ifndef _WIN32
#include <sys/file.h>
#include <fcntl.h>
#include <unistd.h>
#endif
#include <filesystem>

namespace s3put
{
    namespace fs = std::filesystem;
    struct DirLock
    {
        int fd{-1};
        std::string path;
        bool ok{false};
        explicit DirLock(const std::string &manifest_dir)
        {
            path = (fs::path(manifest_dir) / ".build.lock").string();
#ifndef _WIN32
            fd = ::open(path.c_str(), O_CREAT | O_RDWR, 0666);
            if (fd >= 0)
            {
                if (::flock(fd, LOCK_EX | LOCK_NB) == 0)
                    ok = true;
            }
#else
            // Windows
            ok = true; 
#endif
        }
        ~DirLock()
        {
#ifndef _WIN32
            if (fd >= 0)
            {
                ::flock(fd, LOCK_UN);
                ::close(fd);
            }
#endif
        }
    };

    class ManifestBuilder
    {
    public:
        ManifestBuilder() = default;

        // 构建并写入多个分片 manifest 文件，并最终写 latest.manifest
        // 所有“可变因素”均由调用方传入：并发度/输出目录/最新清单路径/单分片最大条目数/统一 version_id
        //
        // - tracker:        已设置好 sst_root / key_prefix / version_id 的 SstTracker
        // - num_threads:    并行线程数（>=1）
        // - manifest_dir:   分片 manifest 落盘目录（如 "/path/to/manifest"）
        // - latest_path:    latest.manifest 的完整输出路径（如 "/path/to/manifest/last.manifest"）
        // - max_per_part:   每个分片最多包含的 SSTFile 条目数
        // - version_id:     本轮统一版本号（与 tracker.SetCurrentVersionId 一致）
        // - out_parts:      可选输出：返回生成的所有分片文件名（相对于 manifest_dir 或绝对路径，由实现决定）
        //
        // 返回 Manifest 表示成功与否
        bool BuildAndWrite(const SstTracker &tracker,
                           size_t num_threads,
                           const std::string &manifest_dir,
                           const std::string &latest_path,
                           size_t max_per_part,
                           const std::string &version_id,
                           std::vector<std::string> *out_parts = nullptr);

        // 工具函数：生成 version_id（通常为毫秒级时间戳字符串）
        static std::string GenerateVersionId();

        // 写 latest.manifest（JSON），内容包含：version_id / timestamp_ms / parts[]
        static bool WriteLatestManifest(const std::string &path,
                                        const std::string &version_id,
                                        int64_t timestamp_ms,
                                        const std::vector<std::string> &manifest_files);

    private:
        // 写单个分片 manifest（使用统一 version_id，不在函数内再生成新版本）
        static bool WriteManifestPart(const std::vector<s3put::manifest::SSTFile> &files,
                                      const std::string &manifest_file,
                                      const std::string &version_id);
    };

} // namespace s3put

#endif // S3PUT_MANIFEST_BUILDER_H
