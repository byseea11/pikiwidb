#ifndef MOCK_H
#define MOCK_H

#include <nlohmann/json.hpp>
#include <fstream>
#include <filesystem>
#include <array>
#include "utils/klog.h"
#include "utils/kconfig.h"

namespace mock
{

    using json = nlohmann::json;

    json LoadFolderHistory(const std::filesystem::path &path = DEFAULTCONFIGFILEDIC)
    {
        json history;

        LOG_DEBUG("Loading folder history from: " + path.string());

        // 确保目录存在（即使文件不存在，也先建目录）
        std::filesystem::create_directories(path.parent_path());

        std::ifstream ifs(path);
        if (!ifs.is_open())
        {
            // 文件不存在，返回空历史
            history["folders"] = json::array();
            LOG_DEBUG("Folder history json not found, returning empty history: " + path.string());
            return history;
        }
        try
        {
            ifs >> history;
        }
        catch (...)
        {
            LOG_ERROR("Failed to parse folder history json: " + path.string());
            history["folders"] = json::array();
        }
        return history;
    }

    bool CheckFolderNameUnique(const json &history, const std::string &folder_name)
    {
        for (const auto &item : history["folders"])
        {
            if (item == folder_name)
            {
                return false;
            }
        }
        return true;
    }

    bool SaveFolderHistory(json &history, const std::string &folder_name, const std::filesystem::path &path = DEFAULTCONFIGFILEDIC)
    {
        if (!history.contains("folders"))
        {
            history["folders"] = json::array(); // Initialize "folders" if it doesn't exist
        }
        history["folders"].push_back(folder_name);

        std::ofstream ofs(path);
        if (!ofs.is_open())
        {
            LOG_ERROR("Failed to open folder history json for writing: " + path.string());
            return false;
        }
        ofs << history.dump(4);
        return true;
    }
} // namespace mock

#endif // MOCK_H
