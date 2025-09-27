# pika\_batch\_ingest

一个面向Pika 数据库的高效批量导入系统。通过Mock 数据生成 → JSON 转换为 SST → 上传 S3 → 自动导入 Pika → RocksDB 存储的流水线，实现大规模数据快速入库，并支持主从同步。

## 目录

- [pika\_batch\_ingest](#pika_batch_ingest)
  - [目录](#目录)
  - [简介](#简介)
  - [系统架构](#系统架构)
  - [整体流程](#整体流程)
    - [ASCII 流程图](#ascii-流程图)
  - [快速开始](#快速开始)
  - [配置文件](#配置文件)
    - [`config.json`（run.sh 驱动配置）](#configjsonrunsh-驱动配置)
    - [`dict.json`（Mock 数据字典/生成规则）](#dictjsonmock-数据字典生成规则)
    - [`mock_threads.json`（Mock 并发）](#mock_threadsjsonmock-并发)
    - [`s3_config.json`（S3/MinIO 与 manifest 上传）](#s3_configjsons3minio-与-manifest-上传)
    - [`iagent_threads.json`（iAgent 并发）](#iagent_threadsjsoniagent-并发)
    - [`pika.json`（Pika 连接）](#pikajsonpika-连接)
    - [`sst_count.json`（SST 统计/校验）](#sst_countjsonsst-统计校验)
    - [`manifest.queue` / `manifest.offset`（内部状态）](#manifestqueue--manifestoffset内部状态)
  - [脚本说明](#脚本说明)
    - [环与公共配置](#环与公共配置)
      - [`env.sh`](#envsh)
    - [构建与清理相关](#构建与清理相关)
      - [`build.sh`](#buildsh)
      - [`pre_build.sh`](#pre_buildsh)
      - [`clear.sh`](#clearsh)
    - [工具与检查](#工具与检查)
      - [`check_cli.sh`](#check_clish)
      - [`proto.sh`](#protosh)
    - [数据流转](#数据流转)
      - [`mock.sh`](#mocksh)
      - [`exchange.sh`](#exchangesh)
      - [`s3put.sh`](#s3putsh)
    - [服务与集成](#服务与集成)
      - [`iagent.sh`](#iagentsh)
      - [`pika.sh`](#pikash)
      - [`tune_l0.sh`](#tune_l0sh)
      - [`run.sh`](#runsh)
    - [测试](#测试)
      - [`test.sh`](#testsh)
  - [应用场景](#应用场景)
  - [依赖](#依赖)
  - [总结](#总结)

---

## 简介

`pika_batch_ingest` 通过模块化流水线将数据快速导入 Pika：

* **Mock**：生成大规模 KV 数据
* **Exchange**：转为 RocksDB SST 文件
* **S3Put**：自动上传 S3 + 管理 manifest
* **iAgent**：检测新 manifest，推送 ingest 命令
* **Pika**：主从节点 ingest，同步数据一致性

---

## 系统架构

```
Mock(生成数据) → Exchange(转SST) → S3Put(上传S3)
                                    │
                                    ▼
                              iAgent(检测清单，通知Pika)
                                    │
                                    ▼
                            Pika集群(主从同步导入)
                                    │
                                    ▼
                                RocksDB
```

---

## 整体流程

```bash
1. 清理环境       ./shell/clear.sh
2. 构建依赖       ./shell/pre_build.sh
3. 编译 proto     ./shell/proto.sh
4. 生成数据       ./shell/mock.sh -n 10G -d kvdict
5. 转换为 SST     ./shell/exchange.sh -d kvdict
6. 上传 S3        ./shell/s3put.sh
7. 启动 Pika集群  ./shell/pika.sh
8. 校验导入       ./shell/check_cli.sh
9. 运行测试       ./shell/test.sh
```

### ASCII 流程图

```
 ┌───────────┐
 │ clear.sh  │ 清理环境
 └─────┬─────┘
       │
 ┌─────▼─────┐
 │ pre_build │ 构建依赖
 └─────┬─────┘
       │
 ┌─────▼─────┐
 │ proto.sh  │ 编译proto
 └─────┬─────┘
       │
 ┌─────▼─────┐
 │ mock.sh   │ 生成数据
 └─────┬─────┘
       │
 ┌─────▼─────┐
 │ exchange  │ 转SST
 └─────┬─────┘
       │
 ┌─────▼─────┐
 │ s3put.sh  │ 上传S3
 └─────┬─────┘
       │
 ┌─────▼─────┐
 │ pika.sh   │ 启动Pika+导入
 └─────┬─────┘
       │
 ┌─────▼─────┐
 │ check_cli │ 校验结果
 └─────┬─────┘
       │
 ┌─────▼─────┐
 │ test.sh   │ 单元测试
 └───────────┘
```

---

## 快速开始

一键跑全流程：

```bash
./shell/run.sh kvdict
```

> **重要：`run.sh` 基于 `config.json` 进行数据生成与流程编排**（例如目标规模、输出目录、引用的 `dict.json` 等）。运行前请先正确配置 **`config.json`**（见下文“配置文件”章节）。

---

## 配置文件

> 命名修正：请统一使用 **`dict.json`**（而非 `dics.json`）。
> 配置文件建议放在 `config/` 目录，并通过脚本读取；密钥类信息请使用环境变量或本地未入库文件管理。

### `config.json`（run.sh 驱动配置）

* **用途**：`run.sh` 会读取该文件来决定生成多大规模的数据、输出到哪里、以及采用哪个字典（`dict.json`）。随后按此配置依次调用 `mock.sh`、`exchange.sh`、`s3put.sh`、`pika.sh` 等。
* **示例**：

```json
{
  "size": "10G",
  "output_dir": "kvdict",
  "dict_file": "config/dict.json",
  "enable_s3_upload": true,
  "s3_config": "config/s3_config.json"
}
```

* **字段说明**：

  * `size`：目标数据规模，支持 `M`/`G`（等价于 `mock.sh -n`）。
  * `output_dir`：生成数据的目录（等价于 `mock.sh -d`；也是 `exchange.sh -d` 的入参）。
  * `dict_file`：Mock 生成规则文件路径（见下节 `dict.json`）。
  * `enable_s3_upload`：是否在生成与转换后执行上传。
  * `s3_config`：`s3put` 使用的 S3 配置文件路径（见下节 `s3_config.json`）。

> **运行提示**：当你执行 `./shell/run.sh kvdict` 时，`kvdict` 仅作为覆盖/兜底的目录名；最终以 `config.json` 为准（脚本优先读配置，其次读命令行）。

### `dict.json`（Mock 数据字典/生成规则）

* **用途**：定义键值生成的分布、前缀、长度，以及每个 JSON 文件的最大体积、总体目标规模和输出目录等。
* **示例**：

```json
{
  "key": {
    "distribution": "normal",
    "poolSize": -1,
    "prefix": "key_",
    "size": 16
  },
  "value": {
    "distribution": "normal",
    "poolSize": -1,
    "prefix": "value_",
    "size": 24
  },
  "maxFileSizeMB": 50,
  "targetSizeMB": 1024,
  "maxSizeGB": 10,
  "directory": "test-1G"
}
```

* **要点**：

  * `key` / `value`：分别定义键和值的**前缀**、**长度**、**分布**（如 `normal`）。
  * `maxFileSizeMB`：单个 JSON 文件的最大体积，超出将拆分。
  * `targetSizeMB`：单批目标体积；结合 `maxSizeGB` 决定总产出。
  * `directory`：输出目录名（若与 `config.json.output_dir` 冲突，以 `config.json` 为准）。

### `mock_threads.json`（Mock 并发）

* **用途**：控制数据生成时的线程数。
* **示例**：

```json
{ "dataGen": 4 }
```

* **建议**：根据机器 CPU/磁盘性能调大可提升 Mock 吞吐，注意与 `maxFileSizeMB` 配合避免过多小文件。

### `s3_config.json`（S3/MinIO 与 manifest 上传）

* **用途**：提供 `s3put` 模块的 S3/MinIO 访问配置，同时控制 **manifest 构建与上传** 行为。
* **示例（请勿明文提交真实密钥，以下仅为字段演示）**：

```json
{
  "endpoint": "https://s3.amazonaws.com",
  "region": "ap-northeast-3",
  "bucket": "pika-sst",
  "access_key": "YOUR_ACCESS_KEY_ID",
  "secret_key": "YOUR_SECRET_ACCESS_KEY",
  "is_minio": false,

  "dict": "sst/test-100M",
  "files_per_manifest": 5,
  "manifest_dir": "manifest",
  "latest_manifest_path": "last.manifest",

  "watch_interval_sec": 5,
  "upload_concurrency": 3,
  "hash_verify_on_unchanged": true,
  "tracker_state_path": "tracker_state.json"
}
```

* **字段说明**：

  * **存储接入**：

    * `endpoint`：S3/MinIO 访问地址；MinIO 通常为 `http(s)://<host>:<port>`
    * `region`：S3 区域（MinIO 可忽略或自定义）
    * `bucket`：目标桶名
    * `access_key` / `secret_key`：访问凭证（**强烈建议改为环境变量或本地未入库文件**）
    * `is_minio`：是否启用 MinIO 兼容模式
  * **清单与数据源**：

    * `dict`：**SST 文件所在目录**（`s3put` 将从这里扫描要上传的 `.sst`）
    * `files_per_manifest`：每个 manifest 包含的 SST 文件数量
    * `manifest_dir`：生成 manifest 的本地目录（再由 `s3put` 上传）
    * `latest_manifest_path`：记录“最新”清单文件名（供下游 iAgent/Pika 拉取）
  * **性能与可靠性**：

    * `upload_concurrency`：并发上传数
    * `watch_interval_sec`：轮询本地新 SST 的间隔秒数
    * `hash_verify_on_unchanged`：内容未变更时是否仍做哈希校验
    * `tracker_state_path`：本地断点/状态跟踪文件（防重复上传）
* **安全建议**：

  * 不要把 `access_key`/`secret_key` 明文提交至仓库。可考虑：

    * 使用环境变量（例如 `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`）
    * 使用云厂商实例角色或本地凭证文件
  * 对 MinIO：将 `is_minio=true` 并设置正确的 `endpoint`。

### `iagent_threads.json`（iAgent 并发）

* **用途**：控制 iAgent 在拉取/去重/推送 manifest 任务时的并发度。
* **示例**：

```json
{ "pipe_conns": 8 }
```

### `pika.json`（Pika 连接）

* **用途**：iAgent / 校验工具等与 Pika 的连接信息。
* **示例**：

```json
{ "host": "127.0.0.1", "port": 9221 }
```

### `sst_count.json`（SST 统计/校验）

* **用途**：记录 SST 生成/导入后的统计信息，便于与 binlog/rocksdb 进行核对。
* **示例**：

```json
{
  "status": "ok",
  "total_encode_bytes": 1049979546,
  "total_keys": 10605854,
  "total_raw_bytes": 424234160
}
```

### `manifest.queue` / `manifest.offset`（内部状态）

* **用途**：iAgent 的本地持久化队列与偏移量，用于**断点续传与去重**。
* **说明**：这两者由 iAgent 自动维护，一般无需手动修改。

---

## 脚本说明

### 环与公共配置

#### `env.sh`

* 定义项目路径、构建目录、配置文件路径
* 提供快捷跳转函数 `cd_build_bin` / `cd_proto`

---

### 构建与清理相关

#### `build.sh`

* 构建主项目和依赖
* 用法：

```bash
./shell/build.sh
JOBS=16 ./shell/build.sh
```

#### `pre_build.sh`

* 一键构建第三方依赖（openssl/curl/aws/rocksdb 等）
* 用法：

```bash
./shell/pre_build.sh
./shell/pre_build.sh openssl curl
```

#### `clear.sh`

* 清理数据、日志、配置
* 用法：

```bash
./shell/clear.sh
./shell/clear.sh -d kvdict
```

---

### 工具与检查

#### `check_cli.sh`

* 校验数据导入结果（队列缺失、RocksDB 状态、随机抽样）
* 用法：

```bash
./shell/check_cli.sh [日志路径]
```

#### `proto.sh`

* 编译 `.proto` 文件为 C++ 代码

---

### 数据流转

#### `mock.sh`

* 生成 KV 模拟数据（支持大小单位 M/G）
* 用法：

```bash
./shell/mock.sh -n 10G -d kvdict
```

#### `exchange.sh`

* JSON → SST 文件转换
* 单文件：

```bash
./shell/exchange.sh -k kv.json -s out.sst
```

* 批量目录：

```bash
./shell/exchange.sh -d kvdict
```

#### `s3put.sh`

* 上传 SST + manifest 到 S3
* 从 `s3_config.json` 读取上传配置（桶、并发、manifest 策略等）
* 实时检测新文件并维护 `latest_manifest_path`

---

### 服务与集成

#### `iagent.sh`

* 周期性检测 S3 新 manifest，去重并将导入任务推送到 Pika（`manifestingest`）

#### `pika.sh`

* 启动 Pika 主从节点并执行 ingest 流程

#### `tune_l0.sh`

* 自动调整 RocksDB L0 参数
* 用法：

```bash
./shell/tune_l0.sh
```

#### `run.sh`

* 串联执行 **清理 → mock → exchange → s3put → pika → 校验**
* **严格依赖 `config.json`**：按其中的 `size`、`output_dir`、`dict_file`、`s3_config` 等项驱动各阶段
* 用法：

```bash
./shell/run.sh kvdict
```

---

### 测试

#### `test.sh`

* 执行 gtest 单元测试
* 用法：

```bash
./shell/test.sh
./shell/test.sh SomeTest.*
```

---

## 应用场景

* 大规模离线导入
* 冷热数据切换
* 跨集群同步
* 基准压测

---

## 依赖

* bash ≥ 4.0, cmake, make, g++/clang++
* jq, protoc
* 第三方：RocksDB, AWS SDK, hiredis, openssl, curl（可用脚本自动构建）

---

## 性能测试

### 100G
```log
Log Message
[@MOCK][TIME] Starting  at Fri Sep 26 13:32:51 2025
[@MOCK][TIME] completed in 17849846 ms.
[@EXCHANGE][TIME] Starting  at Fri Sep 26 18:30:21 2025
[@EXCHANGE][TIME] completed in 5184865 ms.
[@S3PUT][TIME] Starting  at Fri Sep 26 19:56:46 2025
[@S3PUT][TIME] completed in 131844 ms.
[@IAGENT][TIME] Starting  at Fri Sep 26 19:59:05 2025
[@IAGENT][TIME] completed in 166 ms.

==========================================================================
 Ingest 校验摘要 (port 9221, db-path /data/ospp/pikiwidb/db/master)
==========================================================================
状态   : SUCCESS (1)
命令   : ManifestIngestCmd
开始   : 2025-09-27 09:36:44.572
结束   : 2025-09-27 09:37:43.704
耗时   : 59132 ms (59.132 s)
备注   : [ManifestIngestCmd] Do (SST Ingest) completed, key=manifest_1758916683716449000_part406.proto
队列   : 总 410 | 已处理 410 | 缺失 0 → OK
抽样   : 成功 5 | 缺失 0 | 错误 0 / 总 5
RocksDB: SST 38506101394 bytes
OK Keys:
  - key_370103300725
  - key_001000001000
  - key_001022110100
  - key_226203885021
  - key_482802936941
==========================================================================
```

---

## 总结

`pika_batch_ingest` 通过 **模块化流水线 + 脚本化工具 + 可配置化参数**，让用户可以 **配置 → 一键 run → 自动上传 → 自动 ingest → 校验**，并保证 Pika 主从一致性，开箱即用、高效稳定。

