#!/usr/bin/env bash
set -euo pipefail

# ==========================
# Orchestrator
# ==========================
# 功能：
#  - 一键/分步编译 third 目录下的依赖：openssl、curl、aws-crt-cpp、aws-sdk-cpp、rocksdb、hiredis
#  - 生成 protobuf
#
# 使用：
#  1) 默认全量构建：
#       ./shell/pre_build.sh
#  2) 指定步骤（可多选）：
#       ./shell/pre_build.sh openssl curl aws-crt-cpp aws-sdk-cpp rocksdb hiredis proto
#  3) 可选参数：
#       INSTALL_ROOT=<绝对或相对路径>   # 依赖安装根目录，默认：$PROJECT_ROOT/third
#       JOBS=<并行数>                   # 默认自动检测
#
# 示例：
#   INSTALL_ROOT=/data/ospp/pika_batch_ingest/third JOBS=16 ./shell/pre_build.sh curl aws-sdk-cpp
#
# 说明：
#  - 尽量使用相对 PROJECT_ROOT 的路径，避免硬编码绝对路径
#  - 已做幂等处理：存在则 update，不存在则 add/init
#  - macOS 和 Linux 均可；RocksDB 会根据系统使用不同编译选项
#
# ==========================

# ---------- 基本环境 ----------
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

# 可通过环境变量覆盖
INSTALL_ROOT="${INSTALL_ROOT:-$PROJECT_ROOT/third}"
JOBS="${JOBS:-}"

# 自动检测并行编译核数
detect_jobs() {
  if [[ -n "${JOBS}" ]]; then
    echo "${JOBS}"
  else
    if command -v nproc >/dev/null 2>&1; then
      nproc
    elif [[ "$OSTYPE" == "darwin"* ]]; then
      sysctl -n hw.logicalcpu
    else
      echo 4
    fi
  fi
}
JOBS="$(detect_jobs)"

# 平台判断
OS="linux"
if [[ "$OSTYPE" == "darwin"* ]]; then
  OS="macos"
fi

# 颜色输出
c_info(){ echo -e "\033[1;34m[INFO]\033[0m $*"; }
c_ok(){ echo -e "\033[1;32m[OK]\033[0m $*"; }
c_warn(){ echo -e "\033[1;33m[WARN]\033[0m $*"; }
c_err(){ echo -e "\033[1;31m[ERR]\033[0m $*"; }

# 依赖检查
need_bin() {
  if ! command -v "$1" >/dev/null 2>&1; then
    c_err "缺少依赖命令：$1"
    exit 1
  fi
}

for bin in git cmake make; do
  need_bin "$bin"
done

# ---------- 公用路径 ----------
OPENSSL_DIR="$PROJECT_ROOT/third/openssl"
CURL_DIR="$PROJECT_ROOT/third/curl"
AWS_CRT_DIR="$PROJECT_ROOT/third/aws-crt-cpp"
AWS_SDK_DIR="$PROJECT_ROOT/third/aws-sdk-cpp"
ROCKSDB_DIR="$PROJECT_ROOT/third/rocksdb"
HIREDIS_DIR="$PROJECT_ROOT/third/hiredis"

OPENSSL_INSTALL="$OPENSSL_DIR/install"
CURL_INSTALL="$CURL_DIR/install"
AWS_CRT_INSTALL="$AWS_CRT_DIR/install"
AWS_SDK_INSTALL="$AWS_SDK_DIR/install"

# ---------- 公用函数 ----------
ensure_submodule() {
  local path="$1"
  local repo="$2"    # 可为空：如果是项目自带子模块，只需要 init/update
  if [[ -d "$path/.git" ]] || git submodule status "$path" >/dev/null 2>&1; then
    c_info "子模块存在：$path -> 更新"
    git submodule update --init --recursive "$path"
  else
    if [[ -n "$repo" ]]; then
      c_info "添加并初始化子模块：$path"
      git submodule add -f "$repo" "$path" || true
      git submodule update --init --recursive "$path"
    else
      c_info "初始化项目自带子模块：$path"
      git submodule update --init --recursive "$path"
    fi
  fi
}

# ---------- 步骤函数 ----------
step_submodules() {
  c_info "[1] 拉取第三方库子模块"
  git submodule update --init --recursive
  c_ok "子模块初始化完成"
}

step_openssl() {
  c_info "[2] 编译 OpenSSL(3.1.4)"
  ensure_submodule "$OPENSSL_DIR" "https://github.com/openssl/openssl.git"
  pushd "$OPENSSL_DIR" >/dev/null
    git fetch --all --tags
    git reset --hard        # 丢弃已跟踪改动
    git clean -fdx          # 删除未跟踪/构建产物（解决 fuzz corpora 阻塞）
    git checkout -f openssl-3.1.4

    ./config --prefix="$OPENSSL_INSTALL" no-shared no-tests
    make -j"$(detect_jobs)"
    make install
  popd >/dev/null
  c_ok "OpenSSL 安装到：$OPENSSL_INSTALL"
}

step_curl() {
  c_info "[3] 编译 curl(8.16.0 with OpenSSL 3.1.4)"
  ensure_submodule "$CURL_DIR" "https://github.com/curl/curl.git" 
  pushd "$CURL_DIR" >/dev/null
    git fetch --all || true
    git checkout curl-8_16_0
    rm -rf build && mkdir build && cd build

    # OPENSSL 路径来自上一步
    local OPENSSL_INC="$OPENSSL_INSTALL/include"
    local OPENSSL_LIB64="$OPENSSL_INSTALL/lib64"
    local OPENSSL_LIB="$OPENSSL_INSTALL/lib"
    # 兼容不同安装布局（有的系统是 lib64，有的是 lib）
    local SSL_LIB="${OPENSSL_LIB64}/libssl.a"
    local CRYPTO_LIB="${OPENSSL_LIB64}/libcrypto.a"
    if [[ ! -f "$SSL_LIB" ]]; then SSL_LIB="${OPENSSL_LIB}/libssl.a"; fi
    if [[ ! -f "$CRYPTO_LIB" ]]; then CRYPTO_LIB="${OPENSSL_LIB}/libcrypto.a"; fi

    cmake .. \
      -DCMAKE_INSTALL_PREFIX="$CURL_INSTALL" \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_USE_OPENSSL=ON \
      -DOPENSSL_INCLUDE_DIR="$OPENSSL_INC" \
      -DOPENSSL_SSL_LIBRARY="$SSL_LIB" \
      -DOPENSSL_CRYPTO_LIBRARY="$CRYPTO_LIB" \
      -DUSE_LIBIDN2=OFF \
      -DCURL_USE_LIBPSL=OFF \
      -DUSE_BROTLI=OFF \
      -DUSE_ZSTD=OFF \
      -DUSE_NGHTTP2=OFF \
      -DCURL_DISABLE_COOKIES=ON \
      -DBUILD_SHARED_LIBS=OFF \
      -DCURL_STATICLIB=ON \
      -DCMAKE_POSITION_INDEPENDENT_CODE=ON
    make -j"$JOBS"
    make install
  popd >/dev/null
  c_ok "curl 安装到：$CURL_INSTALL"
}

step_aws_crt() {
  c_info "[4] 编译 aws-crt-cpp"
  ensure_submodule "$AWS_CRT_DIR" "https://github.com/awslabs/aws-crt-cpp.git"
  pushd "$AWS_CRT_DIR" >/dev/null
    git submodule update --init --recursive
    rm -rf build install
    mkdir build install && cd build

    local OPENSSL_INC="$OPENSSL_INSTALL/include"
    local OPENSSL_LIB64="$OPENSSL_INSTALL/lib64"
    local OPENSSL_LIB="$OPENSSL_INSTALL/lib"
    local SSL_LIB="${OPENSSL_LIB64}/libssl.a"
    local CRYPTO_LIB="${OPENSSL_LIB64}/libcrypto.a"
    if [[ ! -f "$SSL_LIB" ]]; then SSL_LIB="${OPENSSL_LIB}/libssl.a"; fi
    if [[ ! -f "$CRYPTO_LIB" ]]; then CRYPTO_LIB="${OPENSSL_LIB}/libcrypto.a"; fi

    cmake .. \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX="$AWS_CRT_INSTALL" \
      -DUSE_OPENSSL=ON \
      -DS2N_LIBCRYPTO=openssl \
      -DBUILD_DEPS=ON \
      -DOPENSSL_ROOT_DIR="$OPENSSL_INSTALL" \
      -DOPENSSL_INCLUDE_DIR="$OPENSSL_INC" \
      -DOPENSSL_SSL_LIBRARY="$SSL_LIB" \
      -DOPENSSL_CRYPTO_LIBRARY="$CRYPTO_LIB"
    make -j"$(detect_jobs)"
    make install
  popd >/dev/null
  c_ok "aws-crt-cpp 安装到：$AWS_CRT_INSTALL"
}

step_aws_sdk() {
  c_info "[5] 编译 aws-sdk-cpp (s3;core;transfer, 静态)"
  ensure_submodule "$AWS_SDK_DIR" "https://github.com/aws/aws-sdk-cpp.git"
  pushd "$AWS_SDK_DIR" >/dev/null
    git submodule update --init --recursive
    rm -rf build && mkdir build && cd build

    local OPENSSL_INC="$OPENSSL_INSTALL/include"
    local OPENSSL_LIB64="$OPENSSL_INSTALL/lib64"
    local OPENSSL_LIB="$OPENSSL_INSTALL/lib"
    local SSL_LIB="${OPENSSL_LIB64}/libssl.a"
    local CRYPTO_LIB="${OPENSSL_LIB64}/libcrypto.a"
    if [[ ! -f "$SSL_LIB" ]]; then SSL_LIB="${OPENSSL_LIB}/libssl.a"; fi
    if [[ ! -f "$CRYPTO_LIB" ]]; then CRYPTO_LIB="${OPENSSL_LIB}/libcrypto.a"; fi

    local CURL_INC="$CURL_INSTALL/include"
    local CURL_LIBA="$CURL_INSTALL/lib/libcurl.a"

    cmake .. \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_PREFIX_PATH="$AWS_CRT_INSTALL;$AWS_SDK_INSTALL" \
      -DCMAKE_INSTALL_PREFIX="$AWS_SDK_INSTALL" \
      -DBUILD_ONLY="s3;core;transfer" \
      -DBUILD_SHARED_LIBS=OFF \
      -DBUILD_TESTING=OFF \
      -DENABLE_TESTING=OFF \
      -DAUTORUN_UNIT_TESTS=OFF \
      -DLEGACY_BUILD=ON \
      -DCURL_INCLUDE_DIR="$CURL_INC" \
      -DCURL_LIBRARY="$CURL_LIBA" \
      -DOPENSSL_INCLUDE_DIR="$OPENSSL_INC" \
      -DOPENSSL_SSL_LIBRARY="$SSL_LIB" \
      -DOPENSSL_CRYPTO_LIBRARY="$CRYPTO_LIB" \
      -DCMAKE_EXE_LINKER_FLAGS="\
        -Wl,--start-group \
          $CURL_LIBA \
          $SSL_LIB \
          $CRYPTO_LIB \
        -Wl,--end-group \
        -ldl -lpthread" \
      -DCMAKE_SHARED_LINKER_FLAGS="\
        -Wl,--start-group \
          $SSL_LIB \
          $CRYPTO_LIB \
        -Wl,--end-group \
        -ldl -lpthread"

    make -j"$JOBS"
    make install
  popd >/dev/null
  c_ok "aws-sdk-cpp 安装到：$AWS_SDK_INSTALL"
}

step_rocksdb() {
  c_info "[6] 编译 RocksDB ($OS)"
  ensure_submodule "$ROCKSDB_DIR" "https://github.com/facebook/rocksdb.git"

  pushd "$ROCKSDB_DIR" >/dev/null
    rm -rf build
    mkdir build && cd build
    if [[ "$OS" == "linux" ]]; then
      cmake .. \
        -DCMAKE_BUILD_TYPE=Release \
        -DWITH_TESTS=OFF \
        -DWITH_TOOLS=OFF \
        -DWITH_BENCHMARK_TOOLS=OFF \
        -DWITH_GFLAGS=OFF \
        -DWITH_JEMALLOC=OFF \
        -DUSE_RTTI=1 \
        -DWITH_LIBURING=OFF
      make -j"$JOBS"
      ls -lh librocksdb.so || true
      c_info "可在当前终端添加运行库路径："
      echo "export LD_LIBRARY_PATH=$ROCKSDB_DIR/build:\$LD_LIBRARY_PATH"
    else
      cmake .. -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=ON -DCMAKE_CXX_FLAGS="-Wno-unused-parameter"
      make -j"$JOBS"
      ls librocksdb.dylib || true
      c_info "可在当前终端添加运行库路径："
      echo "export DYLD_LIBRARY_PATH=$ROCKSDB_DIR/build:\$DYLD_LIBRARY_PATH"
    fi
  popd >/dev/null
  c_ok "RocksDB 构建完成"
}

step_hiredis() {
  c_info "[7] 编译 hiredis (静态)"
  ensure_submodule "$HIREDIS_DIR" "https://github.com/redis/hiredis.git"

  pushd "$HIREDIS_DIR" >/dev/null
    git submodule update --init --recursive || true
    rm -rf build && mkdir build && cd build
    cmake .. \
      -DBUILD_SHARED_LIBS=OFF \
      -DENABLE_SSL=OFF \
      -DENABLE_EXAMPLES=OFF \
      -DENABLE_TESTS=OFF \
      -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
      -DCMAKE_INSTALL_PREFIX="$PWD/install"
    make -j"$JOBS"
    make install
  popd >/dev/null
  c_ok "hiredis 安装完成（静态）"
}

step_proto() {
  c_info "[8] 生成 protobuf"
  if [[ -x "$PROJECT_ROOT/shell/proto.sh" ]]; then
    "$PROJECT_ROOT/shell/proto.sh"
  else
    c_warn "未找到 shell/proto.sh 或不可执行，跳过"
  fi
  c_ok "protobuf 步骤完成（若脚本存在）"
}

# ---------- 任务选择 ----------
ALL_STEPS=(submodules openssl curl aws-crt-cpp aws-sdk-cpp rocksdb hiredis proto)

run_step() {
  case "$1" in
    submodules)  step_submodules ;;
    openssl)     step_openssl ;;
    curl)        step_curl ;;
    aws-crt-cpp) step_aws_crt ;;
    aws-sdk-cpp) step_aws_sdk ;;
    rocksdb)     step_rocksdb ;;
    hiredis)     step_hiredis ;;
    proto)       step_proto ;;
    *) c_err "未知步骤：$1"; exit 1 ;;
  esac
}

main() {
  c_info "PROJECT_ROOT=$PROJECT_ROOT"
  c_info "INSTALL_ROOT=$INSTALL_ROOT"
  c_info "OS=$OS  JOBS=$JOBS"

  if [[ $# -eq 0 ]]; then
    # 默认顺序与说明保持一致
    SEQ=(submodules openssl curl aws-crt-cpp aws-sdk-cpp rocksdb hiredis proto)
  else
    SEQ=("$@")
  fi

  for s in "${SEQ[@]}"; do
    run_step "$s"
  done

  c_ok "[SUCCESS] 全部完成"
  echo
  echo "常用环境变量导出（按需执行）："
  if [[ "$OS" == "linux" ]]; then
    echo "export LD_LIBRARY_PATH=$ROCKSDB_DIR/build:\$LD_LIBRARY_PATH"
  else
    echo "export DYLD_LIBRARY_PATH=$ROCKSDB_DIR/build:\$DYLD_LIBRARY_PATH"
  fi
}

main "$@"
