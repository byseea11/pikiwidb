//  Copyright (c) 2017-present, Qihoo, Inc.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_BASE_KEY_FORMAT_H_
#define SRC_BASE_KEY_FORMAT_H_

#include "storage_define.h"

namespace s3put
{
  /*
   * used for string data key or hash/zset/set/list's meta key. format:
   * | reserve1 | key | reserve2 |
   * |    8B    |     |   16B    |
   */

  class BaseKey
  {
  public:
    explicit BaseKey(const Slice &key)
        : key_(key),
          raw_size_(key.size()),
          encoded_size_(0) {}

    ~BaseKey()
    {
      if (start_ != space_)
      {
        delete[] start_;
      }
    }

    // 返回原始 key 的大小
    size_t raw_size() const { return raw_size_; }

    // 返回编码后的大小（调用 Encode 之后才会更新）
    size_t encoded_size() const { return encoded_size_; }

    Slice Encode()
    {
      size_t meta_size = sizeof(reserve1_) + sizeof(reserve2_);
      size_t nzero = std::count(key_.data(), key_.data() + key_.size(), kNeedTransformCharacter);
      size_t usize = nzero + kEncodedKeyDelimSize + key_.size();
      size_t needed = meta_size + usize;

      char *dst;
      if (needed <= sizeof(space_))
      {
        dst = space_;
      }
      else
      {
        dst = new char[needed];
        // Need to allocate space, delete previous space
        if (start_ != space_)
        {
          delete[] start_;
        }
      }

      start_ = dst;
      // reserve1: 8 byte
      memcpy(dst, reserve1_, sizeof(reserve1_));
      dst += sizeof(reserve1_);
      // key
      dst = EncodeUserKey(key_, dst, nzero);
      // TODO(wangshaoyi): no need to reserve tailing,
      // since we already set delimiter
      memcpy(dst, reserve2_, sizeof(reserve2_));

      // 记录编码后的大小
      encoded_size_ = needed;

      return Slice(start_, needed);
    }

  private:
    char *start_ = nullptr;
    char space_[200];
    char reserve1_[8] = {0};
    Slice key_;
    char reserve2_[16] = {0};

    size_t raw_size_{0};     // 原始 key 大小
    size_t encoded_size_{0}; // 编码后大小
  };

  class ParsedBaseKey
  {
  public:
    explicit ParsedBaseKey(const std::string *key)
    {
      const char *ptr = key->data();
      const char *end_ptr = key->data() + key->size();
      decode(ptr, end_ptr);
    }

    explicit ParsedBaseKey(const Slice &key)
    {
      const char *ptr = key.data();
      const char *end_ptr = key.data() + key.size();
      decode(ptr, end_ptr);
    }

    void decode(const char *ptr, const char *end_ptr)
    {
      // skip head reserve
      ptr += kPrefixReserveLength;
      // skip tail reserve2_
      end_ptr -= kSuffixReserveLength;
      DecodeUserKey(ptr, std::distance(ptr, end_ptr), &key_str_);
    }

    virtual ~ParsedBaseKey() = default;

    Slice Key() { return Slice(key_str_); }

  protected:
    std::string key_str_;
  };

  using ParsedBaseMetaKey = ParsedBaseKey;
  using BaseMetaKey = BaseKey;

} // namespace s3put

#endif // SRC_BASE_KEY_FORMAT_H_
