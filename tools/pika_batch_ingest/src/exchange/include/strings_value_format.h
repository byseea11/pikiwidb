//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_STRINGS_VALUE_FORMAT_H_
#define SRC_STRINGS_VALUE_FORMAT_H_

#include <string>

#include "base_value_format.h"
#include "storage_define.h"

namespace s3put {
/*
* | type | value | reserve | cdate | timestamp |
* |  1B  |       |   16B   |   8B  |     8B    |
*  The first bit in reservse field is used to isolate string and hyperloglog  
*/
 // 80H = 1000000B
constexpr uint8_t hyperloglog_reserve_flag = 0x80;
class StringsValue : public InternalValue {
 public:
  explicit StringsValue(const rocksdb::Slice& user_value)
      : InternalValue(DataType::kStrings, user_value),
        raw_size_(user_value.size()),
        encoded_size_(0) {}

  // 返回原始 value 的大小
  size_t raw_size() const { return raw_size_; }

  // 返回编码后的大小（调用 Encode 后才会更新）
  size_t encoded_size() const { return encoded_size_; }

  virtual rocksdb::Slice Encode() override {
    size_t usize = user_value_.size();
    size_t needed = usize + kSuffixReserveLength + 2 * kTimestampLength + kTypeLength;
    char* dst = ReAllocIfNeeded(needed);
    memcpy(dst, &type_, sizeof(type_));
    dst += sizeof(type_);
    char* start_pos = dst;

    memcpy(dst, user_value_.data(), usize);
    dst += usize;
    memcpy(dst, reserve_, kSuffixReserveLength);
    dst += kSuffixReserveLength;
    uint64_t ctime = ctime_ > 0 ? (ctime_ | (1ULL << 63)) : 0;
    EncodeFixed64(dst, ctime);
    dst += kTimestampLength;
    uint64_t etime = etime_ > 0 ? (etime_ | (1ULL << 63)) : 0;
    EncodeFixed64(dst, etime);

    // 更新编码后大小
    encoded_size_ = needed;

    return {start_, needed};
  }

 private:
  size_t raw_size_{0};     // 原始 value 大小
  size_t encoded_size_{0}; // 编码后大小
};


class HyperloglogValue : public InternalValue {
 public:
  explicit HyperloglogValue(const rocksdb::Slice& user_value) : InternalValue(DataType::kStrings, user_value) {}
  virtual rocksdb::Slice Encode() override {
    size_t usize = user_value_.size();
    size_t needed = usize + kSuffixReserveLength + 2 * kTimestampLength + kTypeLength;
    char* dst = ReAllocIfNeeded(needed);
    memcpy(dst, &type_, sizeof(type_));
    dst += sizeof(type_);
    char* start_pos = dst;

    memcpy(dst, user_value_.data(), usize);
    dst += usize;
    reserve_[0] |= hyperloglog_reserve_flag;
    memcpy(dst, reserve_, kSuffixReserveLength);
    dst += kSuffixReserveLength;
    EncodeFixed64(dst, ctime_);
    dst += kTimestampLength;
    EncodeFixed64(dst, etime_);
    return {start_, needed};
  }
};

class ParsedStringsValue : public ParsedInternalValue {
 public:
  // Use this constructor after rocksdb::DB::Get();
  explicit ParsedStringsValue(std::string* internal_value_str) : ParsedInternalValue(internal_value_str) {
    if (internal_value_str->size() >= kStringsValueMinLength) {
      size_t offset = 0;
      type_ = static_cast<DataType>(static_cast<uint8_t>((*internal_value_str)[0]));
      offset += kTypeLength;
      user_value_ = rocksdb::Slice(internal_value_str->data() + offset,
                                   internal_value_str->size() - kStringsValueSuffixLength - offset);
      offset += user_value_.size();
      memcpy(reserve_, internal_value_str->data() + offset, kSuffixReserveLength);
      offset += kSuffixReserveLength;
      uint64_t ctime = DecodeFixed64(internal_value_str->data() + offset);
      offset += sizeof(ctime_);
      uint64_t etime = DecodeFixed64(internal_value_str->data() + offset);

      ctime_ = (ctime & ~(1ULL << 63));
      // if ctime_==ctime, means ctime_ storaged in seconds
      if (ctime_ == ctime) {
        ctime_ *= 1000;
      }
      etime_ = (etime & ~(1ULL << 63));
      // if etime_==etime, means etime_ storaged in seconds
      if (etime == etime_) {
        etime_ *= 1000;
      }
    }
  }

  // Use this constructor in rocksdb::CompactionFilter::Filter();
  explicit ParsedStringsValue(const rocksdb::Slice& internal_value_slice) : ParsedInternalValue(internal_value_slice) {
    if (internal_value_slice.size() >= kStringsValueMinLength) {
      size_t offset = 0;
      type_ = static_cast<DataType>(static_cast<uint8_t>(internal_value_slice[0]));
      offset += kTypeLength;
      user_value_ = rocksdb::Slice(internal_value_slice.data() + offset, internal_value_slice.size() - kStringsValueSuffixLength - offset);
      offset += user_value_.size();
      memcpy(reserve_, internal_value_slice.data() + offset, kSuffixReserveLength);
      offset += kSuffixReserveLength;
      uint64_t ctime = DecodeFixed64(internal_value_slice.data() + offset);
      offset += kTimestampLength;
      uint64_t etime = DecodeFixed64(internal_value_slice.data() + offset);

      ctime_ = (ctime & ~(1ULL << 63));
      // if ctime_==ctime, means ctime_ storaged in seconds
      if (ctime_ == ctime) {
        ctime_ *= 1000;
      }
      etime_ = (etime & ~(1ULL << 63));
      // if etime_==etime, means etime_ storaged in seconds
      if (etime == etime_) {
        etime_ *= 1000;
      }
    }
  }

  void StripSuffix() override {
    if (value_) {
      value_->erase(0, kTypeLength);
      value_->erase(value_->size() - kStringsValueSuffixLength, kStringsValueSuffixLength);
    }
  }

  // Strings type do not have version field;
  void SetVersionToValue() override {}

  void SetCtimeToValue() override {
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() -
                  kStringsValueSuffixLength + kSuffixReserveLength;
      uint64_t ctime = ctime_ > 0 ? (ctime_ | (1ULL << 63)) : 0;
      EncodeFixed64(dst, ctime);
    }
  }

  void SetEtimeToValue() override {
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() -
                  kStringsValueSuffixLength + kSuffixReserveLength + kTimestampLength;
      uint64_t etime = etime_ > 0 ? (etime_ | (1ULL << 63)) : 0;
      EncodeFixed64(dst, etime);
    }
  }

private:
 const static size_t kStringsValueSuffixLength = 2 * kTimestampLength + kSuffixReserveLength;
 const static size_t kStringsValueMinLength = kStringsValueSuffixLength + kTypeLength;
};

}  //  namespace storage
#endif  // SRC_STRINGS_VALUE_FORMAT_H_
