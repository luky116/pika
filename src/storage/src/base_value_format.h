//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_BASE_VALUE_FORMAT_H_
#define SRC_BASE_VALUE_FORMAT_H_

#include <string>

#include "rocksdb/env.h"
#include "rocksdb/slice.h"
#include "src/coding.h"

namespace storage {

class InternalValue {
 public:
  explicit InternalValue(const rocksdb::Slice& user_value)
      : start_(nullptr), user_value_(user_value), version_(0), timestamp_(0) {}
  virtual ~InternalValue() {
    if (start_ != space_) {
      delete[] start_;
    }
  }
  void set_timestamp(int32_t timestamp = 0) { timestamp_ = timestamp; }
  void SetRelativeTimestamp(int32_t ttl) {
    int64_t unix_time;
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);
    timestamp_ = static_cast<int32_t>(unix_time) + ttl;
  }
  void set_version(int32_t version = 0) { version_ = version; }
  static const size_t kDefaultValueSuffixLength = sizeof(int32_t) * 2;
  virtual const rocksdb::Slice Encode() {
      //若 key + timestamp + version 拼接后的总长度不大于 200B，
      // 则 InternalValue::start_ = InternalValue::space_，即使用 InternalValue::space_ 存储序列化后的字节流，否则就在堆上分配一段内存用于存储字节流；
    size_t usize = user_value_.size();
    size_t needed = usize + kDefaultValueSuffixLength;
    char* dst;
    if (needed <= sizeof(space_)) {
      dst = space_;
    } else {
      dst = new char[needed];

      // Need to allocate space, delete previous space
      if (start_ != space_) {
        delete[] start_;
      }
    }
    //调用虚接口AppendTimestampAndVersion 对 key + timestamp + version 进行序列化并存入 InternalValue::start_。
    start_ = dst;
    size_t len = AppendTimestampAndVersion();
    return rocksdb::Slice(start_, len);
  }
  virtual size_t AppendTimestampAndVersion() = 0;

 protected:
  char space_[200];
  char* start_;
  rocksdb::Slice user_value_;
  int32_t version_;
  int32_t timestamp_;
};

class ParsedInternalValue {
 public:
    // 这个构造函数在 rocksdb::DB::Get() 之后会被调用，
    // 用户可能在此处对读取到的值修改 timestamp 和 version，
    // 所以需要把 value 的指针赋值给 value_
  explicit ParsedInternalValue(std::string* value) : value_(value), version_(0), timestamp_(0) {}

    // 这个函数在 rocksdb::CompactionFilter::Filter() 之中会被调用，
    // 用户仅仅仅对 @value 进行分析即可，不会有写动作，所以不需要
    // 把 value 的指针赋值给 value_
  explicit ParsedInternalValue(const rocksdb::Slice& value) : value_(nullptr), version_(0), timestamp_(0) {}

  virtual ~ParsedInternalValue() = default;

  rocksdb::Slice user_value() { return user_value_; }

  int32_t version() { return version_; }

  void set_version(int32_t version) {
    version_ = version;
    SetVersionToValue();
  }

  int32_t timestamp() { return timestamp_; }

  void set_timestamp(int32_t timestamp) {
    timestamp_ = timestamp;
    SetTimestampToValue();
  }

  void SetRelativeTimestamp(int32_t ttl) {
    int64_t unix_time;
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);
    timestamp_ = static_cast<int32_t>(unix_time) + ttl;
    SetTimestampToValue();
  }

  bool IsPermanentSurvival() { return timestamp_ == 0; }

  bool IsStale() {
    if (timestamp_ == 0) {
      return false;
    }
    int64_t unix_time;
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);
    return timestamp_ < unix_time;
  }

  virtual void StripSuffix() = 0;

 protected:
  virtual void SetVersionToValue() = 0;
  virtual void SetTimestampToValue() = 0;
  std::string* value_;
  rocksdb::Slice user_value_;// 用户原始 value
  int32_t version_;
  int32_t timestamp_;
};

}  //  namespace storage
#endif  // SRC_BASE_VALUE_FORMAT_H_
