//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_STRINGS_FILTER_H_
#define SRC_STRINGS_FILTER_H_

#include <memory>
#include <string>

#include "rocksdb/compaction_filter.h"
#include "src/debug.h"
#include "src/strings_value_format.h"

namespace storage {

class StringsFilter : public rocksdb::CompactionFilter {
 public:
  StringsFilter() = default;
  bool Filter(int level, const rocksdb::Slice& key, const rocksdb::Slice& value, std::string* new_value,
              bool* value_changed) const override {
    int64_t unix_time;
    //获取当前时间
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);
    int32_t cur_time = static_cast<int32_t>(unix_time);
    //通过 blackwidow::ParsedStringsValue 对 Strings KV 进行解析
    ParsedStringsValue parsed_strings_value(value);
    TRACE("==========================START==========================");
    TRACE("[StringsFilter], key: %s, value = %s, timestamp: %d, cur_time: %d", key.ToString().c_str(),
          parsed_strings_value.value().ToString().c_str(), parsed_strings_value.timestamp(), cur_time);
    //Filter 接口依据 V 中的 timestamp 与系统当前时间进行比较，如果 V 的 timestamp 小于系统当前时间，则数据过时可以淘汰。
    if (parsed_strings_value.timestamp() != 0 && parsed_strings_value.timestamp() < cur_time) {
      TRACE("Drop[Stale]");
      return true;
    } else {
      TRACE("Reserve");
      return false;
    }
  }

  const char* Name() const override { return "StringsFilter"; }
};
//使用 StringsFilter 的 StringsFilterFactory 会被设置为 Strings 的 default ColumnFamily 的 ColumnFamilyOptions 的 compactionfilterfactory
class StringsFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  StringsFilterFactory() = default;
  std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
      const rocksdb::CompactionFilter::Context& context) override {
    return std::unique_ptr<rocksdb::CompactionFilter>(new StringsFilter());
  }
  const char* Name() const override { return "StringsFilterFactory"; }
};

}  //  namespace storage
#endif  // SRC_STRINGS_FILTER_H_
