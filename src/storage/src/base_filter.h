//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_BASE_FILTER_H_
#define SRC_BASE_FILTER_H_

#include <memory>
#include <string>
#include <vector>

#include "rocksdb/compaction_filter.h"
#include "src/base_data_key_format.h"
#include "src/base_meta_value_format.h"
#include "src/debug.h"

namespace storage {

class BaseMetaFilter : public rocksdb::CompactionFilter {
 public:
  BaseMetaFilter() = default;
  bool Filter(int level, const rocksdb::Slice& key, const rocksdb::Slice& value, std::string* new_value,
              bool* value_changed) const override {
    int64_t unix_time;
    //获取当前时间
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);
    int32_t cur_time = static_cast<int32_t>(unix_time);
      //使用 blackwidow::ParsedBaseMetaValue 对 meta value 进行解析
    ParsedBaseMetaValue parsed_base_meta_value(value);
    TRACE("==========================START==========================");
    TRACE("[MetaFilter], key: %s, count = %d, timestamp: %d, cur_time: %d, version: %d", key.ToString().c_str(),
          parsed_base_meta_value.count(), parsed_base_meta_value.timestamp(), cur_time,
          parsed_base_meta_value.version());
    //若 meta value timestamp 不为零 且 meta value timestamp 小于当前时间 且 meta value version 小于当前时间，则数据可以淘汰
    if (parsed_base_meta_value.timestamp() != 0 && parsed_base_meta_value.timestamp() < cur_time &&
        parsed_base_meta_value.version() < cur_time) {
      TRACE("Drop[Stale & version < cur_time]");
      return true;
    }
    //若 meta value count 为零 且 meta value version 小于当前时间，则数据可以淘汰；
    if (parsed_base_meta_value.count() == 0 && parsed_base_meta_value.version() < cur_time) {
      TRACE("Drop[Empty & version < cur_time]");
      return true;
    }
    //否则数据仍然有效，不能淘汰
    TRACE("Reserve");
    return false;
  }

  const char* Name() const override { return "BaseMetaFilter"; }
};

class BaseMetaFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  BaseMetaFilterFactory() = default;
  std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
      const rocksdb::CompactionFilter::Context& context) override {
    return std::unique_ptr<rocksdb::CompactionFilter>(new BaseMetaFilter());
  }
  const char* Name() const override { return "BaseMetaFilterFactory"; }
};

class BaseDataFilter : public rocksdb::CompactionFilter {
 public:
  BaseDataFilter(rocksdb::DB* db, std::vector<rocksdb::ColumnFamilyHandle*>* cf_handles_ptr)
      : db_(db),
        cf_handles_ptr_(cf_handles_ptr),
        cur_key_(""),
        meta_not_found_(false),
        cur_meta_version_(0),
        cur_meta_timestamp_(0) {}

  bool Filter(int level, const Slice& key, const rocksdb::Slice& value, std::string* new_value,
              bool* value_changed) const override {
      //使用ParsedBaseDataKey 对 data key 进行解析；
    ParsedBaseDataKey parsed_base_data_key(key);
    TRACE("==========================START==========================");
    TRACE("[DataFilter], key: %s, data = %s, version = %d", parsed_base_data_key.key().ToString().c_str(),
          parsed_base_data_key.data().ToString().c_str(), parsed_base_data_key.version());
    // 若 cur_key_ 与 hashtable/zset/set key 不相等，则从 meta ColumnFamily 中获取 hashtable/zset/set 对应的 meta value；
    if (parsed_base_data_key.key().ToString() != cur_key_) {
        //使用 ParsedBaseMetaValue 解析 meta value；
      cur_key_ = parsed_base_data_key.key().ToString();
      std::string meta_value;
      // destroyed when close the database, Reserve Current key value
      if (cf_handles_ptr_->size() == 0) {
        return false;
      }
      //获取 hashtable/zset/set 当前的 curmetaversion_ 与 curmetatimestamp_；
      Status s = db_->Get(default_read_options_, (*cf_handles_ptr_)[0], cur_key_, &meta_value);
      if (s.ok()) {
        meta_not_found_ = false;
        ParsedBaseMetaValue parsed_base_meta_value(&meta_value);
        cur_meta_version_ = parsed_base_meta_value.version();
        cur_meta_timestamp_ = parsed_base_meta_value.timestamp();
      } else if (s.IsNotFound()) {
          //获取不到 meta value 则意味着当前 data KV 可以淘汰；
        meta_not_found_ = true;
      } else {
        cur_key_ = "";
        TRACE("Reserve[Get meta_key faild]");
        return false;
      }
    }
    //获取不到 meta value 则意味着当前 data KV 可以淘汰；
    if (meta_not_found_) {
      TRACE("Drop[Meta key not exist]");
      return true;
    }

    int64_t unix_time;
    //获取当前时间
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);
    //若 cur_meta_timestamp_ 不为零 且 cur_meta_timestamp_ 小于 系统当前时间，则数据可以淘汰；
    if (cur_meta_timestamp_ != 0 && cur_meta_timestamp_ < static_cast<int32_t>(unix_time)) {
      TRACE("Drop[Timeout]");
      return true;
    }
    //若 data key 的 version 小于 cur_meta_version_，秒删功能启用，数据可以淘汰；
    if (cur_meta_version_ > parsed_base_data_key.version()) {
      TRACE("Drop[data_key_version < cur_meta_version]");
      return true;
    } else {
        //否则数据仍然有效，不能淘汰。
      TRACE("Reserve[data_key_version == cur_meta_version]");
      return false;
    }
  }

  const char* Name() const override { return "BaseDataFilter"; }

 private:
  rocksdb::DB* db_;
  std::vector<rocksdb::ColumnFamilyHandle*>* cf_handles_ptr_;
  rocksdb::ReadOptions default_read_options_;
  mutable std::string cur_key_;
  mutable bool meta_not_found_;
  mutable int32_t cur_meta_version_;
  mutable int32_t cur_meta_timestamp_;
};

class BaseDataFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  BaseDataFilterFactory(rocksdb::DB** db_ptr, std::vector<rocksdb::ColumnFamilyHandle*>* handles_ptr)
      : db_ptr_(db_ptr), cf_handles_ptr_(handles_ptr) {}
  std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
      const rocksdb::CompactionFilter::Context& context) override {
    return std::unique_ptr<rocksdb::CompactionFilter>(new BaseDataFilter(*db_ptr_, cf_handles_ptr_));
  }
  const char* Name() const override { return "BaseDataFilterFactory"; }

 private:
  rocksdb::DB** db_ptr_;
  std::vector<rocksdb::ColumnFamilyHandle*>* cf_handles_ptr_;
};

typedef BaseMetaFilter HashesMetaFilter;
typedef BaseMetaFilterFactory HashesMetaFilterFactory;
typedef BaseDataFilter HashesDataFilter;
typedef BaseDataFilterFactory HashesDataFilterFactory;

typedef BaseMetaFilter SetsMetaFilter;
typedef BaseMetaFilterFactory SetsMetaFilterFactory;
typedef BaseDataFilter SetsMemberFilter;
typedef BaseDataFilterFactory SetsMemberFilterFactory;

typedef BaseMetaFilter ZSetsMetaFilter;
typedef BaseMetaFilterFactory ZSetsMetaFilterFactory;
typedef BaseDataFilter ZSetsDataFilter;
typedef BaseDataFilterFactory ZSetsDataFilterFactory;

}  //  namespace storage
#endif  // SRC_BASE_FILTER_H_
