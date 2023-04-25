//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_BACKUPABLE_H_
#define SRC_BACKUPABLE_H_

#include "rocksdb/db.h"

#include "db_checkpoint.h"
#include "storage.h"
#include "util.h"

namespace storage {

const std::string DEFAULT_BK_PATH = "dump";  // Default backup root dir
const std::string DEFAULT_RS_PATH = "db";    // Default restore root dir

// Arguments which will used by BackupSave Thread
// p_engine for BackupEngine handler
// backup_dir
// key_type kv, hash, list, set or zset
struct BackupSaveArgs {
  void* p_engine;
  const std::string backup_dir;
  const std::string key_type;
  Status res;

  BackupSaveArgs(void* _p_engine, const std::string& _backup_dir, const std::string& _key_type)
      : p_engine(_p_engine), backup_dir(_backup_dir), key_type(_key_type) {}
};

struct BackupContent {
  std::vector<std::string> live_files;
  rocksdb::VectorLogPtr live_wal_files;
  uint64_t manifest_file_size = 0;
  uint64_t sequence_number = 0;
};

class BackupEngine {
 public:
  ~BackupEngine();
    // 调用 BackupEngine::NewCheckpoint 为五种数据类型分别创建响应的 DBNemoCheckpoint 放入 engines_，
    // 同时创建 BackupEngine 对象
  static Status Open(Storage* db, BackupEngine** backup_engine_ptr);
    // 调用 DBNemoCheckpointImpl::GetCheckpointFiles 获取五种类型需要备份的 快照内容 存入 backup_content_
  Status SetBackupContent();
    // 创建五个线程，分别调用 CreateNewBackupSpecify 进行数据备份
  Status CreateNewBackup(const std::string& dir);

  void StopBackup();
  // 调用 DBNemoCheckpointImpl::CreateCheckpointWithFiles 执行具体的备份任务
  // 这个函数之所以类型是 public 的，是为了在 线程函数ThreadFuncSaveSpecify 中能够调用之
  Status CreateNewBackupSpecify(const std::string& dir, const std::string& type);

 private:
  BackupEngine() {}

  std::map<std::string, rocksdb::DBCheckpoint*> engines_;// 保存每个类型的 checkpoint 对象
  std::map<std::string, BackupContent> backup_content_;// 保存每个类型需要复制的 快照内容
  std::map<std::string, pthread_t> backup_pthread_ts_;// 保存每个类型执行备份任务的线程对象
    // 调用 rocksdb::DBNemoCheckpoint::Create 创建 checkpoint 对象
  Status NewCheckpoint(rocksdb::DB* rocksdb_db, const std::string& type);
    // 获取每个类型的数据目录
  std::string GetSaveDirByType(const std::string _dir, const std::string& _type) const {
    std::string backup_dir = _dir.empty() ? DEFAULT_BK_PATH : _dir;
    return backup_dir + ((backup_dir.back() != '/') ? "/" : "") + _type;
  }
  Status WaitBackupPthread();
};

}  //  namespace storage
#endif  //  SRC_BACKUPABLE_H_
