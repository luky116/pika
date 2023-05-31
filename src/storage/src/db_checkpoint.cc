//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2012 Facebook.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef ROCKSDB_LITE

#  include "storage/db_checkpoint.h"

#  ifndef __STDC_FORMAT_MACROS
#    define __STDC_FORMAT_MACROS
#  endif

#  include <inttypes.h>

#  include "file/file_util.h"
#  include "rocksdb/db.h"
// #include "file/filename.h"

namespace rocksdb {

class DBCheckpointImpl : public DBCheckpoint {
 public:
  // Creates a DBCheckPoint object to be used for creating openable snapshots
  explicit DBCheckpointImpl(DB* db) : db_(db) {}
  using DBCheckpoint::CreateCheckpoint;
  // 如果备份目录和源数据目录在同一个磁盘上，则对 SST 文件进行硬链接，
  // 对 manifest 文件和 wal 文件进行直接拷贝
  Status CreateCheckpoint(const std::string& checkpoint_dir) override;

  using DBCheckpoint::GetCheckpointFiles;
  // 先阻止文件删除【rocksdb:DB::DisableFileDeletions】，然后获取 rocksdb:DB 快照，如 db 所有文件名称、
  // manifest 文件大小、SequenceNumber 以及同步点(filenum & offset)
  // BackupEngine 把这些信息组织为BackupContent
  Status GetCheckpointFiles(std::vector<std::string>& live_files, VectorLogPtr& live_wal_files,
                            uint64_t& manifest_file_size, uint64_t& sequence_number) override;

  using DBCheckpoint::CreateCheckpointWithFiles;
  // 根据上面获取到的 快照内容 进行文件复制操作
  Status CreateCheckpointWithFiles(const std::string& checkpoint_dir, std::vector<std::string>& live_files,
                                   VectorLogPtr& live_wal_files, uint64_t manifest_file_size,
                                   uint64_t sequence_number) override;

 private:
  DB* db_;
};

Status DBCheckpoint::Create(DB* db, DBCheckpoint** checkpoint_ptr) {
  *checkpoint_ptr = new DBCheckpointImpl(db);
  return Status::OK();
}

Status DBCheckpoint::CreateCheckpoint(const std::string& checkpoint_dir) { return Status::NotSupported(""); }

// 同步操作，通过调用 GetCheckpointFiles 和 CreateCheckpointWithFiles 实现数据备份。
Status DBCheckpointImpl::CreateCheckpoint(const std::string& checkpoint_dir) {
  std::vector<std::string> live_files;
  VectorLogPtr live_wal_files;
  uint64_t manifest_file_size, sequence_number;
  //获取checkpoint文件
  Status s = GetCheckpointFiles(live_files, live_wal_files, manifest_file_size, sequence_number);
  if (s.ok()) {
      //根据上面获取到的快照的内容，对文件进行复制工作
    s = CreateCheckpointWithFiles(checkpoint_dir, live_files, live_wal_files, manifest_file_size, sequence_number);
  }
  return s;
}
//将这些信息进行组装
Status DBCheckpointImpl::GetCheckpointFiles(std::vector<std::string>& live_files, VectorLogPtr& live_wal_files,
                                            uint64_t& manifest_file_size, uint64_t& sequence_number) {
  Status s;
  sequence_number = db_->GetLatestSequenceNumber();
  //组织文件删除
  s = db_->DisableFileDeletions();
  if (s.ok()) {
    // this will return live_files prefixed with "/"
    s = db_->GetLiveFiles(live_files, &manifest_file_size);
  }

  // 再获取快照内容
  if (s.ok()) {
    s = db_->GetSortedWalFiles(live_wal_files);
  }

  if (!s.ok()) {
    db_->EnableFileDeletions(false);
  }

  return s;
}

Status DBCheckpointImpl::CreateCheckpointWithFiles(const std::string& checkpoint_dir,
                                                   std::vector<std::string>& live_files, VectorLogPtr& live_wal_files,
                                                   uint64_t manifest_file_size, uint64_t sequence_number) {
  bool same_fs = true;
  //checkpint目录是否在checkpoint_dir中
  Status s = db_->GetEnv()->FileExists(checkpoint_dir);
  //如果存在的话则退出
  if (s.ok()) {
    return Status::InvalidArgument("Directory exists");
  } else if (!s.IsNotFound()) {
    assert(s.IsIOError());
    return s;
  }

  size_t wal_size = live_wal_files.size();
  Log(db_->GetOptions().info_log, "Started the snapshot process -- creating snapshot in directory %s",
      checkpoint_dir.c_str());
  //如果不存在的话创建临时路径full_private_path = checkpoint_dir + ".tmp"
  std::string full_private_path = checkpoint_dir + ".tmp";

  // 创建快照的路径
  s = db_->GetEnv()->CreateDir(full_private_path);

  // copy/hard link live_files
  std::string manifest_fname, current_fname;
  for (size_t i = 0; s.ok() && i < live_files.size(); ++i) {
    uint64_t number;
    FileType type;
    //根据live_file的名称获取文件的类型，根据不同的类型分别进行复制
    bool ok = ParseFileName(live_files[i], &number, &type);
    if (!ok) {
      s = Status::Corruption("Can't parse file name. This is very bad");
      break;
    }
    // we should only get sst, options, manifest and current files here
    assert(type == kTableFile || type == kDescriptorFile || type == kCurrentFile || type == kOptionsFile);
    assert(live_files[i].size() > 0 && live_files[i][0] == '/');
    if (type == kCurrentFile) {
      // We will craft the current file manually to ensure it's consistent with
      // the manifest number. This is necessary because current's file contents
      // can change during checkpoint creation.
      current_fname = live_files[i];
      continue;
    } else if (type == kDescriptorFile) {
      manifest_fname = live_files[i];
    }
    std::string src_fname = live_files[i];

    //如果文件的类型是SST，进行hark-link,hark-link失败之后在进行尝试copy
    //如果 type 是其他类型则直接进行 Copy，如果 type 是 kDescriptorFile（manifest 文件）还需要指定文件的大小；
    if ((type == kTableFile) && same_fs) {
      Log(db_->GetOptions().info_log, "Hard Linking %s", src_fname.c_str());
      s = db_->GetEnv()->LinkFile(db_->GetName() + src_fname, full_private_path + src_fname);
      if (s.IsNotSupported()) {
        same_fs = false;
        s = Status::OK();
      }
    }
    if ((type != kTableFile) || (!same_fs)) {
      Log(db_->GetOptions().info_log, "Copying %s", src_fname.c_str());
#  if (ROCKSDB_MAJOR < 5 || (ROCKSDB_MAJOR == 5 && ROCKSDB_MINOR < 3))
      s = CopyFile(db_->GetEnv(), db_->GetName() + src_fname, full_private_path + src_fname,
                   (type == kDescriptorFile) ? manifest_file_size : 0);
#  else
      s = CopyFile(db_->GetFileSystem(), db_->GetName() + src_fname, full_private_path + src_fname,
                   (type == kDescriptorFile) ? manifest_file_size : 0, false, nullptr, Temperature::kUnknown);
#  endif
    }
  }
  if (s.ok() && !current_fname.empty() && !manifest_fname.empty()) {
// 5.17.2 Createfile with new argv use_fsync
#  if (ROCKSDB_MAJOR < 5 || (ROCKSDB_MAJOR == 5 && ROCKSDB_MINOR < 17))
    s = CreateFile(db_->GetEnv(), full_private_path + current_fname, manifest_fname.substr(1) + "\n");
#  else
    //单独创建一个current文件，内容是mainifest文件的名称
    s = CreateFile(db_->GetFileSystem(), full_private_path + current_fname, manifest_fname.substr(1) + "\n", false);
#  endif
  }
  // Log(db_->GetOptions().info_log,
  //    "Number of log files %" ROCKSDB_PRIszt, live_wal_files.size());

  // Link WAL files. Copy exact size of last one because it is the only one
  // that has changes after the last flush.
  for (size_t i = 0; s.ok() && i < wal_size; ++i) {
    if ((live_wal_files[i]->Type() == kAliveLogFile) && (live_wal_files[i]->StartSequence() >= sequence_number)) {
      //如果WAL文件是最后文件集合的最后一个，则copy文件，并且只复制
      if (i + 1 == wal_size) {
        Log(db_->GetOptions().info_log, "Copying %s", live_wal_files[i]->PathName().c_str());
#  if (ROCKSDB_MAJOR < 5 || (ROCKSDB_MAJOR == 5 && ROCKSDB_MINOR < 3))
        s = CopyFile(db_->GetEnv(), db_->GetOptions().wal_dir + live_wal_files[i]->PathName(),
                     full_private_path + live_wal_files[i]->PathName(), live_wal_files[i]->SizeFileBytes());
#  else
        s = CopyFile(db_->GetFileSystem(), db_->GetOptions().wal_dir + live_wal_files[i]->PathName(),
                     full_private_path + live_wal_files[i]->PathName(), live_wal_files[i]->SizeFileBytes(), false,
                     nullptr, Temperature::kUnknown);
#  endif
        break;
      }
      //如果备份文件和原始的文件在同一个系统上，则进行hard link
      if (same_fs) {
        // we only care about live log files
        Log(db_->GetOptions().info_log, "Hard Linking %s", live_wal_files[i]->PathName().c_str());
        s = db_->GetEnv()->LinkFile(db_->GetOptions().wal_dir + live_wal_files[i]->PathName(),
                                    full_private_path + live_wal_files[i]->PathName());
        if (s.IsNotSupported()) {
          same_fs = false;
          s = Status::OK();
        }
      }
      //如果备份文件和原始的文件不在同一个系统上，则直接进行copy
      if (!same_fs) {
        Log(db_->GetOptions().info_log, "Copying %s", live_wal_files[i]->PathName().c_str());
#  if (ROCKSDB_MAJOR < 5 || (ROCKSDB_MAJOR == 5 && ROCKSDB_MINOR < 3))
        s = CopyFile(db_->GetEnv(), db_->GetOptions().wal_dir + live_wal_files[i]->PathName(),
                     full_private_path + live_wal_files[i]->PathName(), 0);
#  else
        s = CopyFile(db_->GetFileSystem(), db_->GetOptions().wal_dir + live_wal_files[i]->PathName(),
                     full_private_path + live_wal_files[i]->PathName(), 0, false, nullptr, Temperature::kUnknown);
#  endif
      }
    }
  }

  // 允许文件删除
  db_->EnableFileDeletions(false);

  if (s.ok()) {
    //把临时目录 “@checkpointdir + .tmp” 重命名为 @checkpointdir，并执行 fsync 操作，把数据刷到磁盘
    s = db_->GetEnv()->RenameFile(full_private_path, checkpoint_dir);
  }
  if (s.ok()) {
    std::unique_ptr<Directory> checkpoint_directory;
    db_->GetEnv()->NewDirectory(checkpoint_dir, &checkpoint_directory);
    if (checkpoint_directory != nullptr) {
      s = checkpoint_directory->Fsync();
    }
  }

  if (!s.ok()) {
    // clean all the files we might have created
    Log(db_->GetOptions().info_log, "Snapshot failed -- %s", s.ToString().c_str());
    // we have to delete the dir and all its children
    std::vector<std::string> subchildren;
    db_->GetEnv()->GetChildren(full_private_path, &subchildren);
    for (auto& subchild : subchildren) {
      std::string subchild_path = full_private_path + "/" + subchild;
      Status s1 = db_->GetEnv()->DeleteFile(subchild_path);
      Log(db_->GetOptions().info_log, "Delete file %s -- %s", subchild_path.c_str(), s1.ToString().c_str());
    }
    // finally delete the private dir
    Status s1 = db_->GetEnv()->DeleteDir(full_private_path);
    Log(db_->GetOptions().info_log, "Delete dir %s -- %s", full_private_path.c_str(), s1.ToString().c_str());
    return s;
  }

  // here we know that we succeeded and installed the new snapshot
  Log(db_->GetOptions().info_log, "Snapshot DONE. All is good");
  Log(db_->GetOptions().info_log, "Snapshot sequence number: %" PRIu64, sequence_number);

  return s;
}
}  // namespace rocksdb

#endif  // ROCKSDB_LITE
