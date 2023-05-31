// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "net/include/net_thread.h"
#include "net/include/net_define.h"
#include "net/src/net_thread_name.h"
#include "pstd/include/xdebug.h"

namespace net {

Thread::Thread() : should_stop_(false), running_(false), thread_id_(0) {}

Thread::~Thread() {}

void* Thread::RunThread(void* arg) {
  Thread* thread = reinterpret_cast<Thread*>(arg);
  if (!(thread->thread_name().empty())) {
    SetThreadName(pthread_self(), thread->thread_name());
  }
  thread->ThreadMain();
  return nullptr;
}
//启动线程
int Thread::StartThread() {
  pstd::MutexLock l(&running_mu_);
  should_stop_ = false;
  if (!running_) {
    running_ = true;
    //创建线程
    return pthread_create(&thread_id_, nullptr, RunThread, (void*)this);
  }
  return 0;
}
//  停止线程
int Thread::StopThread() {
    //加锁
  pstd::MutexLock l(&running_mu_);
  should_stop_ = true;
  if (running_) {
    running_ = false;
    //以阻塞的方式等待thread指定的线程结束。当函数返回时，被等待线程的资源被收回
    return pthread_join(thread_id_, nullptr);
  }
  return 0;
}

int Thread::JoinThread() { return pthread_join(thread_id_, nullptr); }

}  // namespace net
