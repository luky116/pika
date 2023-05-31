// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_auxiliary_thread.h"

#include "include/pika_define.h"
#include "include/pika_rm.h"
#include "include/pika_server.h"

extern PikaServer* g_pika_server;
extern PikaReplicaManager* g_pika_rm;

PikaAuxiliaryThread::~PikaAuxiliaryThread() {
  StopThread();
  LOG(INFO) << "PikaAuxiliary thread " << thread_id() << " exit!!!";
}
//此时启动的线程就是位于pika_auxiliary_thread.cc中的线程函数
//这个线程主要做的工作就是1)pika同步meta sync消息 2）trysync消息的发送
//3)驱动BinglogSync启动流程 4)partition级别的keepalive和超时检测
void* PikaAuxiliaryThread::ThreadMain() {
  while (!should_stop()) {//是否停止线程
    if (g_pika_conf->classic_mode()) { //判断当前运行的模式是分布式模式还是经典模式
      if (g_pika_server->ShouldMetaSync()) {//主从模式
        g_pika_rm->SendMetaSyncRequest();//slave向master发送主从同步请求
      } else if (g_pika_server->MetaSyncDone()) {
        g_pika_rm->RunSyncSlavePartitionStateMachine();
      }
    } else {
      g_pika_rm->RunSyncSlavePartitionStateMachine();// 分布式模式则直接启动状态机的同步
    }

    Status s = g_pika_rm->CheckSyncTimeout(pstd::NowMicros());// 检查超时的节点
    if (!s.ok()) {
      LOG(WARNING) << s.ToString();
    }

    g_pika_server->CheckLeaderProtectedMode();

    // TODO(whoiami) timeout
    s = g_pika_server->TriggerSendBinlogSync();// 触发binlog的主从同步，生成每个节点待发送的binlog数据任务
    if (!s.ok()) {
      LOG(WARNING) << s.ToString();
    }
    // send to peer
    int res = g_pika_server->SendToPeer(); // 将待发送的任务加入到工作线程队列中
    if (!res) {
      // sleep 100 ms
      mu_.Lock();
      cv_.TimedWait(100);
      mu_.Unlock();
    } else {
      // LOG_EVERY_N(INFO, 1000) << "Consume binlog number " << res;
    }
  }
  return NULL;
}
