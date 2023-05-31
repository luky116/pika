// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef NET_SRC_NET_EPOLL_H_
#define NET_SRC_NET_EPOLL_H_
#include <vector>

#include <sys/epoll.h>

#include "net/src/net_multiplexer.h"

namespace net {

class NetEpoll final : public NetMultiplexer {
 public:
  NetEpoll(int queue_limit = kUnlimitedQueue);
  ~NetEpoll() = default;
  //注册事件
  int NetAddEvent(int fd, int mask) override;
  //从epfd中删除一个fd
  int NetDelEvent(int fd, int) override;
  //修改已经注册的fd的监听事件
  int NetModEvent(int fd, int old_mask, int mask) override;
  //等待事件触发
  int NetPoll(int timeout) override;

 private:
  std::vector<struct epoll_event> events_;
};

}  // namespace net
#endif  // NET_SRC_NET_EPOLL_H_
