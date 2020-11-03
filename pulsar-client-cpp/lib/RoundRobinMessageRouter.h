/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#ifndef PULSAR_RR_MESSAGE_ROUTER_HEADER_
#define PULSAR_RR_MESSAGE_ROUTER_HEADER_

<<<<<<< HEAD
=======
#include <pulsar/defines.h>
>>>>>>> f773c602c... Test pr 10 (#27)
#include <pulsar/MessageRoutingPolicy.h>
#include <pulsar/ProducerConfiguration.h>
#include <pulsar/TopicMetadata.h>
#include <mutex>
#include "Hash.h"
#include "MessageRouterBase.h"

<<<<<<< HEAD
#pragma GCC visibility push(default)
namespace pulsar {
class RoundRobinMessageRouter : public MessageRouterBase {
=======
namespace pulsar {
class PULSAR_PUBLIC RoundRobinMessageRouter : public MessageRouterBase {
>>>>>>> f773c602c... Test pr 10 (#27)
   public:
    RoundRobinMessageRouter(ProducerConfiguration::HashingScheme hashingScheme);
    virtual ~RoundRobinMessageRouter();
    virtual int getPartition(const Message& msg, const TopicMetadata& topicMetadata);

   private:
    std::mutex mutex_;
    unsigned int prevPartition_;
};
typedef std::unique_lock<std::mutex> Lock;
}  // namespace pulsar
<<<<<<< HEAD
#pragma GCC visibility pop
=======
>>>>>>> f773c602c... Test pr 10 (#27)
#endif  // PULSAR_RR_MESSAGE_ROUTER_HEADER_
