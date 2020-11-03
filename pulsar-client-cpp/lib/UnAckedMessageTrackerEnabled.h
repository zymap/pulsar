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
#ifndef LIB_UNACKEDMESSAGETRACKERENABLED_H_
#define LIB_UNACKEDMESSAGETRACKERENABLED_H_
#include "lib/UnAckedMessageTrackerInterface.h"

#include <mutex>

namespace pulsar {
<<<<<<< HEAD

=======
>>>>>>> f773c602c... Test pr 10 (#27)
class UnAckedMessageTrackerEnabled : public UnAckedMessageTrackerInterface {
   public:
    ~UnAckedMessageTrackerEnabled();
    UnAckedMessageTrackerEnabled(long timeoutMs, const ClientImplPtr, ConsumerImplBase&);
<<<<<<< HEAD
=======
    UnAckedMessageTrackerEnabled(long timeoutMs, long tickDuration, const ClientImplPtr, ConsumerImplBase&);
>>>>>>> f773c602c... Test pr 10 (#27)
    bool add(const MessageId& m);
    bool remove(const MessageId& m);
    void removeMessagesTill(const MessageId& msgId);
    void removeTopicMessage(const std::string& topic);
    void timeoutHandler();

    void clear();

   private:
    void timeoutHandlerHelper();
    bool isEmpty();
    long size();
<<<<<<< HEAD
    std::set<MessageId> currentSet_;
    std::set<MessageId> oldSet_;
=======
    std::map<MessageId, std::set<MessageId>&> messageIdPartitionMap;
    std::deque<std::set<MessageId>> timePartitions;
>>>>>>> f773c602c... Test pr 10 (#27)
    std::mutex lock_;
    DeadlineTimerPtr timer_;
    ConsumerImplBase& consumerReference_;
    ClientImplPtr client_;
    long timeoutMs_;
<<<<<<< HEAD
=======
    long tickDurationInMs_;
>>>>>>> f773c602c... Test pr 10 (#27)
};
}  // namespace pulsar

#endif /* LIB_UNACKEDMESSAGETRACKERENABLED_H_ */
