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
#ifndef TOPIC_METADATA_HPP_
#define TOPIC_METADATA_HPP_

<<<<<<< HEAD
#pragma GCC visibility push(default)
=======
#include <pulsar/defines.h>
>>>>>>> f773c602c... Test pr 10 (#27)

namespace pulsar {
/**
 * Metadata of a topic that can be used for message routing.
 */
<<<<<<< HEAD
class TopicMetadata {
=======
class PULSAR_PUBLIC TopicMetadata {
>>>>>>> f773c602c... Test pr 10 (#27)
   public:
    virtual ~TopicMetadata() {}

    virtual int getNumPartitions() const = 0;
};
}  // namespace pulsar

<<<<<<< HEAD
#pragma GCC visibility pop

=======
>>>>>>> f773c602c... Test pr 10 (#27)
#endif /* TOPIC_METADATA_HPP_ */
