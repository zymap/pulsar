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
#ifndef _PULSAR_TOPIC_NAME_HEADER_
#define _PULSAR_TOPIC_NAME_HEADER_

<<<<<<< HEAD
=======
#include <pulsar/defines.h>
>>>>>>> f773c602c... Test pr 10 (#27)
#include "NamespaceName.h"
#include "ServiceUnitId.h"

#include <string>
#include <curl/curl.h>
#include <mutex>

<<<<<<< HEAD
#pragma GCC visibility push(default)

namespace pulsar {
class TopicName : public ServiceUnitId {
=======
namespace pulsar {
class PULSAR_PUBLIC TopicDomain {
   public:
    static const std::string Persistent;
    static const std::string NonPersistent;
};  // class TopicDomain

class PULSAR_PUBLIC TopicName : public ServiceUnitId {
>>>>>>> f773c602c... Test pr 10 (#27)
   private:
    std::string topicName_;
    std::string domain_;
    std::string property_;
    std::string cluster_;
    std::string namespacePortion_;
    std::string localName_;
    bool isV2Topic_;
    std::shared_ptr<NamespaceName> namespaceName_;
<<<<<<< HEAD
=======
    int partition_ = -1;
>>>>>>> f773c602c... Test pr 10 (#27)

   public:
    bool isV2Topic();
    std::string getLookupName();
    std::string getDomain();
    std::string getProperty();
    std::string getCluster();
    std::string getNamespacePortion();
    std::string getLocalName();
    std::string getEncodedLocalName();
    std::string toString();
<<<<<<< HEAD
    NamespaceNamePtr getNamespaceName();
=======
    bool isPersistent() const;
    NamespaceNamePtr getNamespaceName();
    int getPartitionIndex() const noexcept { return partition_; }
>>>>>>> f773c602c... Test pr 10 (#27)
    static std::shared_ptr<TopicName> get(const std::string& topicName);
    bool operator==(const TopicName& other);
    static std::string getEncodedName(const std::string& nameBeforeEncoding);
    const std::string getTopicPartitionName(unsigned int partition);
<<<<<<< HEAD
=======
    static int getPartitionIndex(const std::string& topic);
>>>>>>> f773c602c... Test pr 10 (#27)

   private:
    static CURL* getCurlHandle();
    static CURL* curl;
    static std::mutex curlHandleMutex;
    static bool parse(const std::string& topicName, std::string& domain, std::string& property,
                      std::string& cluster, std::string& namespacePortion, std::string& localName);
    TopicName();
    bool validate();
    bool init(const std::string& topicName);
<<<<<<< HEAD
};
=======
};  // class TopicName
>>>>>>> f773c602c... Test pr 10 (#27)
typedef std::shared_ptr<TopicName> TopicNamePtr;
}  // namespace pulsar
// end of namespace pulsar

<<<<<<< HEAD
#pragma GCC visibility pop

=======
>>>>>>> f773c602c... Test pr 10 (#27)
#endif  //_PULSAR_TOPIC_NAME_HEADER_
