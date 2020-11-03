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
#include <pulsar/Client.h>
#include <gtest/gtest.h>

#include "../lib/Future.h"
#include "../lib/Utils.h"

using namespace pulsar;

<<<<<<< HEAD
=======
static std::string serviceUrl = "pulsar://localhost:6650";

>>>>>>> f773c602c... Test pr 10 (#27)
TEST(ProducerTest, producerNotInitialized) {
    Producer producer;

    Message msg = MessageBuilder().setContent("test").build();

    ASSERT_EQ(ResultProducerNotInitialized, producer.send(msg));

<<<<<<< HEAD
    Promise<Result, Message> promise;
    producer.sendAsync(msg, WaitForCallbackValue<Message>(promise));

    Message m;
    ASSERT_EQ(ResultProducerNotInitialized, promise.getFuture().get(m));
=======
    Promise<Result, MessageId> promise;
    producer.sendAsync(msg, WaitForCallbackValue<MessageId>(promise));

    MessageId mi;
    ASSERT_EQ(ResultProducerNotInitialized, promise.getFuture().get(mi));
>>>>>>> f773c602c... Test pr 10 (#27)

    ASSERT_EQ(ResultProducerNotInitialized, producer.close());

    Promise<bool, Result> promiseClose;
    producer.closeAsync(WaitForCallback(promiseClose));

    Result result;
    promiseClose.getFuture().get(result);
    ASSERT_EQ(ResultProducerNotInitialized, result);

    ASSERT_TRUE(producer.getTopic().empty());
}
<<<<<<< HEAD
=======

TEST(ProducerTest, exactlyOnceWithProducerNameSpecified) {
    Client client(serviceUrl);

    std::string topicName = "persistent://public/default/exactlyOnceWithProducerNameSpecified";

    Producer producer1;
    ProducerConfiguration producerConfiguration1;
    producerConfiguration1.setProducerName("p-name-1");

    ASSERT_EQ(ResultOk, client.createProducer(topicName, producerConfiguration1, producer1));

    Producer producer2;
    ProducerConfiguration producerConfiguration2;
    producerConfiguration2.setProducerName("p-name-2");
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producerConfiguration2, producer2));

    Producer producer3;
    Result result = client.createProducer(topicName, producerConfiguration2, producer3);
    ASSERT_EQ(ResultProducerBusy, result);
}
>>>>>>> f773c602c... Test pr 10 (#27)
