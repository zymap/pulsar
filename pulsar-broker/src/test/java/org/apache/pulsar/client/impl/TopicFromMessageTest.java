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
package org.apache.pulsar.client.impl;

import com.google.common.collect.Lists;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
<<<<<<< HEAD

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

=======
>>>>>>> f773c602c... Test pr 10 (#27)
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TopicFromMessageTest extends ProducerConsumerBase {
<<<<<<< HEAD
    private static final long testTimeout = 90000; // 1.5 min
    private static final Logger log = LoggerFactory.getLogger(TopicFromMessageTest.class);
=======

    private static final long TEST_TIMEOUT = 90000; // 1.5 min
    private static final int BATCHING_MAX_MESSAGES_THRESHOLD = 2;
>>>>>>> f773c602c... Test pr 10 (#27)

    @Override
    @BeforeMethod
    public void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    @AfterMethod
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

<<<<<<< HEAD
    @Test(timeOut = testTimeout)
=======
    @Test(timeOut = TEST_TIMEOUT)
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testSingleTopicConsumerNoBatchShortName() throws Exception {
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic("topic1").subscriptionName("sub1").subscribe();
             Producer<byte[]> producer = pulsarClient.newProducer()
                .topic("topic1").enableBatching(false).create()) {
            producer.send("foobar".getBytes());
            Assert.assertEquals(consumer.receive().getTopicName(), "persistent://public/default/topic1");
        }
    }

<<<<<<< HEAD
    @Test(timeOut = testTimeout)
=======
    @Test(timeOut = TEST_TIMEOUT)
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testSingleTopicConsumerNoBatchFullName() throws Exception {
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic("my-property/my-ns/topic1").subscriptionName("sub1").subscribe();
             Producer<byte[]> producer = pulsarClient.newProducer()
                .topic("my-property/my-ns/topic1").enableBatching(false).create()) {
            producer.send("foobar".getBytes());
            Assert.assertEquals(consumer.receive().getTopicName(), "persistent://my-property/my-ns/topic1");
        }
    }

<<<<<<< HEAD
    @Test(timeOut = testTimeout)
=======
    @Test(timeOut = TEST_TIMEOUT)
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testMultiTopicConsumerNoBatchShortName() throws Exception {
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topics(Lists.newArrayList("topic1", "topic2")).subscriptionName("sub1").subscribe();
             Producer<byte[]> producer1 = pulsarClient.newProducer()
                .topic("topic1").enableBatching(false).create();
             Producer<byte[]> producer2 = pulsarClient.newProducer()
                .topic("topic2").enableBatching(false).create()) {
            producer1.send("foobar".getBytes());
            producer2.send("foobar".getBytes());
            Assert.assertEquals(consumer.receive().getTopicName(), "persistent://public/default/topic1");
            Assert.assertEquals(consumer.receive().getTopicName(), "persistent://public/default/topic2");
        }
    }

<<<<<<< HEAD
    @Test(timeOut = testTimeout)
=======
    @Test(timeOut = TEST_TIMEOUT)
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testSingleTopicConsumerBatchShortName() throws Exception {
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic("topic1").subscriptionName("sub1").subscribe();
             Producer<byte[]> producer = pulsarClient.newProducer()
<<<<<<< HEAD
                .topic("topic1").enableBatching(true).batchingMaxMessages(1).create()) {
=======
                .topic("topic1").enableBatching(true).batchingMaxMessages(BATCHING_MAX_MESSAGES_THRESHOLD).create()) {
>>>>>>> f773c602c... Test pr 10 (#27)
            producer.send("foobar".getBytes());

            Assert.assertEquals(consumer.receive().getTopicName(), "persistent://public/default/topic1");
        }
    }

<<<<<<< HEAD
    @Test(timeOut = testTimeout)
=======
    @Test(timeOut = TEST_TIMEOUT)
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testMultiTopicConsumerBatchShortName() throws Exception {
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topics(Lists.newArrayList("topic1", "topic2")).subscriptionName("sub1").subscribe();
             Producer<byte[]> producer1 = pulsarClient.newProducer()
<<<<<<< HEAD
                .topic("topic1").enableBatching(true).batchingMaxMessages(1).create();
             Producer<byte[]> producer2 = pulsarClient.newProducer()
                .topic("topic2").enableBatching(true).batchingMaxMessages(1).create()) {
=======
                .topic("topic1").enableBatching(true).batchingMaxMessages(BATCHING_MAX_MESSAGES_THRESHOLD).create();
             Producer<byte[]> producer2 = pulsarClient.newProducer()
                .topic("topic2").enableBatching(true).batchingMaxMessages(BATCHING_MAX_MESSAGES_THRESHOLD).create()) {
>>>>>>> f773c602c... Test pr 10 (#27)

            producer1.send("foobar".getBytes());
            producer2.send("foobar".getBytes());

            Assert.assertEquals(consumer.receive().getTopicName(), "persistent://public/default/topic1");
            Assert.assertEquals(consumer.receive().getTopicName(), "persistent://public/default/topic2");
        }
    }

}
