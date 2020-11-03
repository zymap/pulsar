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

import static org.testng.Assert.assertEquals;

<<<<<<< HEAD
import org.apache.bookkeeper.test.PortManager;
=======
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MockBrokerService;
import org.apache.pulsar.client.api.PulsarClient;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 */
public class ConsumerUnsubscribeTest {

    MockBrokerService mockBrokerService;
<<<<<<< HEAD
    private static final int WEB_SERVICE_PORT = PortManager.nextFreePort();
    private static final int WEB_SERVICE_TLS_PORT = PortManager.nextFreePort();
    private static final int BROKER_SERVICE_PORT = PortManager.nextFreePort();
    private static final int BROKER_SERVICE_TLS_PORT = PortManager.nextFreePort();

    @BeforeClass
    public void setup() {
        mockBrokerService = new MockBrokerService(WEB_SERVICE_PORT, WEB_SERVICE_TLS_PORT, BROKER_SERVICE_PORT,
                BROKER_SERVICE_TLS_PORT);
=======

    @BeforeClass
    public void setup() {
        mockBrokerService = new MockBrokerService();
>>>>>>> f773c602c... Test pr 10 (#27)
        mockBrokerService.start();
    }

    @AfterClass
    public void teardown() {
        mockBrokerService.stop();
    }

    @Test
    public void testConsumerUnsubscribeReference() throws Exception {
        PulsarClientImpl client = (PulsarClientImpl) PulsarClient.builder()
<<<<<<< HEAD
                .serviceUrl("pulsar://127.0.0.1:" + BROKER_SERVICE_PORT).build();
=======
                .serviceUrl(mockBrokerService.getBrokerAddress())
                .build();
>>>>>>> f773c602c... Test pr 10 (#27)

        Consumer<?> consumer = client.newConsumer().topic("persistent://public/default/t1").subscriptionName("sub1").subscribe();
        consumer.unsubscribe();

        assertEquals(client.consumersCount(), 0);
        client.close();
    }
}
