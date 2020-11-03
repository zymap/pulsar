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
package org.apache.pulsar.proxy.server;

import static org.mockito.Mockito.doReturn;
import static org.testng.Assert.assertTrue;

<<<<<<< HEAD
import org.apache.bookkeeper.test.PortManager;
=======
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import lombok.Cleanup;

>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.mockito.Mockito;
import org.testng.Assert;
<<<<<<< HEAD
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
=======
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.testng.annotations.Test;

public class ProxyLookupThrottlingTest extends MockedPulsarServiceBaseTest {

    private final String DUMMY_VALUE = "DUMMY_VALUE";
    private final int NUM_CONCURRENT_LOOKUP = 3;
    private final int NUM_CONCURRENT_INBOUND_CONNECTION = 5;
    private ProxyService proxyService;
    private ProxyConfiguration proxyConfig = new ProxyConfiguration();

    @Override
<<<<<<< HEAD
    @BeforeClass
    protected void setup() throws Exception {
        internalSetup();

        proxyConfig.setServicePort(PortManager.nextFreePort());
=======
    @BeforeMethod
    protected void setup() throws Exception {
        internalSetup();

        proxyConfig.setServicePort(Optional.of(0));
>>>>>>> f773c602c... Test pr 10 (#27)
        proxyConfig.setZookeeperServers(DUMMY_VALUE);
        proxyConfig.setConfigurationStoreServers(DUMMY_VALUE);
        proxyConfig.setMaxConcurrentLookupRequests(NUM_CONCURRENT_LOOKUP);
        proxyConfig.setMaxConcurrentInboundConnections(NUM_CONCURRENT_INBOUND_CONNECTION);

        AuthenticationService authenticationService = new AuthenticationService(
                PulsarConfigurationLoader.convertFrom(proxyConfig));
        proxyService = Mockito.spy(new ProxyService(proxyConfig, authenticationService));
        doReturn(mockZooKeeperClientFactory).when(proxyService).getZooKeeperClientFactory();

        proxyService.start();
    }

    @Override
<<<<<<< HEAD
    @AfterClass
=======
    @AfterMethod
>>>>>>> f773c602c... Test pr 10 (#27)
    protected void cleanup() throws Exception {
        internalCleanup();
        proxyService.close();
    }

    @Test
    public void testLookup() throws Exception {
<<<<<<< HEAD
        PulsarClient client = PulsarClient.builder().serviceUrl("pulsar://localhost:" + proxyConfig.getServicePort().get())
                .connectionsPerBroker(5).ioThreads(5).build();
        assertTrue(proxyService.getLookupRequestSemaphore().tryAcquire());
        assertTrue(proxyService.getLookupRequestSemaphore().tryAcquire());
=======
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(proxyService.getServiceUrl())
                .connectionsPerBroker(5)
                .ioThreads(5)
                .operationTimeout(1000, TimeUnit.MILLISECONDS)
                .build();
        assertTrue(proxyService.getLookupRequestSemaphore().tryAcquire());
        assertTrue(proxyService.getLookupRequestSemaphore().tryAcquire());

        @Cleanup
>>>>>>> f773c602c... Test pr 10 (#27)
        Producer<byte[]> producer1 = client.newProducer(Schema.BYTES).topic("persistent://sample/test/local/producer-topic")
                .create();
        assertTrue(proxyService.getLookupRequestSemaphore().tryAcquire());
        try {
<<<<<<< HEAD
=======
            @Cleanup
>>>>>>> f773c602c... Test pr 10 (#27)
            Producer<byte[]> producer2 = client.newProducer(Schema.BYTES).topic("persistent://sample/test/local/producer-topic")
                    .create();
            Assert.fail("Should have failed since can't acquire LookupRequestSemaphore");
        } catch (Exception ex) {
            // Ignore
        }
<<<<<<< HEAD
        Assert.assertEquals(LookupProxyHandler.rejectedPartitionsMetadataRequests.get(), 1.0d);
        proxyService.getLookupRequestSemaphore().release();
        try {
=======
        Assert.assertEquals(LookupProxyHandler.rejectedPartitionsMetadataRequests.get(), 5.0d);
        proxyService.getLookupRequestSemaphore().release();
        try {
            @Cleanup
>>>>>>> f773c602c... Test pr 10 (#27)
            Producer<byte[]> producer3 = client.newProducer(Schema.BYTES).topic("persistent://sample/test/local/producer-topic")
                    .create();
        } catch (Exception ex) {
            Assert.fail("Should not have failed since can acquire LookupRequestSemaphore");
        }
<<<<<<< HEAD
        Assert.assertEquals(LookupProxyHandler.rejectedPartitionsMetadataRequests.get(), 1.0d);
        client.close();
=======

        Assert.assertEquals(LookupProxyHandler.rejectedPartitionsMetadataRequests.get(), 5.0d);
>>>>>>> f773c602c... Test pr 10 (#27)
    }
}
