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
package org.apache.pulsar.websocket.proxy.v1;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import com.google.common.collect.Sets;

import java.net.URI;
<<<<<<< HEAD
=======
import java.util.Optional;
>>>>>>> f773c602c... Test pr 10 (#27)
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

<<<<<<< HEAD
import org.apache.bookkeeper.test.PortManager;
=======
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.client.api.v1.V1_ProducerConsumerBase;
import org.apache.pulsar.websocket.WebSocketService;
import org.apache.pulsar.websocket.proxy.SimpleConsumerSocket;
import org.apache.pulsar.websocket.proxy.SimpleProducerSocket;
import org.apache.pulsar.websocket.service.ProxyServer;
import org.apache.pulsar.websocket.service.WebSocketProxyConfiguration;
import org.apache.pulsar.websocket.service.WebSocketServiceStarter;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class V1_ProxyAuthenticationTest extends V1_ProducerConsumerBase {

<<<<<<< HEAD
    private int port;
=======
>>>>>>> f773c602c... Test pr 10 (#27)
    private ProxyServer proxyServer;
    private WebSocketService service;
    private WebSocketClient consumeClient;
    private WebSocketClient produceClient;

    @BeforeMethod
    public void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();

<<<<<<< HEAD
        port = PortManager.nextFreePort();
        WebSocketProxyConfiguration config = new WebSocketProxyConfiguration();
        config.setWebServicePort(port);
=======
        WebSocketProxyConfiguration config = new WebSocketProxyConfiguration();
        config.setWebServicePort(Optional.of(0));
>>>>>>> f773c602c... Test pr 10 (#27)
        config.setClusterName("use");
        config.setAuthenticationEnabled(true);
        // If this is not set, 500 error occurs.
        config.setConfigurationStoreServers("dummy");
        config.setSuperUserRoles(Sets.newHashSet("pulsar.super_user"));

        if (methodName.equals("authenticatedSocketTest") || methodName.equals("statsTest")) {
            config.setAuthenticationProviders(Sets.newHashSet("org.apache.pulsar.websocket.proxy.MockAuthenticationProvider"));
        } else {
            config.setAuthenticationProviders(Sets.newHashSet("org.apache.pulsar.websocket.proxy.MockUnauthenticationProvider"));
        }
        if (methodName.equals("anonymousSocketTest")) {
            config.setAnonymousUserRole("anonymousUser");
        }

        service = spy(new WebSocketService(config));
        doReturn(mockZooKeeperClientFactory).when(service).getZooKeeperClientFactory();
        proxyServer = new ProxyServer(config);
        WebSocketServiceStarter.start(proxyServer, service);
        log.info("Proxy Server Started");
    }

    @AfterMethod
    protected void cleanup() throws Exception {
        ExecutorService executor = newFixedThreadPool(1);
        try {
            executor.submit(() -> {
                try {
                    consumeClient.stop();
                    produceClient.stop();
                    log.info("proxy clients are stopped successfully");
                } catch (Exception e) {
                    log.error(e.getMessage());
                }
            }).get(2, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("failed to close clients ", e);
        }
        executor.shutdownNow();

        super.internalCleanup();
        service.close();
        proxyServer.stop();
        log.info("Finished Cleaning Up Test setup");

    }

    public void socketTest() throws Exception {
        final String topic = "prop/use/my-ns/my-topic1";
<<<<<<< HEAD
        final String consumerUri = "ws://localhost:" + port + "/ws/consumer/persistent/" + topic + "/my-sub";
        final String producerUri = "ws://localhost:" + port + "/ws/producer/persistent/" + topic;
=======
        final String consumerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get() + "/ws/consumer/persistent/" + topic + "/my-sub";
        final String producerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get() + "/ws/producer/persistent/" + topic;
>>>>>>> f773c602c... Test pr 10 (#27)
        URI consumeUri = URI.create(consumerUri);
        URI produceUri = URI.create(producerUri);

        consumeClient = new WebSocketClient();
        SimpleConsumerSocket consumeSocket = new SimpleConsumerSocket();
        produceClient = new WebSocketClient();
        SimpleProducerSocket produceSocket = new SimpleProducerSocket();

        consumeClient.start();
        ClientUpgradeRequest consumeRequest = new ClientUpgradeRequest();
        Future<Session> consumerFuture = consumeClient.connect(consumeSocket, consumeUri, consumeRequest);
        log.info("Connecting to : {}", consumeUri);

        ClientUpgradeRequest produceRequest = new ClientUpgradeRequest();
        produceClient.start();
        Future<Session> producerFuture = produceClient.connect(produceSocket, produceUri, produceRequest);
        Assert.assertTrue(consumerFuture.get().isOpen());
        Assert.assertTrue(producerFuture.get().isOpen());

        consumeSocket.awaitClose(1, TimeUnit.SECONDS);
        produceSocket.awaitClose(1, TimeUnit.SECONDS);
        Assert.assertTrue(produceSocket.getBuffer().size() > 0);
        Assert.assertEquals(produceSocket.getBuffer(), consumeSocket.getBuffer());
    }

    @Test(timeOut=10000)
    public void authenticatedSocketTest() throws Exception {
        socketTest();
    }

    @Test(timeOut=10000)
    public void anonymousSocketTest() throws Exception {
        socketTest();
    }

    @Test(timeOut=10000)
    public void unauthenticatedSocketTest() throws Exception{
        Exception exception = null;
        try {
            socketTest();
        } catch (Exception e) {
            exception = e;
        }
        Assert.assertTrue(exception instanceof java.util.concurrent.ExecutionException);
    }

    @Test(timeOut=10000)
    public void statsTest() throws Exception {
        final String topic = "prop/use/my-ns/my-topic2";
<<<<<<< HEAD
        final String consumerUri = "ws://localhost:" + port + "/ws/consumer/persistent/" + topic + "/my-sub";
        final String producerUri = "ws://localhost:" + port + "/ws/producer/persistent/" + topic;
=======
        final String consumerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get() + "/ws/consumer/persistent/" + topic + "/my-sub";
        final String producerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get() + "/ws/producer/persistent/" + topic;
>>>>>>> f773c602c... Test pr 10 (#27)
        URI consumeUri = URI.create(consumerUri);
        URI produceUri = URI.create(producerUri);

        WebSocketClient consumeClient = new WebSocketClient();
        SimpleConsumerSocket consumeSocket = new SimpleConsumerSocket();
        WebSocketClient produceClient = new WebSocketClient();
        SimpleProducerSocket produceSocket = new SimpleProducerSocket();

<<<<<<< HEAD
        final String baseUrl = "http://localhost:" + port + "/admin/proxy-stats/";
=======
        final String baseUrl = "http://localhost:" + proxyServer.getListenPortHTTP().get() + "/admin/proxy-stats/";
>>>>>>> f773c602c... Test pr 10 (#27)
        Client client = ClientBuilder.newClient();

        try {
            consumeClient.start();
            ClientUpgradeRequest consumeRequest = new ClientUpgradeRequest();
            Future<Session> consumerFuture = consumeClient.connect(consumeSocket, consumeUri, consumeRequest);
            Assert.assertTrue(consumerFuture.get().isOpen());

            produceClient.start();
            ClientUpgradeRequest produceRequest = new ClientUpgradeRequest();
            Future<Session> producerFuture = produceClient.connect(produceSocket, produceUri, produceRequest);
            Assert.assertTrue(producerFuture.get().isOpen());

            int retry = 0;
            int maxRetry = 500;
            while (consumeSocket.getReceivedMessagesCount() < 3) {
                Thread.sleep(10);
                if (retry++ > maxRetry) {
                    break;
                }
            }

            service.getProxyStats().generate();

            verifyResponseStatus(client, baseUrl + "metrics");
            verifyResponseStatus(client, baseUrl + "stats");
            verifyResponseStatus(client, baseUrl + topic + "/stats");
        } finally {
            consumeClient.stop();
            produceClient.stop();
            client.close();
        }
    }

    private void verifyResponseStatus(Client client, String url) {
        WebTarget webTarget = client.target(url);
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        Response response = (Response) invocationBuilder.get();
        Assert.assertEquals(response.getStatus(), 200);
    }

    private static final Logger log = LoggerFactory.getLogger(V1_ProxyAuthenticationTest.class);
}
