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
package org.apache.pulsar.websocket.proxy;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
<<<<<<< HEAD
=======
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
>>>>>>> f773c602c... Test pr 10 (#27)

import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
<<<<<<< HEAD
=======
import java.util.Optional;
>>>>>>> f773c602c... Test pr 10 (#27)
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletResponse;
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
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.websocket.WebSocketService;
import org.apache.pulsar.websocket.service.ProxyServer;
import org.apache.pulsar.websocket.service.WebSocketProxyConfiguration;
import org.apache.pulsar.websocket.service.WebSocketServiceStarter;
import org.apache.pulsar.websocket.stats.ProxyTopicStat;
import org.apache.pulsar.websocket.stats.ProxyTopicStat.ConsumerStats;
import org.apache.pulsar.websocket.stats.ProxyTopicStat.ProducerStats;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.UpgradeException;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.logging.LoggingFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ProxyPublishConsumeTest extends ProducerConsumerBase {
    protected String methodName;
<<<<<<< HEAD
    private int port;
=======
>>>>>>> f773c602c... Test pr 10 (#27)

    private ProxyServer proxyServer;
    private WebSocketService service;

    private static final int TIME_TO_CHECK_BACKLOG_QUOTA = 5;

    @BeforeMethod
    public void setup() throws Exception {
        conf.setBacklogQuotaCheckIntervalInSeconds(TIME_TO_CHECK_BACKLOG_QUOTA);

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
        config.setClusterName("test");
        config.setConfigurationStoreServers("dummy-zk-servers");
        service = spy(new WebSocketService(config));
        doReturn(mockZooKeeperClientFactory).when(service).getZooKeeperClientFactory();
        proxyServer = new ProxyServer(config);
        WebSocketServiceStarter.start(proxyServer, service);
        log.info("Proxy Server Started");
    }

    @AfterMethod
    protected void cleanup() throws Exception {
        super.resetConfig();
        super.internalCleanup();
        service.close();
        proxyServer.stop();
        log.info("Finished Cleaning Up Test setup");
    }

    @Test(timeOut = 10000)
    public void socketTest() throws Exception {
<<<<<<< HEAD
        final String consumerUri = "ws://localhost:" + port
                + "/ws/v2/consumer/persistent/my-property/my-ns/my-topic1/my-sub1?subscriptionType=Failover";
        String readerUri = "ws://localhost:" + port + "/ws/v2/reader/persistent/my-property/my-ns/my-topic1";
        String producerUri = "ws://localhost:" + port + "/ws/v2/producer/persistent/my-property/my-ns/my-topic1/";
=======
        final String consumerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get()
                + "/ws/v2/consumer/persistent/my-property/my-ns/my-topic1/my-sub1?subscriptionType=Failover";
        String readerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get() + "/ws/v2/reader/persistent/my-property/my-ns/my-topic1";
        String producerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get() + "/ws/v2/producer/persistent/my-property/my-ns/my-topic1/";
>>>>>>> f773c602c... Test pr 10 (#27)

        URI consumeUri = URI.create(consumerUri);
        URI readUri = URI.create(readerUri);
        URI produceUri = URI.create(producerUri);

        WebSocketClient consumeClient1 = new WebSocketClient();
        SimpleConsumerSocket consumeSocket1 = new SimpleConsumerSocket();
        WebSocketClient consumeClient2 = new WebSocketClient();
        SimpleConsumerSocket consumeSocket2 = new SimpleConsumerSocket();
        WebSocketClient readClient = new WebSocketClient();
        SimpleConsumerSocket readSocket = new SimpleConsumerSocket();
        WebSocketClient produceClient = new WebSocketClient();
        SimpleProducerSocket produceSocket = new SimpleProducerSocket();

        try {
            consumeClient1.start();
            consumeClient2.start();
            ClientUpgradeRequest consumeRequest1 = new ClientUpgradeRequest();
            ClientUpgradeRequest consumeRequest2 = new ClientUpgradeRequest();
            Future<Session> consumerFuture1 = consumeClient1.connect(consumeSocket1, consumeUri, consumeRequest1);
            Future<Session> consumerFuture2 = consumeClient2.connect(consumeSocket2, consumeUri, consumeRequest2);
            log.info("Connecting to : {}", consumeUri);

            readClient.start();
            ClientUpgradeRequest readRequest = new ClientUpgradeRequest();
            Future<Session> readerFuture = readClient.connect(readSocket, readUri, readRequest);
            log.info("Connecting to : {}", readUri);

            // let it connect
<<<<<<< HEAD
            Assert.assertTrue(consumerFuture1.get().isOpen());
            Assert.assertTrue(consumerFuture2.get().isOpen());
            Assert.assertTrue(readerFuture.get().isOpen());
=======
            assertTrue(consumerFuture1.get().isOpen());
            assertTrue(consumerFuture2.get().isOpen());
            assertTrue(readerFuture.get().isOpen());
>>>>>>> f773c602c... Test pr 10 (#27)

            // Also make sure subscriptions and reader are already created
            Thread.sleep(500);

            ClientUpgradeRequest produceRequest = new ClientUpgradeRequest();
            produceClient.start();
            Future<Session> producerFuture = produceClient.connect(produceSocket, produceUri, produceRequest);
<<<<<<< HEAD
            Assert.assertTrue(producerFuture.get().isOpen());
=======
            assertTrue(producerFuture.get().isOpen());
>>>>>>> f773c602c... Test pr 10 (#27)

            int retry = 0;
            int maxRetry = 400;
            while ((consumeSocket1.getReceivedMessagesCount() < 10 && consumeSocket2.getReceivedMessagesCount() < 10)
                    || readSocket.getReceivedMessagesCount() < 10) {
                Thread.sleep(10);
                if (retry++ > maxRetry) {
                    final String msg = String.format("Consumer still has not received the message after %s ms",
                            (maxRetry * 10));
                    log.warn(msg);
                    throw new IllegalStateException(msg);
                }
            }

            // if the subscription type is exclusive (default), either of the consumer sessions has already been closed
<<<<<<< HEAD
            Assert.assertTrue(consumerFuture1.get().isOpen());
            Assert.assertTrue(consumerFuture2.get().isOpen());
            Assert.assertTrue(produceSocket.getBuffer().size() > 0);

            if (consumeSocket1.getBuffer().size() > consumeSocket2.getBuffer().size()) {
                Assert.assertEquals(produceSocket.getBuffer(), consumeSocket1.getBuffer());
            } else {
                Assert.assertEquals(produceSocket.getBuffer(), consumeSocket2.getBuffer());
            }
            Assert.assertEquals(produceSocket.getBuffer(), readSocket.getBuffer());
=======
            assertTrue(consumerFuture1.get().isOpen());
            assertTrue(consumerFuture2.get().isOpen());
            assertTrue(produceSocket.getBuffer().size() > 0);

            if (consumeSocket1.getBuffer().size() > consumeSocket2.getBuffer().size()) {
                assertEquals(produceSocket.getBuffer(), consumeSocket1.getBuffer());
            } else {
                assertEquals(produceSocket.getBuffer(), consumeSocket2.getBuffer());
            }
            assertEquals(produceSocket.getBuffer(), readSocket.getBuffer());
>>>>>>> f773c602c... Test pr 10 (#27)
        } finally {
            stopWebSocketClient(consumeClient1, consumeClient2, readClient, produceClient);
        }
    }

<<<<<<< HEAD
=======
    @Test
    public void unsubscribeTest() throws Exception {
        final String namespace = "my-property/my-ns";
        final String topic = namespace + "/" + "my-topic7";
        final String topicName = "persistent://" + topic + System.nanoTime();
        admin.topics().createPartitionedTopic(topicName, 3);

        final String subscription = "my-sub";
        final String consumerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get() + "/ws/v2/consumer/persistent/" + topic + "/" + subscription;

        URI consumeUri = URI.create(consumerUri);
        WebSocketClient consumeClient = new WebSocketClient();
        SimpleConsumerSocket consumeSocket = new SimpleConsumerSocket();
        Thread.sleep(500);

        try {
            // setup a consumer
            consumeClient.start();
            ClientUpgradeRequest consumeRequest = new ClientUpgradeRequest();
            Future<Session> consumerFuture = consumeClient.connect(consumeSocket, consumeUri, consumeRequest);
            consumerFuture.get();
            List<String> subs = admin.topics().getSubscriptions(topic);
            Assert.assertEquals(subs.size(), 1);
            Assert.assertEquals(subs.get(0), subscription);
            // do unsubscribe
            consumeSocket.unsubscribe();
            //wait for delete
            Thread.sleep(1000);
            subs = admin.topics().getSubscriptions(topic);
            Assert.assertEquals(subs.size(), 0);
        } finally {
            stopWebSocketClient(consumeClient);
        }
    }

>>>>>>> f773c602c... Test pr 10 (#27)
    @Test(timeOut = 10000)
    public void emptySubcriptionConsumerTest() throws Exception {

        // Empty subcription name
<<<<<<< HEAD
        final String consumerUri = "ws://localhost:" + port
=======
        final String consumerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get()
>>>>>>> f773c602c... Test pr 10 (#27)
                + "/ws/v2/consumer/persistent/my-property/my-ns/my-topic2/?subscriptionType=Exclusive";
        URI consumeUri = URI.create(consumerUri);

        WebSocketClient consumeClient1 = new WebSocketClient();
        SimpleConsumerSocket consumeSocket1 = new SimpleConsumerSocket();

        try {
            consumeClient1.start();
            ClientUpgradeRequest consumeRequest1 = new ClientUpgradeRequest();
            Future<Session> consumerFuture1 = consumeClient1.connect(consumeSocket1, consumeUri, consumeRequest1);
            consumerFuture1.get();
<<<<<<< HEAD
            Assert.fail("should fail: empty subscription");
        } catch (Exception e) {
            // Expected
            Assert.assertTrue(e.getCause() instanceof UpgradeException);
            Assert.assertEquals(((UpgradeException) e.getCause()).getResponseStatusCode(),
=======
            fail("should fail: empty subscription");
        } catch (Exception e) {
            // Expected
            assertTrue(e.getCause() instanceof UpgradeException);
            assertEquals(((UpgradeException) e.getCause()).getResponseStatusCode(),
>>>>>>> f773c602c... Test pr 10 (#27)
                    HttpServletResponse.SC_BAD_REQUEST);
        } finally {
            stopWebSocketClient(consumeClient1);
        }
    }

    @Test(timeOut = 10000)
    public void conflictingConsumerTest() throws Exception {
<<<<<<< HEAD
        final String consumerUri = "ws://localhost:" + port
=======
        final String consumerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get()
>>>>>>> f773c602c... Test pr 10 (#27)
                + "/ws/v2/consumer/persistent/my-property/my-ns/my-topic3/sub1?subscriptionType=Exclusive";
        URI consumeUri = URI.create(consumerUri);

        WebSocketClient consumeClient1 = new WebSocketClient();
        WebSocketClient consumeClient2 = new WebSocketClient();
        SimpleConsumerSocket consumeSocket1 = new SimpleConsumerSocket();
        SimpleConsumerSocket consumeSocket2 = new SimpleConsumerSocket();

        try {
            consumeClient1.start();
            ClientUpgradeRequest consumeRequest1 = new ClientUpgradeRequest();
            Future<Session> consumerFuture1 = consumeClient1.connect(consumeSocket1, consumeUri, consumeRequest1);
            consumerFuture1.get();

            try {
                consumeClient2.start();
                ClientUpgradeRequest consumeRequest2 = new ClientUpgradeRequest();
                Future<Session> consumerFuture2 = consumeClient2.connect(consumeSocket2, consumeUri, consumeRequest2);
                consumerFuture2.get();
<<<<<<< HEAD
                Assert.fail("should fail: conflicting subscription name");
            } catch (Exception e) {
                // Expected
                Assert.assertTrue(e.getCause() instanceof UpgradeException);
                Assert.assertEquals(((UpgradeException) e.getCause()).getResponseStatusCode(),
=======
                fail("should fail: conflicting subscription name");
            } catch (Exception e) {
                // Expected
                assertTrue(e.getCause() instanceof UpgradeException);
                assertEquals(((UpgradeException) e.getCause()).getResponseStatusCode(),
>>>>>>> f773c602c... Test pr 10 (#27)
                        HttpServletResponse.SC_CONFLICT);
            } finally {
                stopWebSocketClient(consumeClient2);
            }
        } finally {
            stopWebSocketClient(consumeClient1);
        }
    }

    @Test(timeOut = 10000)
    public void conflictingProducerTest() throws Exception {
<<<<<<< HEAD
        final String producerUri = "ws://localhost:" + port
=======
        final String producerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get()
>>>>>>> f773c602c... Test pr 10 (#27)
                + "/ws/v2/producer/persistent/my-property/my-ns/my-topic4?producerName=my-producer";
        URI produceUri = URI.create(producerUri);

        WebSocketClient produceClient1 = new WebSocketClient();
        WebSocketClient produceClient2 = new WebSocketClient();
        SimpleProducerSocket produceSocket1 = new SimpleProducerSocket();
        SimpleProducerSocket produceSocket2 = new SimpleProducerSocket();

        try {
            produceClient1.start();
            ClientUpgradeRequest produceRequest1 = new ClientUpgradeRequest();
            Future<Session> producerFuture1 = produceClient1.connect(produceSocket1, produceUri, produceRequest1);
            producerFuture1.get();

            try {
                produceClient2.start();
                ClientUpgradeRequest produceRequest2 = new ClientUpgradeRequest();
                Future<Session> producerFuture2 = produceClient2.connect(produceSocket2, produceUri, produceRequest2);
                producerFuture2.get();
<<<<<<< HEAD
                Assert.fail("should fail: conflicting producer name");
            } catch (Exception e) {
                // Expected
                Assert.assertTrue(e.getCause() instanceof UpgradeException);
                Assert.assertEquals(((UpgradeException) e.getCause()).getResponseStatusCode(),
=======
                fail("should fail: conflicting producer name");
            } catch (Exception e) {
                // Expected
                assertTrue(e.getCause() instanceof UpgradeException);
                assertEquals(((UpgradeException) e.getCause()).getResponseStatusCode(),
>>>>>>> f773c602c... Test pr 10 (#27)
                        HttpServletResponse.SC_CONFLICT);
            } finally {
                stopWebSocketClient(produceClient2);
            }
        } finally {
            stopWebSocketClient(produceClient1);
        }
    }

    @Test// (timeOut = 30000)
    public void producerBacklogQuotaExceededTest() throws Exception {
        String namespace = "my-property/ns-ws-quota";
        admin.namespaces().createNamespace(namespace);
        admin.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("test"));
        admin.namespaces().setBacklogQuota(namespace,
                new BacklogQuota(10, BacklogQuota.RetentionPolicy.producer_request_hold));

        final String topic = namespace + "/my-topic5";
        final String subscription = "my-sub";
<<<<<<< HEAD
        final String consumerUri = "ws://localhost:" + port + "/ws/v2/consumer/persistent/" + topic + "/" + subscription;
        final String producerUri = "ws://localhost:" + port + "/ws/v2/producer/persistent/" + topic;
=======
        final String consumerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get() + "/ws/v2/consumer/persistent/" + topic + "/" + subscription;
        final String producerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get() + "/ws/v2/producer/persistent/" + topic;
>>>>>>> f773c602c... Test pr 10 (#27)

        URI consumeUri = URI.create(consumerUri);
        URI produceUri = URI.create(producerUri);

        WebSocketClient consumeClient = new WebSocketClient();
        WebSocketClient produceClient1 = new WebSocketClient();
        WebSocketClient produceClient2 = new WebSocketClient();

        SimpleConsumerSocket consumeSocket = new SimpleConsumerSocket();
        SimpleProducerSocket produceSocket1 = new SimpleProducerSocket();
        SimpleProducerSocket produceSocket2 = new SimpleProducerSocket();

        // Create subscription
        try {
            consumeClient.start();
            ClientUpgradeRequest consumeRequest = new ClientUpgradeRequest();
            Future<Session> consumerFuture = consumeClient.connect(consumeSocket, consumeUri, consumeRequest);
            consumerFuture.get();
        } finally {
            stopWebSocketClient(consumeClient);
        }

        // Fill the backlog
        try {
            produceClient1.start();
            ClientUpgradeRequest produceRequest = new ClientUpgradeRequest();
            Future<Session> producerFuture = produceClient1.connect(produceSocket1, produceUri, produceRequest);
            producerFuture.get();
            produceSocket1.sendMessage(100);
        } finally {
            stopWebSocketClient(produceClient1);
        }

        Thread.sleep((TIME_TO_CHECK_BACKLOG_QUOTA + 1) * 1000);

        // New producer fails to connect
        try {
            produceClient2.start();
            ClientUpgradeRequest produceRequest = new ClientUpgradeRequest();
            Future<Session> producerFuture = produceClient2.connect(produceSocket2, produceUri, produceRequest);
            producerFuture.get();
<<<<<<< HEAD
            Assert.fail("should fail: backlog quota exceeded");
        } catch (Exception e) {
            // Expected
            Assert.assertTrue(e.getCause() instanceof UpgradeException);
            Assert.assertEquals(((UpgradeException) e.getCause()).getResponseStatusCode(),
=======
            fail("should fail: backlog quota exceeded");
        } catch (Exception e) {
            // Expected
            assertTrue(e.getCause() instanceof UpgradeException);
            assertEquals(((UpgradeException) e.getCause()).getResponseStatusCode(),
>>>>>>> f773c602c... Test pr 10 (#27)
                    HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        } finally {
            stopWebSocketClient(produceClient2);
            admin.topics().skipAllMessages("persistent://" + topic, subscription);
            admin.topics().delete("persistent://" + topic);
            admin.namespaces().removeBacklogQuota(namespace);
            admin.namespaces().deleteNamespace(namespace);
        }
    }

    /**
     * It verifies proxy topic-stats and proxy-metrics api
     *
     * @throws Exception
     */
    @Test(timeOut = 10000)
    public void testProxyStats() throws Exception {
        final String topic = "my-property/my-ns/my-topic6";
<<<<<<< HEAD
        final String consumerUri = "ws://localhost:" + port + "/ws/v2/consumer/persistent/" + topic
                + "/my-sub?subscriptionType=Failover";
        final String producerUri = "ws://localhost:" + port + "/ws/v2/producer/persistent/" + topic + "/";
        final String readerUri = "ws://localhost:" + port + "/ws/v2/reader/persistent/" + topic;
=======
        final String consumerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get() + "/ws/v2/consumer/persistent/" + topic
                + "/my-sub?subscriptionType=Failover";
        final String producerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get() + "/ws/v2/producer/persistent/" + topic + "/";
        final String readerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get() + "/ws/v2/reader/persistent/" + topic;
>>>>>>> f773c602c... Test pr 10 (#27)
        System.out.println(consumerUri + ", " + producerUri);
        URI consumeUri = URI.create(consumerUri);
        URI produceUri = URI.create(producerUri);
        URI readUri = URI.create(readerUri);

        WebSocketClient consumeClient1 = new WebSocketClient();
        SimpleConsumerSocket consumeSocket1 = new SimpleConsumerSocket();
        WebSocketClient produceClient = new WebSocketClient();
        SimpleProducerSocket produceSocket = new SimpleProducerSocket();
        WebSocketClient readClient = new WebSocketClient();
        SimpleConsumerSocket readSocket = new SimpleConsumerSocket();

        try {
            consumeClient1.start();
            ClientUpgradeRequest consumeRequest1 = new ClientUpgradeRequest();
            Future<Session> consumerFuture1 = consumeClient1.connect(consumeSocket1, consumeUri, consumeRequest1);
            log.info("Connecting to : {}", consumeUri);

            readClient.start();
            ClientUpgradeRequest readRequest = new ClientUpgradeRequest();
            Future<Session> readerFuture = readClient.connect(readSocket, readUri, readRequest);
            log.info("Connecting to : {}", readUri);

<<<<<<< HEAD
            Assert.assertTrue(consumerFuture1.get().isOpen());
            Assert.assertTrue(readerFuture.get().isOpen());
=======
            assertTrue(consumerFuture1.get().isOpen());
            assertTrue(readerFuture.get().isOpen());
>>>>>>> f773c602c... Test pr 10 (#27)

            Thread.sleep(500);

            ClientUpgradeRequest produceRequest = new ClientUpgradeRequest();
            produceClient.start();
            Future<Session> producerFuture = produceClient.connect(produceSocket, produceUri, produceRequest);
            // let it connect

<<<<<<< HEAD
            Assert.assertTrue(producerFuture.get().isOpen());
=======
            assertTrue(producerFuture.get().isOpen());
>>>>>>> f773c602c... Test pr 10 (#27)

            // sleep so, proxy can deliver few messages to consumers for stats
            int retry = 0;
            int maxRetry = 400;
            while (consumeSocket1.getReceivedMessagesCount() < 2) {
                Thread.sleep(10);
                if (retry++ > maxRetry) {
                    final String msg = String.format("Consumer still has not received the message after %s ms", (maxRetry * 10));
                    log.warn(msg);
                    break;
                }
            }

            Client client = ClientBuilder.newClient(new ClientConfig().register(LoggingFeature.class));
<<<<<<< HEAD
            final String baseUrl = pulsar.getWebServiceAddress()
                    .replace(Integer.toString(pulsar.getConfiguration().getWebServicePort().get()), (Integer.toString(port)))
=======
            final String baseUrl = pulsar.getSafeWebServiceAddress()
                    .replace(Integer.toString(pulsar.getConfiguration().getWebServicePort().get()),
                            (Integer.toString(proxyServer.getListenPortHTTP().get())))
>>>>>>> f773c602c... Test pr 10 (#27)
                    + "/admin/v2/proxy-stats/";

            // verify proxy metrics
            verifyProxyMetrics(client, baseUrl);

            // verify proxy stats
            verifyProxyStats(client, baseUrl, topic);

            // verify topic stat
            verifyTopicStat(client, baseUrl + "persistent/", topic);

        } finally {
<<<<<<< HEAD
            stopWebSocketClient(consumeClient1, produceClient);
=======
            stopWebSocketClient(consumeClient1, produceClient, readClient);
>>>>>>> f773c602c... Test pr 10 (#27)
        }
    }

    @Test(timeOut = 10000)
    public void consumeMessagesInPartitionedTopicTest() throws Exception {
        final String namespace = "my-property/my-ns";
        final String topic = namespace + "/" + "my-topic7";
        admin.topics().createPartitionedTopic("persistent://" + topic, 3);

        final String subscription = "my-sub";
<<<<<<< HEAD
        final String consumerUri = "ws://localhost:" + port + "/ws/v2/consumer/persistent/" + topic + "/" + subscription;
        final String producerUri = "ws://localhost:" + port + "/ws/v2/producer/persistent/" + topic;
=======
        final String consumerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get() + "/ws/v2/consumer/persistent/" + topic + "/" + subscription;
        final String producerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get() + "/ws/v2/producer/persistent/" + topic;
>>>>>>> f773c602c... Test pr 10 (#27)

        URI consumeUri = URI.create(consumerUri);
        URI produceUri = URI.create(producerUri);

        WebSocketClient consumeClient = new WebSocketClient();
        WebSocketClient produceClient = new WebSocketClient();

        SimpleConsumerSocket consumeSocket = new SimpleConsumerSocket();
        SimpleProducerSocket produceSocket = new SimpleProducerSocket();

        try {
            produceClient.start();
            ClientUpgradeRequest produceRequest = new ClientUpgradeRequest();
            Future<Session> producerFuture = produceClient.connect(produceSocket, produceUri, produceRequest);
            producerFuture.get();
            produceSocket.sendMessage(100);
        } finally {
            stopWebSocketClient(produceClient);
        }

        Thread.sleep(500);

        try {
            consumeClient.start();
            ClientUpgradeRequest consumeRequest = new ClientUpgradeRequest();
            Future<Session> consumerFuture = consumeClient.connect(consumeSocket, consumeUri, consumeRequest);
            consumerFuture.get();
        } finally {
            stopWebSocketClient(consumeClient);
        }
    }

    @Test(timeOut = 10000)
    public void socketPullModeTest() throws Exception {
        final String topic = "my-property/my-ns/my-topic8";
        final String subscription = "my-sub";
        final String consumerUri = String.format(
                "ws://localhost:%d/ws/v2/consumer/persistent/%s/%s?pullMode=true&subscriptionType=Shared",
<<<<<<< HEAD
                port, topic, subscription
        );
        final String producerUri = String.format("ws://localhost:%d/ws/v2/producer/persistent/%s", port, topic);
=======
                proxyServer.getListenPortHTTP().get(), topic, subscription
        );
        final String producerUri = String.format("ws://localhost:%d/ws/v2/producer/persistent/%s", proxyServer.getListenPortHTTP().get(), topic);
>>>>>>> f773c602c... Test pr 10 (#27)

        URI consumeUri = URI.create(consumerUri);
        URI produceUri = URI.create(producerUri);

        WebSocketClient consumeClient1 = new WebSocketClient();
        SimpleConsumerSocket consumeSocket1 = new SimpleConsumerSocket();
        WebSocketClient consumeClient2 = new WebSocketClient();
        SimpleConsumerSocket consumeSocket2 = new SimpleConsumerSocket();
        WebSocketClient produceClient = new WebSocketClient();
        SimpleProducerSocket produceSocket = new SimpleProducerSocket();

        try {
            consumeClient1.start();
            consumeClient2.start();
            ClientUpgradeRequest consumeRequest1 = new ClientUpgradeRequest();
            ClientUpgradeRequest consumeRequest2 = new ClientUpgradeRequest();
            Future<Session> consumerFuture1 = consumeClient1.connect(consumeSocket1, consumeUri, consumeRequest1);
            Future<Session> consumerFuture2 = consumeClient2.connect(consumeSocket2, consumeUri, consumeRequest2);
            log.info("Connecting to : {}", consumeUri);

            // let it connect
<<<<<<< HEAD
            Assert.assertTrue(consumerFuture1.get().isOpen());
            Assert.assertTrue(consumerFuture2.get().isOpen());
=======
            assertTrue(consumerFuture1.get().isOpen());
            assertTrue(consumerFuture2.get().isOpen());
>>>>>>> f773c602c... Test pr 10 (#27)

            ClientUpgradeRequest produceRequest = new ClientUpgradeRequest();
            produceClient.start();
            Future<Session> producerFuture = produceClient.connect(produceSocket, produceUri, produceRequest);
<<<<<<< HEAD
            Assert.assertTrue(producerFuture.get().isOpen());
=======
            assertTrue(producerFuture.get().isOpen());
>>>>>>> f773c602c... Test pr 10 (#27)
            produceSocket.sendMessage(100);

            Thread.sleep(500);

            // Verify no messages received despite production
<<<<<<< HEAD
            Assert.assertEquals(consumeSocket1.getReceivedMessagesCount(), 0);
            Assert.assertEquals(consumeSocket2.getReceivedMessagesCount(), 0);
=======
            assertEquals(consumeSocket1.getReceivedMessagesCount(), 0);
            assertEquals(consumeSocket2.getReceivedMessagesCount(), 0);
>>>>>>> f773c602c... Test pr 10 (#27)

            consumeSocket1.sendPermits(3);
            consumeSocket2.sendPermits(2);
            consumeSocket2.sendPermits(2);
            consumeSocket2.sendPermits(2);

            Thread.sleep(500);

<<<<<<< HEAD
            Assert.assertEquals(consumeSocket1.getReceivedMessagesCount(), 3);
            Assert.assertEquals(consumeSocket2.getReceivedMessagesCount(), 6);
=======
            assertEquals(consumeSocket1.getReceivedMessagesCount(), 3);
            assertEquals(consumeSocket2.getReceivedMessagesCount(), 6);
>>>>>>> f773c602c... Test pr 10 (#27)

        } finally {
            stopWebSocketClient(consumeClient1, consumeClient2, produceClient);
        }
    }

    private void verifyTopicStat(Client client, String baseUrl, String topic) {
        String statUrl = baseUrl + topic + "/stats";
        WebTarget webTarget = client.target(statUrl);
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        Response response = (Response) invocationBuilder.get();
        String responseStr = response.readEntity(String.class);
        final Gson gson = new Gson();
        final ProxyTopicStat data = gson.fromJson(responseStr, ProxyTopicStat.class);
<<<<<<< HEAD
        Assert.assertFalse(data.producerStats.isEmpty());
        Assert.assertFalse(data.consumerStats.isEmpty());
=======
        assertFalse(data.producerStats.isEmpty());
        assertFalse(data.consumerStats.isEmpty());
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    private void verifyProxyMetrics(Client client, String baseUrl) {
        // generate metrics
        service.getProxyStats().generate();
        // collect metrics
        String statUrl = baseUrl + "metrics";
        WebTarget webTarget = client.target(statUrl);
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        Response response = (Response) invocationBuilder.get();
        String responseStr = response.readEntity(String.class);
        final Gson gson = new Gson();
        List<Metrics> data = gson.fromJson(responseStr, new TypeToken<List<Metrics>>() {
        }.getType());
<<<<<<< HEAD
        Assert.assertFalse(data.isEmpty());
=======
        assertFalse(data.isEmpty());
>>>>>>> f773c602c... Test pr 10 (#27)
        // re-generate metrics
        service.getProxyStats().generate();
        invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        response = (Response) invocationBuilder.get();
        responseStr = response.readEntity(String.class);
        data = gson.fromJson(responseStr, new TypeToken<List<Metrics>>() {
        }.getType());
<<<<<<< HEAD
        Assert.assertFalse(data.isEmpty());
=======
        assertFalse(data.isEmpty());
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    private void verifyProxyStats(Client client, String baseUrl, String topic) {

        String statUrl = baseUrl + "stats";
        WebTarget webTarget = client.target(statUrl);
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        Response response = (Response) invocationBuilder.get();
        String responseStr = response.readEntity(String.class);
        final Gson gson = new Gson();
        final Map<String, ProxyTopicStat> data = gson.fromJson(responseStr,
                new TypeToken<Map<String, ProxyTopicStat>>() {
                }.getType());
        // number of topic is loaded = 1
<<<<<<< HEAD
        Assert.assertEquals(data.size(), 1);
        Entry<String, ProxyTopicStat> entry = data.entrySet().iterator().next();
        Assert.assertEquals(entry.getKey(), "persistent://" + topic);
        ProxyTopicStat stats = entry.getValue();
        // number of consumers are connected = 2 (one is reader)
        Assert.assertEquals(stats.consumerStats.size(), 2);
        ConsumerStats consumerStats = stats.consumerStats.iterator().next();
        // Assert.assertTrue(consumerStats.numberOfMsgDelivered > 0);
        Assert.assertNotNull(consumerStats.remoteConnection);

        // number of producers are connected = 1
        Assert.assertEquals(stats.producerStats.size(), 1);
        ProducerStats producerStats = stats.producerStats.iterator().next();
        // Assert.assertTrue(producerStats.numberOfMsgPublished > 0);
        Assert.assertNotNull(producerStats.remoteConnection);
=======
        assertEquals(data.size(), 1);
        Entry<String, ProxyTopicStat> entry = data.entrySet().iterator().next();
        assertEquals(entry.getKey(), "persistent://" + topic);
        ProxyTopicStat stats = entry.getValue();
        // number of consumers are connected = 2 (one is reader)
        assertEquals(stats.consumerStats.size(), 2);
        ConsumerStats consumerStats = stats.consumerStats.iterator().next();
        // Assert.assertTrue(consumerStats.numberOfMsgDelivered > 0);
        assertNotNull(consumerStats.remoteConnection);

        // number of producers are connected = 1
        assertEquals(stats.producerStats.size(), 1);
        ProducerStats producerStats = stats.producerStats.iterator().next();
        // Assert.assertTrue(producerStats.numberOfMsgPublished > 0);
        assertNotNull(producerStats.remoteConnection);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    private void stopWebSocketClient(WebSocketClient... clients) {
        ExecutorService executor = newFixedThreadPool(1);
        try {
            executor.submit(() -> {
<<<<<<< HEAD
                try {
                    for (WebSocketClient client : clients) {
                        client.stop();
                    }
                    log.info("proxy clients are stopped successfully");
                } catch (Exception e) {
                    log.error(e.getMessage());
                }
=======
                for (WebSocketClient client : clients) {
                    try {
                        client.stop();
                    } catch (Exception e) {
                        log.error(e.getMessage());
                    }
                }
                log.info("proxy clients are stopped successfully");
>>>>>>> f773c602c... Test pr 10 (#27)
            }).get(2, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("failed to close proxy clients", e);
        }
<<<<<<< HEAD
    }

    private static final Logger log = LoggerFactory.getLogger(ProxyPublishConsumeTest.class);
}
=======
        executor.shutdownNow();
    }

    private static final Logger log = LoggerFactory.getLogger(ProxyPublishConsumeTest.class);
}
>>>>>>> f773c602c... Test pr 10 (#27)
