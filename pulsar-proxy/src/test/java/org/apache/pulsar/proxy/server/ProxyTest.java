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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.Mockito.doReturn;
import static org.testng.Assert.assertEquals;

<<<<<<< HEAD
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;

import org.apache.bookkeeper.test.PortManager;
=======
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.util.Optional;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import lombok.Cleanup;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import org.apache.avro.reflect.Nullable;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
<<<<<<< HEAD
=======
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandActiveConsumerChange;
import org.apache.pulsar.common.api.proto.PulsarApi.ProtocolVersion;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
<<<<<<< HEAD
import org.apache.pulsar.common.policies.data.TenantInfo;
=======
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.schema.SchemaInfo;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
<<<<<<< HEAD
=======
import org.testng.Assert;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ProxyTest extends MockedPulsarServiceBaseTest {

    private static final Logger log = LoggerFactory.getLogger(ProxyTest.class);

    private final String DUMMY_VALUE = "DUMMY_VALUE";

    private ProxyService proxyService;
    private ProxyConfiguration proxyConfig = new ProxyConfiguration();

<<<<<<< HEAD
=======
    @Data
    @ToString
    @EqualsAndHashCode
    public static class Foo {
        @Nullable
        private String field1;
        @Nullable
        private String field2;
        private int field3;
    }

>>>>>>> f773c602c... Test pr 10 (#27)
    @Override
    @BeforeClass
    protected void setup() throws Exception {
        internalSetup();

<<<<<<< HEAD
        proxyConfig.setServicePort(PortManager.nextFreePort());
=======
        proxyConfig.setServicePort(Optional.ofNullable(0));
>>>>>>> f773c602c... Test pr 10 (#27)
        proxyConfig.setZookeeperServers(DUMMY_VALUE);
        proxyConfig.setConfigurationStoreServers(DUMMY_VALUE);

        proxyService = Mockito.spy(new ProxyService(proxyConfig, new AuthenticationService(
                                                            PulsarConfigurationLoader.convertFrom(proxyConfig))));
        doReturn(mockZooKeeperClientFactory).when(proxyService).getZooKeeperClientFactory();

        proxyService.start();
    }

    @Override
    @AfterClass
    protected void cleanup() throws Exception {
        internalCleanup();

        proxyService.close();
    }

    @Test
    public void testProducer() throws Exception {
<<<<<<< HEAD
        PulsarClient client = PulsarClient.builder().serviceUrl("pulsar://localhost:" + proxyConfig.getServicePort().get())
                .build();
        Producer<byte[]> producer = client.newProducer(Schema.BYTES).topic("persistent://sample/test/local/producer-topic")
                .create();
=======
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(proxyService.getServiceUrl())
                .build();

        @Cleanup
        Producer<byte[]> producer = client.newProducer()
            .topic("persistent://sample/test/local/producer-topic")
            .create();
>>>>>>> f773c602c... Test pr 10 (#27)

        for (int i = 0; i < 10; i++) {
            producer.send("test".getBytes());
        }
<<<<<<< HEAD

        client.close();
=======
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Test
    public void testProducerConsumer() throws Exception {
<<<<<<< HEAD
        PulsarClient client = PulsarClient.builder().serviceUrl("pulsar://localhost:" + proxyConfig.getServicePort().get())
                .build();
=======
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(proxyService.getServiceUrl())
                .build();

        @Cleanup
>>>>>>> f773c602c... Test pr 10 (#27)
        Producer<byte[]> producer = client.newProducer(Schema.BYTES)
            .topic("persistent://sample/test/local/producer-consumer-topic")
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        // Create a consumer directly attached to broker
<<<<<<< HEAD
=======
        @Cleanup
>>>>>>> f773c602c... Test pr 10 (#27)
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic("persistent://sample/test/local/producer-consumer-topic").subscriptionName("my-sub").subscribe();

        for (int i = 0; i < 10; i++) {
            producer.send("test".getBytes());
        }

        for (int i = 0; i < 10; i++) {
            Message<byte[]> msg = consumer.receive(1, TimeUnit.SECONDS);
            checkNotNull(msg);
            consumer.acknowledge(msg);
        }

        Message<byte[]> msg = consumer.receive(0, TimeUnit.SECONDS);
        checkArgument(msg == null);
<<<<<<< HEAD

        consumer.close();
        client.close();
=======
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Test
    public void testPartitions() throws Exception {
<<<<<<< HEAD
        admin.tenants().createTenant("sample", new TenantInfo());
        PulsarClient client = PulsarClient.builder().serviceUrl("pulsar://localhost:" + proxyConfig.getServicePort().get())
                .build();
        admin.topics().createPartitionedTopic("persistent://sample/test/local/partitioned-topic", 2);

=======
        TenantInfo tenantInfo = createDefaultTenantInfo();
        admin.tenants().createTenant("sample", tenantInfo);
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(proxyService.getServiceUrl())
                .build();
        admin.topics().createPartitionedTopic("persistent://sample/test/local/partitioned-topic", 2);

        @Cleanup
>>>>>>> f773c602c... Test pr 10 (#27)
        Producer<byte[]> producer = client.newProducer(Schema.BYTES)
            .topic("persistent://sample/test/local/partitioned-topic")
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.RoundRobinPartition).create();

        // Create a consumer directly attached to broker
<<<<<<< HEAD
=======
        @Cleanup
>>>>>>> f773c602c... Test pr 10 (#27)
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("persistent://sample/test/local/partitioned-topic")
                .subscriptionName("my-sub").subscribe();

        for (int i = 0; i < 10; i++) {
            producer.send("test".getBytes());
        }

        for (int i = 0; i < 10; i++) {
            Message<byte[]> msg = consumer.receive(1, TimeUnit.SECONDS);
            checkNotNull(msg);
        }
<<<<<<< HEAD

        client.close();
=======
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Test
    public void testRegexSubscription() throws Exception {
<<<<<<< HEAD
        PulsarClient client = PulsarClient.builder().serviceUrl("pulsar://localhost:" + proxyConfig.getServicePort().get())
=======
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(proxyService.getServiceUrl())
>>>>>>> f773c602c... Test pr 10 (#27)
            .connectionsPerBroker(5).ioThreads(5).build();

        // create two topics by subscribing to a topic and closing it
        try (Consumer<byte[]> ignored = client.newConsumer()
<<<<<<< HEAD
            .topic("persistent://sample/test/local/topic1")
            .subscriptionName("ignored")
            .subscribe()) {
        }
        try (Consumer<byte[]> ignored = client.newConsumer()
            .topic("persistent://sample/test/local/topic2")
            .subscriptionName("ignored")
            .subscribe()) {
        }

        // make sure regex subscription
        String regexSubscriptionPattern = "persistent://sample/test/local/topic.*";
        log.info("Regex subscribe to topics {}", regexSubscriptionPattern);
        try (Consumer<byte[]> consumer = client.newConsumer()
            .topicsPattern(regexSubscriptionPattern)
            .subscriptionName("regex-sub")
=======
            .topic("persistent://sample/test/local/regex-sub-topic1")
            .subscriptionName("proxy-ignored")
            .subscribe()) {
        }
        try (Consumer<byte[]> ignored = client.newConsumer()
            .topic("persistent://sample/test/local/regex-sub-topic2")
            .subscriptionName("proxy-ignored")
            .subscribe()) {
        }

        String subName = "regex-sub-proxy-test-" + System.currentTimeMillis();

        // make sure regex subscription
        String regexSubscriptionPattern = "persistent://sample/test/local/regex-sub-topic.*";
        log.info("Regex subscribe to topics {}", regexSubscriptionPattern);
        try (Consumer<byte[]> consumer = client.newConsumer()
            .topicsPattern(regexSubscriptionPattern)
            .subscriptionName(subName)
            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
>>>>>>> f773c602c... Test pr 10 (#27)
            .subscribe()) {
            log.info("Successfully subscribe to topics using regex {}", regexSubscriptionPattern);

            final int numMessages = 20;

            try (Producer<byte[]> producer = client.newProducer(Schema.BYTES)
<<<<<<< HEAD
                .topic("persistent://sample/test/local/topic1")
=======
                .topic("persistent://sample/test/local/regex-sub-topic1")
>>>>>>> f773c602c... Test pr 10 (#27)
                .create()) {
                for (int i = 0; i < numMessages; i++) {
                    producer.send(("message-" + i).getBytes(UTF_8));
                }
            }

            for (int i = 0; i < numMessages; i++) {
                Message<byte[]> msg = consumer.receive();
                assertEquals("message-" + i, new String(msg.getValue(), UTF_8));
            }
        }
    }

    @Test
<<<<<<< HEAD
    private void testProtocolVersionAdvertisement() throws Exception {
        final String url = "pulsar://localhost:" + proxyConfig.getServicePort().get();
=======
    public void testGetSchema() throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(proxyService.getServiceUrl())
                .build();
        Schema<Foo> schema = Schema.AVRO(Foo.class);
        try {
            try (Producer<Foo> ignored = client.newProducer(schema).topic("persistent://sample/test/local/get-schema")
                .create()) {
            }
        } catch (Exception ex) {
            Assert.fail("Should not have failed since can acquire LookupRequestSemaphore");
        }
        byte[] schemaVersion = new byte[8];
        byte b = new Long(0l).byteValue();
        for (int i = 0; i<8; i++){
            schemaVersion[i] = b;
        }
        SchemaInfo schemaInfo = ((PulsarClientImpl) client).getLookup()
                .getSchema(TopicName.get("persistent://sample/test/local/get-schema"), schemaVersion).get().orElse(null);
        Assert.assertEquals(new String(schemaInfo.getSchema()), new String(schema.getSchemaInfo().getSchema()));
    }

    @Test
    public void testProtocolVersionAdvertisement() throws Exception {
>>>>>>> f773c602c... Test pr 10 (#27)
        final String topic = "persistent://sample/test/local/protocol-version-advertisement";
        final String sub = "my-sub";

        ClientConfigurationData conf = new ClientConfigurationData();
<<<<<<< HEAD
        conf.setServiceUrl(url);
        PulsarClient client = getClientActiveConsumerChangeNotSupported(conf);

        Producer<byte[]> producer = client.newProducer().topic(topic).create();
=======
        conf.setServiceUrl(proxyService.getServiceUrl());

        @Cleanup
        PulsarClient client = getClientActiveConsumerChangeNotSupported(conf);

        @Cleanup
        Producer<byte[]> producer = client.newProducer().topic(topic).create();

        @Cleanup
>>>>>>> f773c602c... Test pr 10 (#27)
        Consumer<byte[]> consumer = client.newConsumer().topic(topic).subscriptionName(sub)
                .subscriptionType(SubscriptionType.Failover).subscribe();

        for (int i = 0; i < 10; i++) {
            producer.send("test-msg".getBytes());
        }

        for (int i = 0; i < 10; i++) {
            Message<byte[]> msg = consumer.receive(10, TimeUnit.SECONDS);
            checkNotNull(msg);
            consumer.acknowledge(msg);
        }
<<<<<<< HEAD

        producer.close();
        consumer.close();
        client.close();
=======
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    private static PulsarClient getClientActiveConsumerChangeNotSupported(ClientConfigurationData conf)
            throws Exception {
        ThreadFactory threadFactory = new DefaultThreadFactory("pulsar-client-io", Thread.currentThread().isDaemon());
        EventLoopGroup eventLoopGroup = EventLoopUtil.newEventLoopGroup(conf.getNumIoThreads(), threadFactory);

        ConnectionPool cnxPool = new ConnectionPool(conf, eventLoopGroup, () -> {
            return new ClientCnx(conf, eventLoopGroup, ProtocolVersion.v11_VALUE) {
                @Override
                protected void handleActiveConsumerChange(CommandActiveConsumerChange change) {
                    throw new UnsupportedOperationException();
                }
            };
        });

        return new PulsarClientImpl(conf, eventLoopGroup, cnxPool);
    }

}
