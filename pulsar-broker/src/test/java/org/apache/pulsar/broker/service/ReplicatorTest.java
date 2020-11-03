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
package org.apache.pulsar.broker.service;

<<<<<<< HEAD
import static org.mockito.Matchers.eq;
=======
import static org.mockito.ArgumentMatchers.eq;
>>>>>>> f773c602c... Test pr 10 (#27)
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

<<<<<<< HEAD
=======
import com.google.common.collect.Sets;
import com.scurrilous.circe.checksum.Crc32cIntChecksum;

import io.netty.buffer.ByteBuf;

>>>>>>> f773c602c... Test pr 10 (#27)
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

<<<<<<< HEAD
=======
import lombok.Cleanup;

>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCursorCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.CursorAlreadyClosedException;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
<<<<<<< HEAD
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.namespace.OwnedBundle;
import org.apache.pulsar.broker.namespace.OwnershipCache;
import org.apache.pulsar.broker.service.BrokerServiceException.NamingException;
import org.apache.pulsar.broker.service.persistent.PersistentReplicator;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdminException.PreconditionFailedException;
=======
import org.apache.pulsar.broker.service.BrokerServiceException.NamingException;
import org.apache.pulsar.broker.service.persistent.PersistentReplicator;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.RawMessage;
import org.apache.pulsar.client.api.RawReader;
import org.apache.pulsar.client.api.Schema;
<<<<<<< HEAD
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.common.api.Commands;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
=======
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BacklogQuota.RetentionPolicy;
import org.apache.pulsar.common.policies.data.ReplicatorStats;
<<<<<<< HEAD
=======
import org.apache.pulsar.common.protocol.Commands;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

<<<<<<< HEAD
import com.google.common.collect.Sets;
import com.scurrilous.circe.checksum.Crc32cIntChecksum;

import io.netty.buffer.ByteBuf;

/**
 * Starts 2 brokers that are in 2 different clusters
=======
/**
 * Starts 3 brokers that are in 3 different clusters
>>>>>>> f773c602c... Test pr 10 (#27)
 */
public class ReplicatorTest extends ReplicatorTestBase {

    protected String methodName;

    @BeforeMethod
    public void beforeMethod(Method m) throws Exception {
        methodName = m.getName();
<<<<<<< HEAD
    }

    @Override
    @BeforeClass(timeOut = 30000)
=======
        admin1.namespaces().removeBacklogQuota("pulsar/ns");
        admin1.namespaces().removeBacklogQuota("pulsar/ns1");
        admin1.namespaces().removeBacklogQuota("pulsar/global/ns");
    }

    @Override
    @BeforeClass(timeOut = 300000)
>>>>>>> f773c602c... Test pr 10 (#27)
    void setup() throws Exception {
        super.setup();
    }

    @Override
<<<<<<< HEAD
    @AfterClass(timeOut = 30000)
=======
    @AfterClass(timeOut = 300000)
>>>>>>> f773c602c... Test pr 10 (#27)
    void shutdown() throws Exception {
        super.shutdown();
    }

    @DataProvider(name = "partitionedTopic")
    public Object[][] partitionedTopicProvider() {
        return new Object[][] { { Boolean.TRUE }, { Boolean.FALSE } };
    }

<<<<<<< HEAD
    @Test(enabled = true, timeOut = 30000)
=======
    @Test
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testConfigChange() throws Exception {
        log.info("--- Starting ReplicatorTest::testConfigChange ---");
        // This test is to verify that the config change on global namespace is successfully applied in broker during
        // runtime.
        // Run a set of producer tasks to create the topics
        List<Future<Void>> results = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
<<<<<<< HEAD
            final TopicName dest = TopicName.get(String.format("persistent://pulsar/ns/topic-%d", i));
=======
            final TopicName dest = TopicName.get(String
                    .format("persistent://pulsar/ns/topic-%d-%d", System.currentTimeMillis(), i));
>>>>>>> f773c602c... Test pr 10 (#27)

            results.add(executor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {

<<<<<<< HEAD
                    MessageProducer producer = new MessageProducer(url1, dest);
                    log.info("--- Starting producer --- " + url1);

=======
                    @Cleanup
                    MessageProducer producer = new MessageProducer(url1, dest);
                    log.info("--- Starting producer --- " + url1);

                    @Cleanup
>>>>>>> f773c602c... Test pr 10 (#27)
                    MessageConsumer consumer = new MessageConsumer(url1, dest);
                    log.info("--- Starting Consumer --- " + url1);

                    producer.produce(2);
<<<<<<< HEAD

                    consumer.receive(2);

                    producer.close();
                    consumer.close();
=======
                    consumer.receive(2);
>>>>>>> f773c602c... Test pr 10 (#27)
                    return null;
                }
            }));
        }

        for (Future<Void> result : results) {
            try {
                result.get();
            } catch (Exception e) {
                log.error("exception in getting future result ", e);
                fail(String.format("replication test failed with %s exception", e.getMessage()));
            }
        }

        Thread.sleep(1000L);
        // Make sure that the internal replicators map contains remote cluster info
        ConcurrentOpenHashMap<String, PulsarClient> replicationClients1 = ns1.getReplicationClients();
        ConcurrentOpenHashMap<String, PulsarClient> replicationClients2 = ns2.getReplicationClients();
        ConcurrentOpenHashMap<String, PulsarClient> replicationClients3 = ns3.getReplicationClients();

        Assert.assertNotNull(replicationClients1.get("r2"));
        Assert.assertNotNull(replicationClients1.get("r3"));
        Assert.assertNotNull(replicationClients2.get("r1"));
        Assert.assertNotNull(replicationClients2.get("r3"));
        Assert.assertNotNull(replicationClients3.get("r1"));
        Assert.assertNotNull(replicationClients3.get("r2"));

        // Case 1: Update the global namespace replication configuration to only contains the local cluster itself
        admin1.namespaces().setNamespaceReplicationClusters("pulsar/ns", Sets.newHashSet("r1"));

        // Wait for config changes to be updated.
        Thread.sleep(1000L);

        // Make sure that the internal replicators map still contains remote cluster info
        Assert.assertNotNull(replicationClients1.get("r2"));
        Assert.assertNotNull(replicationClients1.get("r3"));
        Assert.assertNotNull(replicationClients2.get("r1"));
        Assert.assertNotNull(replicationClients2.get("r3"));
        Assert.assertNotNull(replicationClients3.get("r1"));
        Assert.assertNotNull(replicationClients3.get("r2"));

        // Case 2: Update the configuration back
        admin1.namespaces().setNamespaceReplicationClusters("pulsar/ns", Sets.newHashSet("r1", "r2", "r3"));

        // Wait for config changes to be updated.
        Thread.sleep(1000L);

        // Make sure that the internal replicators map still contains remote cluster info
        Assert.assertNotNull(replicationClients1.get("r2"));
        Assert.assertNotNull(replicationClients1.get("r3"));
        Assert.assertNotNull(replicationClients2.get("r1"));
        Assert.assertNotNull(replicationClients2.get("r3"));
        Assert.assertNotNull(replicationClients3.get("r1"));
        Assert.assertNotNull(replicationClients3.get("r2"));

        // Case 3: TODO: Once automatic cleanup is implemented, add tests case to verify auto removal of clusters
    }

    @SuppressWarnings("unchecked")
    @Test(timeOut = 30000)
    public void testConcurrentReplicator() throws Exception {

        log.info("--- Starting ReplicatorTest::testConcurrentReplicator ---");

        final String namespace = "pulsar/concurrent";
        admin1.namespaces().createNamespace(namespace);
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2"));
<<<<<<< HEAD
        final TopicName topicName = TopicName.get(String.format("persistent://" + namespace + "/topic-%d", 0));
        PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString()).statsInterval(0, TimeUnit.SECONDS)
=======
        final TopicName topicName = TopicName
                .get(String.format("persistent://" + namespace + "/topic-%d-%d", System.currentTimeMillis(), 0));

        @Cleanup
        PulsarClient client1 = PulsarClient.builder()
                .serviceUrl(url1.toString()).statsInterval(0, TimeUnit.SECONDS)
>>>>>>> f773c602c... Test pr 10 (#27)
                .build();
        Producer<byte[]> producer = client1.newProducer().topic(topicName.toString())
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        producer.close();

<<<<<<< HEAD
        PersistentTopic topic = (PersistentTopic) pulsar1.getBrokerService().getOrCreateTopic(topicName.toString()).get();

        PulsarClientImpl pulsarClient = spy((PulsarClientImpl) pulsar1.getBrokerService().getReplicationClient("r3"));
=======
        PersistentTopic topic = (PersistentTopic) pulsar1.getBrokerService()
                .getOrCreateTopic(topicName.toString()).get();

        PulsarClientImpl pulsarClient = spy((PulsarClientImpl) pulsar1.getBrokerService()
                .getReplicationClient("r3"));
>>>>>>> f773c602c... Test pr 10 (#27)
        final Method startRepl = PersistentTopic.class.getDeclaredMethod("startReplicator", String.class);
        startRepl.setAccessible(true);

        Field replClientField = BrokerService.class.getDeclaredField("replicationClients");
        replClientField.setAccessible(true);
<<<<<<< HEAD
        ConcurrentOpenHashMap<String, PulsarClient> replicationClients = (ConcurrentOpenHashMap<String, PulsarClient>) replClientField
=======
        ConcurrentOpenHashMap<String, PulsarClient> replicationClients =
                (ConcurrentOpenHashMap<String, PulsarClient>) replClientField
>>>>>>> f773c602c... Test pr 10 (#27)
                .get(pulsar1.getBrokerService());
        replicationClients.put("r3", pulsarClient);

        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2", "r3"));
        ExecutorService executor = Executors.newFixedThreadPool(5);
        for (int i = 0; i < 5; i++) {
            executor.submit(() -> {
                try {
                    startRepl.invoke(topic, "r3");
                } catch (Exception e) {
                    fail("setting replicator failed", e);
                }
            });
        }
        Thread.sleep(3000);

<<<<<<< HEAD
        Mockito.verify(pulsarClient, Mockito.times(1)).createProducerAsync(Mockito.any(ProducerConfigurationData.class),
                Mockito.any(Schema.class), eq(null));

        client1.shutdown();
    }

    @Test(enabled = false, timeOut = 30000)
    public void testConfigChangeNegativeCases() throws Exception {
        log.info("--- Starting ReplicatorTest::testConfigChangeNegativeCases ---");
        // Negative test cases for global namespace config change. Verify that the namespace config change can not be
        // updated when the namespace is being unloaded.
        // Set up field access to internal namespace state in NamespaceService
        Field ownershipCacheField = NamespaceService.class.getDeclaredField("ownershipCache");
        ownershipCacheField.setAccessible(true);
        OwnershipCache ownerCache = (OwnershipCache) ownershipCacheField.get(pulsar1.getNamespaceService());
        Assert.assertNotNull(pulsar1, "pulsar1 is null");
        Assert.assertNotNull(pulsar1.getNamespaceService(), "pulsar1.getNamespaceService() is null");
        NamespaceBundle globalNsBundle = pulsar1.getNamespaceService().getNamespaceBundleFactory()
                .getFullBundle(NamespaceName.get("pulsar/ns"));
        ownerCache.tryAcquiringOwnership(globalNsBundle);
        Assert.assertNotNull(ownerCache.getOwnedBundle(globalNsBundle),
                "pulsar1.getNamespaceService().getOwnedServiceUnit(NamespaceName.get(\"pulsar/ns\")) is null");
        Field stateField = OwnedBundle.class.getDeclaredField("isActive");
        stateField.setAccessible(true);
        // set the namespace to be disabled
        ownerCache.disableOwnership(globalNsBundle);

        // Make sure the namespace config update failed
        try {
            admin1.namespaces().setNamespaceReplicationClusters("pulsar/ns", Sets.newHashSet("r1"));
            fail("Should have raised exception");
        } catch (PreconditionFailedException pfe) {
            // OK
        }

        // restore the namespace state
        ownerCache.removeOwnership(globalNsBundle).get();
        ownerCache.tryAcquiringOwnership(globalNsBundle);
    }

    @Test(enabled = true, timeOut = 30000)
    public void testReplication() throws Exception {

=======
        Mockito.verify(pulsarClient, Mockito.times(1))
                .createProducerAsync(
                        Mockito.any(ProducerConfigurationData.class),
                        Mockito.any(Schema.class), eq(null));

        executor.shutdown();
    }

    @DataProvider(name = "namespace")
    public Object[][] namespaceNameProvider() {
        return new Object[][] { { "pulsar/ns" }, { "pulsar/global/ns" } };
    }

    @Test(dataProvider = "namespace")
    public void testReplication(String namespace) throws Exception {
>>>>>>> f773c602c... Test pr 10 (#27)
        log.info("--- Starting ReplicatorTest::testReplication ---");

        // This test is to verify that the config change on global namespace is successfully applied in broker during
        // runtime.
        // Run a set of producer tasks to create the topics
<<<<<<< HEAD
        SortedSet<String> testDests = new TreeSet<String>();
        List<Future<Void>> results = Lists.newArrayList();
        for (int i = 0; i < 3; i++) {
            final TopicName dest = TopicName.get(String.format("persistent://pulsar/ns/repltopic-%d", i));
            testDests.add(dest.toString());

            results.add(executor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {

                    MessageProducer producer1 = new MessageProducer(url1, dest);
                    log.info("--- Starting producer --- " + url1);

                    MessageProducer producer2 = new MessageProducer(url2, dest);
                    log.info("--- Starting producer --- " + url2);

                    MessageProducer producer3 = new MessageProducer(url3, dest);
                    log.info("--- Starting producer --- " + url3);

                    MessageConsumer consumer1 = new MessageConsumer(url1, dest);
                    log.info("--- Starting Consumer --- " + url1);

                    MessageConsumer consumer2 = new MessageConsumer(url2, dest);
                    log.info("--- Starting Consumer --- " + url2);

                    MessageConsumer consumer3 = new MessageConsumer(url3, dest);
                    log.info("--- Starting Consumer --- " + url3);

                    // Produce from cluster1 and consume from the rest
                    producer1.produce(2);

                    consumer1.receive(2);

                    consumer2.receive(2);

                    consumer3.receive(2);

                    // Produce from cluster2 and consume from the rest
                    producer2.produce(2);

                    consumer1.receive(2);

                    consumer2.receive(2);

                    consumer3.receive(2);

                    // Produce from cluster3 and consume from the rest
                    producer3.produce(2);

                    consumer1.receive(2);

                    consumer2.receive(2);

                    consumer3.receive(2);

                    // Produce from cluster1&2 and consume from cluster3
                    producer1.produce(1);
                    producer2.produce(1);

                    consumer1.receive(1);

                    consumer2.receive(1);

                    consumer3.receive(1);

                    consumer1.receive(1);

                    consumer2.receive(1);

                    consumer3.receive(1);

                    producer1.close();
                    producer2.close();
                    producer3.close();
                    consumer1.close();
                    consumer2.close();
                    consumer3.close();
                    return null;
                }
            }));
        }

        for (Future<Void> result : results) {
            try {
                result.get();
            } catch (Exception e) {
                log.error("exception in getting future result ", e);
                fail(String.format("replication test failed with %s exception", e.getMessage()));
            }
        }
    }

    @Test(enabled = false, timeOut = 30000)
    public void testReplicationOverrides() throws Exception {

=======
        final TopicName dest = TopicName
                .get(String.format("persistent://%s/repltopic-%d", namespace, System.nanoTime()));

        @Cleanup
        MessageProducer producer1 = new MessageProducer(url1, dest);
        log.info("--- Starting producer --- " + url1);

        @Cleanup
        MessageProducer producer2 = new MessageProducer(url2, dest);
        log.info("--- Starting producer --- " + url2);

        @Cleanup
        MessageProducer producer3 = new MessageProducer(url3, dest);
        log.info("--- Starting producer --- " + url3);

        @Cleanup
        MessageConsumer consumer1 = new MessageConsumer(url1, dest);
        log.info("--- Starting Consumer --- " + url1);

        @Cleanup
        MessageConsumer consumer2 = new MessageConsumer(url2, dest);
        log.info("--- Starting Consumer --- " + url2);

        @Cleanup
        MessageConsumer consumer3 = new MessageConsumer(url3, dest);
        log.info("--- Starting Consumer --- " + url3);

        // Produce from cluster1 and consume from the rest
        producer1.produce(2);

        consumer1.receive(2);

        consumer2.receive(2);

        consumer3.receive(2);

        // Produce from cluster2 and consume from the rest
        producer2.produce(2);

        consumer1.receive(2);

        consumer2.receive(2);

        consumer3.receive(2);

        // Produce from cluster3 and consume from the rest
        producer3.produce(2);

        consumer1.receive(2);

        consumer2.receive(2);

        consumer3.receive(2);

        // Produce from cluster1&2 and consume from cluster3
        producer1.produce(1);
        producer2.produce(1);

        consumer1.receive(1);

        consumer2.receive(1);

        consumer3.receive(1);

        consumer1.receive(1);

        consumer2.receive(1);

        consumer3.receive(1);
    }

    @Test
    public void testReplicationOverrides() throws Exception {
>>>>>>> f773c602c... Test pr 10 (#27)
        log.info("--- Starting ReplicatorTest::testReplicationOverrides ---");

        // This test is to verify that the config change on global namespace is successfully applied in broker during
        // runtime.
        // Run a set of producer tasks to create the topics
<<<<<<< HEAD
        SortedSet<String> testDests = new TreeSet<String>();
        List<Future<Void>> results = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            final TopicName dest = TopicName.get(String.format("persistent://pulsar/ns/repltopic-%d", i));
            testDests.add(dest.toString());

            results.add(executor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {

                    MessageProducer producer1 = new MessageProducer(url1, dest);
                    log.info("--- Starting producer --- " + url1);

                    MessageProducer producer2 = new MessageProducer(url2, dest);
                    log.info("--- Starting producer --- " + url2);

                    MessageProducer producer3 = new MessageProducer(url3, dest);
                    log.info("--- Starting producer --- " + url3);

                    MessageConsumer consumer1 = new MessageConsumer(url1, dest);
                    log.info("--- Starting Consumer --- " + url1);

                    MessageConsumer consumer2 = new MessageConsumer(url2, dest);
                    log.info("--- Starting Consumer --- " + url2);

                    MessageConsumer consumer3 = new MessageConsumer(url3, dest);
                    log.info("--- Starting Consumer --- " + url3);

                    // Produce a message that isn't replicated
                    producer1.produce(1, producer1.newMessage().disableReplication());

                    // Produce a message not replicated to r2
                    producer1.produce(1, producer1.newMessage().replicationClusters(Lists.newArrayList("r1", "r3")));

                    // Produce a default replicated message
                    producer1.produce(1);

                    consumer1.receive(3);
                    consumer2.receive(1);
                    if (!consumer2.drained()) {
                        throw new Exception("consumer2 - unexpected message in queue");
                    }
                    consumer3.receive(2);
                    if (!consumer3.drained()) {
                        throw new Exception("consumer3 - unexpected message in queue");
                    }

                    producer1.close();
                    producer2.close();
                    producer3.close();
                    consumer1.close();
                    consumer2.close();
                    consumer3.close();
                    return null;
                }
            }));
        }

        for (Future<Void> result : results) {
            try {
                result.get();
            } catch (Exception e) {
                log.error("exception in getting future result ", e);
                fail(String.format("replication test failed with %s exception", e.getMessage()));
            }
        }
    }

    @Test(enabled = true, timeOut = 30000)
=======
        for (int i = 0; i < 10; i++) {
            final TopicName dest = TopicName
                    .get(String.format("persistent://pulsar/ns/repltopic-%d", System.nanoTime()));

            @Cleanup
            MessageProducer producer1 = new MessageProducer(url1, dest);
            log.info("--- Starting producer --- " + url1);

            @Cleanup
            MessageProducer producer2 = new MessageProducer(url2, dest);
            log.info("--- Starting producer --- " + url2);

            @Cleanup
            MessageProducer producer3 = new MessageProducer(url3, dest);
            log.info("--- Starting producer --- " + url3);

            @Cleanup
            MessageConsumer consumer1 = new MessageConsumer(url1, dest);
            log.info("--- Starting Consumer --- " + url1);

            @Cleanup
            MessageConsumer consumer2 = new MessageConsumer(url2, dest);
            log.info("--- Starting Consumer --- " + url2);

            @Cleanup
            MessageConsumer consumer3 = new MessageConsumer(url3, dest);
            log.info("--- Starting Consumer --- " + url3);

            // Produce a message that isn't replicated
            producer1.produce(1, producer1.newMessage().disableReplication());

            consumer1.receive(1);
            assertTrue(consumer2.drained());
            assertTrue(consumer3.drained());

            // Produce a message not replicated to r2
            producer1.produce(1, producer1.newMessage().replicationClusters(Lists.newArrayList("r1", "r3")));
            consumer1.receive(1);
            assertTrue(consumer2.drained());
            consumer3.receive(1);

            // Produce a default replicated message
            producer1.produce(1);

            consumer1.receive(1);
            consumer2.receive(1);
            consumer3.receive(1);

            assertTrue(consumer1.drained());
            assertTrue(consumer2.drained());
            assertTrue(consumer3.drained());
        }
    }

    @Test()
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testFailures() throws Exception {

        log.info("--- Starting ReplicatorTest::testFailures ---");

        try {
            // 1. Create a consumer using the reserved consumer id prefix "pulsar.repl."

<<<<<<< HEAD
            final TopicName dest = TopicName.get(String.format("persistent://pulsar/ns/res-cons-id"));
=======
            final TopicName dest = TopicName
                    .get(String.format("persistent://pulsar/ns/res-cons-id-%d", System.currentTimeMillis()));
>>>>>>> f773c602c... Test pr 10 (#27)

            // Create another consumer using replication prefix as sub id
            MessageConsumer consumer = new MessageConsumer(url2, dest, "pulsar.repl.");
            consumer.close();

        } catch (Exception e) {
            // SUCCESS
        }

    }

    @Test(timeOut = 30000)
    public void testReplicatePeekAndSkip() throws Exception {

<<<<<<< HEAD
        SortedSet<String> testDests = new TreeSet<String>();

        final TopicName dest = TopicName.get("persistent://pulsar/ns/peekAndSeekTopic");
        testDests.add(dest.toString());

        MessageProducer producer1 = new MessageProducer(url1, dest);
=======
        final TopicName dest = TopicName.get(
                String.format("persistent://pulsar/ns/peekAndSeekTopic-%d", System.currentTimeMillis()));

        @Cleanup
        MessageProducer producer1 = new MessageProducer(url1, dest);

        @Cleanup
>>>>>>> f773c602c... Test pr 10 (#27)
        MessageConsumer consumer1 = new MessageConsumer(url3, dest);

        // Produce from cluster1 and consume from the rest
        producer1.produce(2);
<<<<<<< HEAD
        producer1.close();
=======
>>>>>>> f773c602c... Test pr 10 (#27)
        PersistentTopic topic = (PersistentTopic) pulsar1.getBrokerService().getTopicReference(dest.toString()).get();
        PersistentReplicator replicator = (PersistentReplicator) topic.getReplicators()
                .get(topic.getReplicators().keys().get(0));
        replicator.skipMessages(2);
        CompletableFuture<Entry> result = replicator.peekNthMessage(1);
        Entry entry = result.get(50, TimeUnit.MILLISECONDS);
        assertNull(entry);
<<<<<<< HEAD
        consumer1.close();
=======
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Test(timeOut = 30000)
    public void testReplicatorClearBacklog() throws Exception {

        // This test is to verify that reset cursor fails on global topic
        SortedSet<String> testDests = new TreeSet<String>();

<<<<<<< HEAD
        final TopicName dest = TopicName.get("persistent://pulsar/ns/clearBacklogTopic");
        testDests.add(dest.toString());

        MessageProducer producer1 = new MessageProducer(url1, dest);
=======
        final TopicName dest = TopicName
                .get(String.format("persistent://pulsar/ns/clearBacklogTopic-%d", System.currentTimeMillis()));
        testDests.add(dest.toString());

        @Cleanup
        MessageProducer producer1 = new MessageProducer(url1, dest);

        @Cleanup
>>>>>>> f773c602c... Test pr 10 (#27)
        MessageConsumer consumer1 = new MessageConsumer(url3, dest);

        // Produce from cluster1 and consume from the rest
        producer1.produce(2);
<<<<<<< HEAD
        producer1.close();
=======
>>>>>>> f773c602c... Test pr 10 (#27)
        PersistentTopic topic = (PersistentTopic) pulsar1.getBrokerService().getTopicReference(dest.toString()).get();
        PersistentReplicator replicator = (PersistentReplicator) spy(
                topic.getReplicators().get(topic.getReplicators().keys().get(0)));
        replicator.readEntriesFailed(new ManagedLedgerException.InvalidCursorPositionException("failed"), null);
        replicator.clearBacklog().get();
        Thread.sleep(100);
        replicator.updateRates(); // for code-coverage
        replicator.expireMessages(1); // for code-coverage
        ReplicatorStats status = replicator.getStats();
<<<<<<< HEAD
        assertTrue(status.replicationBacklog == 0);
        consumer1.close();
=======
        assertEquals(status.replicationBacklog, 0);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Test(enabled = true, timeOut = 30000)
    public void testResetCursorNotFail() throws Exception {

        log.info("--- Starting ReplicatorTest::testResetCursorNotFail ---");

        // This test is to verify that reset cursor fails on global topic
<<<<<<< HEAD
        SortedSet<String> testDests = new TreeSet<String>();
        List<Future<Void>> results = Lists.newArrayList();
        for (int i = 0; i < 1; i++) {
            final TopicName dest = TopicName.get(String.format("persistent://pulsar/ns/resetrepltopic-%d", i));
            testDests.add(dest.toString());

            results.add(executor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {

                    MessageProducer producer1 = new MessageProducer(url1, dest);
                    log.info("--- Starting producer --- " + url1);

                    MessageConsumer consumer1 = new MessageConsumer(url1, dest);
                    log.info("--- Starting Consumer --- " + url1);

                    // Produce from cluster1 and consume from the rest
                    producer1.produce(2);

                    consumer1.receive(2);

                    producer1.close();
                    consumer1.close();
                    return null;
                }
            }));
        }

        for (Future<Void> result : results) {
            try {
                result.get();
            } catch (Exception e) {
                log.error("exception in getting future result ", e);
                fail(String.format("replication test failed with %s exception", e.getMessage()));
            }
        }
        admin1.topics().resetCursor(testDests.first(), "sub-id", System.currentTimeMillis());
    }

    @Test(enabled = true, timeOut = 30000)
    public void testReplicationForBatchMessages() throws Exception {

        log.info("--- Starting ReplicatorTest::testReplicationForBatchMessages ---");

        // Run a set of producer tasks to create the topics
        SortedSet<String> testDests = new TreeSet<String>();
        List<Future<Void>> results = Lists.newArrayList();
        for (int i = 0; i < 3; i++) {
            final TopicName dest = TopicName.get(String.format("persistent://pulsar/ns/repltopicbatch-%d", i));
            testDests.add(dest.toString());

            results.add(executor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {

                    MessageProducer producer1 = new MessageProducer(url1, dest, true);
                    log.info("--- Starting producer --- " + url1);

                    MessageProducer producer2 = new MessageProducer(url2, dest, true);
                    log.info("--- Starting producer --- " + url2);

                    MessageProducer producer3 = new MessageProducer(url3, dest, true);
                    log.info("--- Starting producer --- " + url3);

                    MessageConsumer consumer1 = new MessageConsumer(url1, dest);
                    log.info("--- Starting Consumer --- " + url1);

                    MessageConsumer consumer2 = new MessageConsumer(url2, dest);
                    log.info("--- Starting Consumer --- " + url2);

                    MessageConsumer consumer3 = new MessageConsumer(url3, dest);
                    log.info("--- Starting Consumer --- " + url3);

                    // Produce from cluster1 and consume from the rest
                    producer1.produceBatch(10);

                    consumer1.receive(10);

                    consumer2.receive(10);

                    consumer3.receive(10);

                    // Produce from cluster2 and consume from the rest
                    producer2.produceBatch(10);

                    consumer1.receive(10);

                    consumer2.receive(10);

                    consumer3.receive(10);

                    producer1.close();
                    producer2.close();
                    producer3.close();
                    consumer1.close();
                    consumer2.close();
                    consumer3.close();
                    return null;
                }
            }));
        }

        for (Future<Void> result : results) {
            try {
                result.get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                log.error("exception in getting future result ", e);
                fail(String.format("replication test failed with %s exception", e.getMessage()));
            }
        }
=======
        final TopicName dest = TopicName
                .get(String.format("persistent://pulsar/ns/resetrepltopic-%d", System.nanoTime()));

        @Cleanup
        MessageProducer producer1 = new MessageProducer(url1, dest);
        log.info("--- Starting producer --- " + url1);

        @Cleanup
        MessageConsumer consumer1 = new MessageConsumer(url1, dest);
        log.info("--- Starting Consumer --- " + url1);

        // Produce from cluster1 and consume from the rest
        producer1.produce(2);

        consumer1.receive(2);

        admin1.topics().resetCursor(dest.toString(), "sub-id", System.currentTimeMillis());
    }

    @Test
    public void testReplicationForBatchMessages() throws Exception {
        log.info("--- Starting ReplicatorTest::testReplicationForBatchMessages ---");

        // Run a set of producer tasks to create the topics
        final TopicName dest = TopicName
                .get(String.format("persistent://pulsar/ns/repltopicbatch-%d", System.nanoTime()));

        @Cleanup
        MessageProducer producer1 = new MessageProducer(url1, dest, true);
        log.info("--- Starting producer --- " + url1);

        @Cleanup
        MessageProducer producer2 = new MessageProducer(url2, dest, true);
        log.info("--- Starting producer --- " + url2);

        @Cleanup
        MessageProducer producer3 = new MessageProducer(url3, dest, true);
        log.info("--- Starting producer --- " + url3);

        @Cleanup
        MessageConsumer consumer1 = new MessageConsumer(url1, dest);
        log.info("--- Starting Consumer --- " + url1);

        @Cleanup
        MessageConsumer consumer2 = new MessageConsumer(url2, dest);
        log.info("--- Starting Consumer --- " + url2);

        @Cleanup
        MessageConsumer consumer3 = new MessageConsumer(url3, dest);
        log.info("--- Starting Consumer --- " + url3);

        // Produce from cluster1 and consume from the rest
        producer1.produceBatch(10);

        consumer1.receive(10);

        consumer2.receive(10);

        consumer3.receive(10);

        // Produce from cluster2 and consume from the rest
        producer2.produceBatch(10);

        consumer1.receive(10);

        consumer2.receive(10);

        consumer3.receive(10);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    /**
     * It verifies that: if it fails while removing replicator-cluster-cursor: it should not restart the replicator and
     * it should have cleaned up from the list
     *
     * @throws Exception
     */
    @Test(timeOut = 30000)
    public void testDeleteReplicatorFailure() throws Exception {
        log.info("--- Starting ReplicatorTest::testDeleteReplicatorFailure ---");
<<<<<<< HEAD
        final String topicName = "persistent://pulsar/ns/repltopicbatch";
        final TopicName dest = TopicName.get(topicName);
=======
        final String topicName = "persistent://pulsar/ns/repltopicbatch-" + System.currentTimeMillis() + "-";
        final TopicName dest = TopicName.get(topicName);

        @Cleanup
>>>>>>> f773c602c... Test pr 10 (#27)
        MessageProducer producer1 = new MessageProducer(url1, dest);
        PersistentTopic topic = (PersistentTopic) pulsar1.getBrokerService().getTopicReference(topicName).get();
        final String replicatorClusterName = topic.getReplicators().keys().get(0);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) topic.getManagedLedger();
        CountDownLatch latch = new CountDownLatch(1);
        // delete cursor already : so next time if topic.removeReplicator will get exception but then it should
        // remove-replicator from the list even with failure
        ledger.asyncDeleteCursor("pulsar.repl." + replicatorClusterName, new DeleteCursorCallback() {
            @Override
            public void deleteCursorComplete(Object ctx) {
                latch.countDown();
            }

            @Override
            public void deleteCursorFailed(ManagedLedgerException exception, Object ctx) {
                latch.countDown();
            }
        }, null);
        latch.await();

        Method removeReplicator = PersistentTopic.class.getDeclaredMethod("removeReplicator", String.class);
        removeReplicator.setAccessible(true);
        // invoke removeReplicator : it fails as cursor is not present: but still it should remove the replicator from
        // list without restarting it
        @SuppressWarnings("unchecked")
        CompletableFuture<Void> result = (CompletableFuture<Void>) removeReplicator.invoke(topic,
                replicatorClusterName);
        result.thenApply((v) -> {
            assertNull(topic.getPersistentReplicator(replicatorClusterName));
            return null;
        });
<<<<<<< HEAD

        producer1.close();
=======
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @SuppressWarnings("unchecked")
    @Test(priority = 5, timeOut = 30000)
    public void testReplicatorProducerClosing() throws Exception {
        log.info("--- Starting ReplicatorTest::testDeleteReplicatorFailure ---");
<<<<<<< HEAD
        final String topicName = "persistent://pulsar/ns/repltopicbatch";
        final TopicName dest = TopicName.get(topicName);
=======
        final String topicName = "persistent://pulsar/ns/repltopicbatch-" + System.currentTimeMillis() + "-";
        final TopicName dest = TopicName.get(topicName);

        @Cleanup
>>>>>>> f773c602c... Test pr 10 (#27)
        MessageProducer producer1 = new MessageProducer(url1, dest);
        PersistentTopic topic = (PersistentTopic) pulsar1.getBrokerService().getTopicReference(topicName).get();
        final String replicatorClusterName = topic.getReplicators().keys().get(0);
        Replicator replicator = topic.getPersistentReplicator(replicatorClusterName);
        pulsar2.close();
        pulsar3.close();
        replicator.disconnect(false);
        Thread.sleep(100);
        Field field = AbstractReplicator.class.getDeclaredField("producer");
        field.setAccessible(true);
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) field.get(replicator);
        assertNull(producer);
<<<<<<< HEAD
        producer1.close();
=======
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    /**
     * Issue #199
     *
     * It verifies that: if the remote cluster reaches backlog quota limit, replicator temporarily stops and once the
     * backlog drains it should resume replication.
     *
     * @throws Exception
     */

    @Test(timeOut = 60000, enabled = true, priority = -1)
    public void testResumptionAfterBacklogRelaxed() throws Exception {
        List<RetentionPolicy> policies = Lists.newArrayList();
        policies.add(RetentionPolicy.producer_exception);
        policies.add(RetentionPolicy.producer_request_hold);

        for (RetentionPolicy policy : policies) {
            // Use 1Mb quota by default
            admin1.namespaces().setBacklogQuota("pulsar/ns1", new BacklogQuota(1 * 1024 * 1024, policy));
            Thread.sleep(200);

            TopicName dest = TopicName
                    .get(String.format("persistent://pulsar/ns1/%s-%d", policy, System.currentTimeMillis()));

            // Producer on r1
<<<<<<< HEAD
            MessageProducer producer1 = new MessageProducer(url1, dest);

            // Consumer on r2
=======
            @Cleanup
            MessageProducer producer1 = new MessageProducer(url1, dest);

            // Consumer on r2
            @Cleanup
>>>>>>> f773c602c... Test pr 10 (#27)
            MessageConsumer consumer2 = new MessageConsumer(url2, dest);

            // Replicator for r1 -> r2
            PersistentTopic topic = (PersistentTopic) pulsar1.getBrokerService().getTopicReference(dest.toString()).get();
            Replicator replicator = topic.getPersistentReplicator("r2");

            // Produce 1 message in r1. This message will be replicated immediately into r2 and it will become part of
            // local backlog
            producer1.produce(1);

            Thread.sleep(500);

            // Restrict backlog quota limit to 1 byte to stop replication
            admin1.namespaces().setBacklogQuota("pulsar/ns1", new BacklogQuota(1, policy));

            Thread.sleep((TIME_TO_CHECK_BACKLOG_QUOTA + 1) * 1000);

            assertEquals(replicator.getStats().replicationBacklog, 0);

            // Next message will not be replicated, because r2 has reached the quota
            producer1.produce(1);

            Thread.sleep(500);

            assertEquals(replicator.getStats().replicationBacklog, 1);

            // Consumer will now drain 1 message and the replication backlog will be cleared
            consumer2.receive(1);

            // Wait until the 2nd message got delivered to consumer
            consumer2.receive(1);

            int retry = 10;
            for (int i = 0; i < retry && replicator.getStats().replicationBacklog > 0; i++) {
                if (i != retry - 1) {
                    Thread.sleep(100);
                }
            }

            assertEquals(replicator.getStats().replicationBacklog, 0);
<<<<<<< HEAD

            producer1.close();
            consumer2.close();
=======
>>>>>>> f773c602c... Test pr 10 (#27)
        }
    }

    /**
     * It verifies that PersistentReplicator considers CursorAlreadyClosedException as non-retriable-read exception and
     * it should closed the producer as cursor is already closed because replicator is already deleted.
     *
     * @throws Exception
     */
    @Test(timeOut = 15000)
    public void testCloseReplicatorStartProducer() throws Exception {
<<<<<<< HEAD
        TopicName dest = TopicName.get("persistent://pulsar/ns1/closeCursor");
        // Producer on r1
        MessageProducer producer1 = new MessageProducer(url1, dest);
        // Consumer on r1
        MessageConsumer consumer1 = new MessageConsumer(url1, dest);
        // Consumer on r2
=======
        TopicName dest = TopicName.get("persistent://pulsar/ns1/closeCursor-" + System.currentTimeMillis() + "-");
        // Producer on r1
        @Cleanup
        MessageProducer producer1 = new MessageProducer(url1, dest);
        // Consumer on r1
        @Cleanup
        MessageConsumer consumer1 = new MessageConsumer(url1, dest);
        // Consumer on r2
        @Cleanup
>>>>>>> f773c602c... Test pr 10 (#27)
        MessageConsumer consumer2 = new MessageConsumer(url2, dest);

        // Replicator for r1 -> r2
        PersistentTopic topic = (PersistentTopic) pulsar1.getBrokerService().getTopicReference(dest.toString()).get();
        PersistentReplicator replicator = (PersistentReplicator) topic.getPersistentReplicator("r2");

        // close the cursor
        Field cursorField = PersistentReplicator.class.getDeclaredField("cursor");
        cursorField.setAccessible(true);
        ManagedCursor cursor = (ManagedCursor) cursorField.get(replicator);
        cursor.close();
        // try to read entries
        producer1.produce(10);

        try {
            cursor.readEntriesOrWait(10);
            fail("It should have failed");
        } catch (Exception e) {
            assertEquals(e.getClass(), CursorAlreadyClosedException.class);
        }

        // replicator-readException: cursorAlreadyClosed
        replicator.readEntriesFailed(new CursorAlreadyClosedException("Cursor already closed exception"), null);

        // wait replicator producer to be closed
        Thread.sleep(100);

        // Replicator producer must be closed
        Field producerField = AbstractReplicator.class.getDeclaredField("producer");
        producerField.setAccessible(true);
        @SuppressWarnings("unchecked")
        ProducerImpl<byte[]> replicatorProducer = (ProducerImpl<byte[]>) producerField.get(replicator);
<<<<<<< HEAD
        assertEquals(replicatorProducer, null);

        producer1.close();
        consumer1.close();
        consumer2.close();
=======
        assertNull(replicatorProducer);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Test(timeOut = 30000)
    public void verifyChecksumAfterReplication() throws Exception {
<<<<<<< HEAD
        final String topicName = "persistent://pulsar/ns/checksumAfterReplication";
=======
        final String topicName = "persistent://pulsar/ns/checksumAfterReplication-" + System.currentTimeMillis() + "-";
>>>>>>> f773c602c... Test pr 10 (#27)

        PulsarClient c1 = PulsarClient.builder().serviceUrl(url1.toString()).build();
        Producer<byte[]> p1 = c1.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        PulsarClient c2 = PulsarClient.builder().serviceUrl(url2.toString()).build();
        RawReader reader2 = RawReader.create(c2, topicName, "sub").get();

        p1.send("Hello".getBytes());

        RawMessage msg = reader2.readNextAsync().get();

        ByteBuf b = msg.getHeadersAndPayload();

        assertTrue(Commands.hasChecksum(b));
        int parsedChecksum = Commands.readChecksum(b);
        int computedChecksum = Crc32cIntChecksum.computeChecksum(b);

        assertEquals(parsedChecksum, computedChecksum);

        p1.close();
        reader2.closeAsync().get();
    }

    /**
     * It verifies that broker should not start replicator for partitioned-topic (topic without -partition postfix)
     *
     * @param isPartitionedTopic
     * @throws Exception
     */
    @Test(dataProvider = "partitionedTopic")
    public void testReplicatorOnPartitionedTopic(boolean isPartitionedTopic) throws Exception {

        log.info("--- Starting ReplicatorTest::{} --- ", methodName);

        final String namespace = "pulsar/partitionedNs-" + isPartitionedTopic;
<<<<<<< HEAD
        final String persistentTopicName = "persistent://" + namespace + "/partTopic-" + isPartitionedTopic;
        final String nonPersistentTopicName = "non-persistent://" + namespace + "/partTopic-" + isPartitionedTopic;
=======
        final String persistentTopicName =
                "persistent://" + namespace + "/partTopic-" + System.currentTimeMillis() + "-" + isPartitionedTopic;
        final String nonPersistentTopicName =
                "non-persistent://" + namespace + "/partTopic-" + System.currentTimeMillis() + "-"+ isPartitionedTopic;
>>>>>>> f773c602c... Test pr 10 (#27)
        BrokerService brokerService = pulsar1.getBrokerService();

        admin1.namespaces().createNamespace(namespace);
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2", "r3"));

        if (isPartitionedTopic) {
            admin1.topics().createPartitionedTopic(persistentTopicName, 5);
            admin1.topics().createPartitionedTopic(nonPersistentTopicName, 5);
        }

        // load namespace with dummy topic on ns
        PulsarClient client = PulsarClient.builder().serviceUrl(url1.toString()).build();
        client.newProducer().topic("persistent://" + namespace + "/dummyTopic")
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        // persistent topic test
        try {
            brokerService.getOrCreateTopic(persistentTopicName).get();
            if (isPartitionedTopic) {
                fail("Topic creation fails with partitioned topic as replicator init fails");
            }
        } catch (Exception e) {
            if (!isPartitionedTopic) {
                fail("Topic creation should not fail without any partitioned topic");
            }
            assertTrue(e.getCause() instanceof NamingException);
        }

        // non-persistent topic test
        try {
            brokerService.getOrCreateTopic(nonPersistentTopicName).get();
            if (isPartitionedTopic) {
                fail("Topic creation fails with partitioned topic as replicator init fails");
            }
        } catch (Exception e) {
            if (!isPartitionedTopic) {
                fail("Topic creation should not fail without any partitioned topic");
            }
            assertTrue(e.getCause() instanceof NamingException);
        }

    }

<<<<<<< HEAD
=======
    @Test
    public void testReplicatedCluster() throws Exception {

        log.info("--- Starting ReplicatorTest::testReplicatedCluster ---");

        final String namespace = "pulsar/global/repl";
        final String topicName = String.format("persistent://%s/topic1-%d", namespace, System.currentTimeMillis());
        admin1.namespaces().createNamespace(namespace);
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2", "r3"));
        admin1.topics().createPartitionedTopic(topicName, 4);

        PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();
        PulsarClient client2 = PulsarClient.builder().serviceUrl(url2.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();

        Producer<byte[]> producer1 = client1.newProducer().topic(topicName).create();
        org.apache.pulsar.client.api.Consumer<byte[]> consumer1 = client1.newConsumer().topic(topicName).subscriptionName("s1").subscribe();
        org.apache.pulsar.client.api.Consumer<byte[]> consumer2 = client2.newConsumer().topic(topicName).subscriptionName("s1").subscribe();
        byte[] value = "test".getBytes();

        // publish message local only
        TypedMessageBuilder<byte[]> msg = producer1.newMessage().replicationClusters(Lists.newArrayList("r1")).value(value);
        msg.send();
        assertEquals(consumer1.receive().getValue(), value);

        Message<byte[]> msg2 = consumer2.receive(1, TimeUnit.SECONDS);
        if (msg2 != null) {
            fail("msg should have not been replicated to remote cluster");
        }

        consumer1.close();
        consumer2.close();
        producer1.close();

    }

    /**
     * This validates that broker supports update-partition api for global topics.
     * <pre>
     *  1. Create global topic with 4 partitions
     *  2. Update partition with 8 partitions
     *  3. Create producer on the partition topic which loads all new partitions
     *  4. Check subscriptions are created on all new partitions.
     * </pre>
     * @throws Exception
     */
    @Test
    public void testUpdateGlobalTopicPartition() throws Exception {
        log.info("--- Starting ReplicatorTest::testUpdateGlobalTopicPartition ---");

        final String cluster1 = pulsar1.getConfig().getClusterName();
        final String cluster2 = pulsar2.getConfig().getClusterName();
        final String namespace = "pulsar/ns-" + System.nanoTime();
        final String topicName = "persistent://" + namespace + "/topic1";
        int startPartitions = 4;
        int newPartitions = 8;
        final String subscriberName = "sub1";
        admin1.namespaces().createNamespace(namespace, Sets.newHashSet(cluster1, cluster2));
        admin1.topics().createPartitionedTopic(topicName, startPartitions);

        PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();
        PulsarClient client2 = PulsarClient.builder().serviceUrl(url2.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();

        Consumer<byte[]> consumer1 = client1.newConsumer().topic(topicName).subscriptionName(subscriberName)
                .subscribe();
        Consumer<byte[]> consumer2 = client2.newConsumer().topic(topicName).subscriptionName(subscriberName)
                .subscribe();

        admin1.topics().updatePartitionedTopic(topicName, newPartitions);

        assertEquals(admin1.topics().getPartitionedTopicMetadata(topicName).partitions, newPartitions);

        // create producers to load all the partition topics
        Producer<byte[]> producer1 = client1.newProducer().topic(topicName).create();
        Producer<byte[]> producer2 = client2.newProducer().topic(topicName).create();

        for (int i = startPartitions; i < newPartitions; i++) {
            String partitionedTopic = topicName + TopicName.PARTITIONED_TOPIC_SUFFIX + i;
            assertEquals(admin1.topics().getSubscriptions(partitionedTopic).size(), 1);
            assertEquals(admin2.topics().getSubscriptions(partitionedTopic).size(), 1);
        }

        producer1.close();
        producer2.close();
        consumer1.close();
        consumer2.close();

        client1.close();
        client2.close();
    }
>>>>>>> f773c602c... Test pr 10 (#27)
    private static final Logger log = LoggerFactory.getLogger(ReplicatorTest.class);

}
