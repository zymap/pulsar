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

import static org.testng.Assert.assertEquals;
<<<<<<< HEAD

import com.google.common.collect.Sets;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
=======
import static org.testng.Assert.assertNotNull;

import com.google.common.collect.Sets;

import io.netty.util.concurrent.DefaultThreadFactory;

import java.net.URL;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
>>>>>>> f773c602c... Test pr 10 (#27)
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

<<<<<<< HEAD
import org.apache.bookkeeper.test.PortManager;
=======
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
<<<<<<< HEAD
import org.apache.pulsar.client.api.MessageId;
=======
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
<<<<<<< HEAD
=======
import org.apache.pulsar.client.api.PulsarClientException;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
<<<<<<< HEAD
import org.apache.pulsar.common.util.FutureUtil;
=======
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.pulsar.zookeeper.ZookeeperServerTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicatorTestBase {
    URL url1;
    URL urlTls1;
    ServiceConfiguration config1 = new ServiceConfiguration();
    PulsarService pulsar1;
    BrokerService ns1;

    PulsarAdmin admin1;
    LocalBookkeeperEnsemble bkEnsemble1;

    URL url2;
    URL urlTls2;
    ServiceConfiguration config2 = new ServiceConfiguration();
    PulsarService pulsar2;
    BrokerService ns2;
    PulsarAdmin admin2;
    LocalBookkeeperEnsemble bkEnsemble2;

    URL url3;
    URL urlTls3;
    ServiceConfiguration config3 = new ServiceConfiguration();
    PulsarService pulsar3;
    BrokerService ns3;
    PulsarAdmin admin3;
    LocalBookkeeperEnsemble bkEnsemble3;

    ZookeeperServerTest globalZkS;

<<<<<<< HEAD
    ExecutorService executor = new ThreadPoolExecutor(5, 20, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
=======
    ExecutorService executor = new ThreadPoolExecutor(5, 20, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
            new DefaultThreadFactory("ReplicatorTestBase"));
>>>>>>> f773c602c... Test pr 10 (#27)

    static final int TIME_TO_CHECK_BACKLOG_QUOTA = 5;

    protected final static String TLS_SERVER_CERT_FILE_PATH = "./src/test/resources/certificate/server.crt";
    protected final static String TLS_SERVER_KEY_FILE_PATH = "./src/test/resources/certificate/server.key";

    // Default frequency
    public int getBrokerServicePurgeInactiveFrequency() {
        return 60;
    }

    public boolean isBrokerServicePurgeInactiveTopic() {
        return false;
    }

    void setup() throws Exception {
        log.info("--- Starting ReplicatorTestBase::setup ---");
<<<<<<< HEAD
        int globalZKPort = PortManager.nextFreePort();
        globalZkS = new ZookeeperServerTest(globalZKPort);
        globalZkS.start();

        // Start region 1
        int zkPort1 = PortManager.nextFreePort();
        bkEnsemble1 = new LocalBookkeeperEnsemble(3, zkPort1, () -> PortManager.nextFreePort());
        bkEnsemble1.start();

        int webServicePort1 = PortManager.nextFreePort();
        int webServicePortTls1 = PortManager.nextFreePort();

=======
        globalZkS = new ZookeeperServerTest(0);
        globalZkS.start();

        // Start region 1
        bkEnsemble1 = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble1.start();

>>>>>>> f773c602c... Test pr 10 (#27)
        // NOTE: we have to instantiate a new copy of System.getProperties() to make sure pulsar1 and pulsar2 have
        // completely
        // independent config objects instead of referring to the same properties object
        config1.setClusterName("r1");
        config1.setAdvertisedAddress("localhost");
<<<<<<< HEAD
        config1.setWebServicePort(webServicePort1);
        config1.setWebServicePortTls(webServicePortTls1);
        config1.setZookeeperServers("127.0.0.1:" + zkPort1);
        config1.setConfigurationStoreServers("127.0.0.1:" + globalZKPort + "/foo");
        config1.setBrokerDeleteInactiveTopicsEnabled(isBrokerServicePurgeInactiveTopic());
        config1.setBrokerServicePurgeInactiveFrequencyInSeconds(
                inSec(getBrokerServicePurgeInactiveFrequency(), TimeUnit.SECONDS));
        config1.setBrokerServicePort(PortManager.nextFreePort());
        config1.setBrokerServicePortTls(PortManager.nextFreePort());
=======
        config1.setWebServicePort(Optional.of(0));
        config1.setWebServicePortTls(Optional.of(0));
        config1.setZookeeperServers("127.0.0.1:" + bkEnsemble1.getZookeeperPort());
        config1.setConfigurationStoreServers("127.0.0.1:" + globalZkS.getZookeeperPort() + "/foo");
        config1.setBrokerDeleteInactiveTopicsEnabled(isBrokerServicePurgeInactiveTopic());
        config1.setBrokerDeleteInactiveTopicsFrequencySeconds(
                inSec(getBrokerServicePurgeInactiveFrequency(), TimeUnit.SECONDS));
        config1.setBrokerServicePort(Optional.of(0));
        config1.setBrokerServicePortTls(Optional.of(0));
>>>>>>> f773c602c... Test pr 10 (#27)
        config1.setTlsCertificateFilePath(TLS_SERVER_CERT_FILE_PATH);
        config1.setTlsKeyFilePath(TLS_SERVER_KEY_FILE_PATH);
        config1.setTlsTrustCertsFilePath(TLS_SERVER_CERT_FILE_PATH);
        config1.setBacklogQuotaCheckIntervalInSeconds(TIME_TO_CHECK_BACKLOG_QUOTA);
        config1.setDefaultNumberOfNamespaceBundles(1);
<<<<<<< HEAD
=======
        config1.setAllowAutoTopicCreationType("non-partitioned");
>>>>>>> f773c602c... Test pr 10 (#27)
        pulsar1 = new PulsarService(config1);
        pulsar1.start();
        ns1 = pulsar1.getBrokerService();

<<<<<<< HEAD
        url1 = new URL("http://localhost:" + webServicePort1);
        urlTls1 = new URL("https://localhost:" + webServicePortTls1);
=======
        url1 = new URL(pulsar1.getWebServiceAddress());
        urlTls1 = new URL(pulsar1.getWebServiceAddressTls());
>>>>>>> f773c602c... Test pr 10 (#27)
        admin1 = PulsarAdmin.builder().serviceHttpUrl(url1.toString()).build();

        // Start region 2

        // Start zk & bks
<<<<<<< HEAD
        int zkPort2 = PortManager.nextFreePort();
        bkEnsemble2 = new LocalBookkeeperEnsemble(3, zkPort2, () -> PortManager.nextFreePort());
        bkEnsemble2.start();

        int webServicePort2 = PortManager.nextFreePort();
        int webServicePortTls2 = PortManager.nextFreePort();
        config2.setClusterName("r2");
        config2.setAdvertisedAddress("localhost");
        config2.setWebServicePort(webServicePort2);
        config2.setWebServicePortTls(webServicePortTls2);
        config2.setZookeeperServers("127.0.0.1:" + zkPort2);
        config2.setConfigurationStoreServers("127.0.0.1:" + globalZKPort + "/foo");
        config2.setBrokerDeleteInactiveTopicsEnabled(isBrokerServicePurgeInactiveTopic());
        config2.setBrokerServicePurgeInactiveFrequencyInSeconds(
                inSec(getBrokerServicePurgeInactiveFrequency(), TimeUnit.SECONDS));
        config2.setBrokerServicePort(PortManager.nextFreePort());
        config2.setBrokerServicePortTls(PortManager.nextFreePort());
=======
        bkEnsemble2 = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble2.start();

        config2.setClusterName("r2");
        config2.setAdvertisedAddress("localhost");
        config2.setWebServicePort(Optional.of(0));
        config2.setWebServicePortTls(Optional.of(0));
        config2.setZookeeperServers("127.0.0.1:" + bkEnsemble2.getZookeeperPort());
        config2.setConfigurationStoreServers("127.0.0.1:" + globalZkS.getZookeeperPort() + "/foo");
        config2.setBrokerDeleteInactiveTopicsEnabled(isBrokerServicePurgeInactiveTopic());
        config2.setBrokerDeleteInactiveTopicsFrequencySeconds(
                inSec(getBrokerServicePurgeInactiveFrequency(), TimeUnit.SECONDS));
        config2.setBrokerServicePort(Optional.of(0));
        config2.setBrokerServicePortTls(Optional.of(0));
>>>>>>> f773c602c... Test pr 10 (#27)
        config2.setTlsCertificateFilePath(TLS_SERVER_CERT_FILE_PATH);
        config2.setTlsKeyFilePath(TLS_SERVER_KEY_FILE_PATH);
        config2.setTlsTrustCertsFilePath(TLS_SERVER_CERT_FILE_PATH);
        config2.setBacklogQuotaCheckIntervalInSeconds(TIME_TO_CHECK_BACKLOG_QUOTA);
        config2.setDefaultNumberOfNamespaceBundles(1);
<<<<<<< HEAD
=======
        config2.setAllowAutoTopicCreationType("non-partitioned");
>>>>>>> f773c602c... Test pr 10 (#27)
        pulsar2 = new PulsarService(config2);
        pulsar2.start();
        ns2 = pulsar2.getBrokerService();

<<<<<<< HEAD
        url2 = new URL("http://localhost:" + webServicePort2);
        urlTls2 = new URL("https://localhost:" + webServicePortTls2);
=======
        url2 = new URL(pulsar2.getWebServiceAddress());
        urlTls2 = new URL(pulsar2.getWebServiceAddressTls());
>>>>>>> f773c602c... Test pr 10 (#27)
        admin2 = PulsarAdmin.builder().serviceHttpUrl(url2.toString()).build();

        // Start region 3

        // Start zk & bks
<<<<<<< HEAD
        int zkPort3 = PortManager.nextFreePort();
        bkEnsemble3 = new LocalBookkeeperEnsemble(3, zkPort3, () -> PortManager.nextFreePort());
        bkEnsemble3.start();

        int webServicePort3 = PortManager.nextFreePort();
        int webServicePortTls3 = PortManager.nextFreePort();
        config3.setClusterName("r3");
        config3.setAdvertisedAddress("localhost");
        config3.setWebServicePort(webServicePort3);
        config3.setWebServicePortTls(webServicePortTls3);
        config3.setZookeeperServers("127.0.0.1:" + zkPort3);
        config3.setConfigurationStoreServers("127.0.0.1:" + globalZKPort + "/foo");
        config3.setBrokerDeleteInactiveTopicsEnabled(isBrokerServicePurgeInactiveTopic());
        config3.setBrokerServicePurgeInactiveFrequencyInSeconds(
                inSec(getBrokerServicePurgeInactiveFrequency(), TimeUnit.SECONDS));
        config3.setBrokerServicePort(PortManager.nextFreePort());
        config3.setBrokerServicePortTls(PortManager.nextFreePort());
=======
        bkEnsemble3 = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble3.start();

        config3.setClusterName("r3");
        config3.setAdvertisedAddress("localhost");
        config3.setWebServicePort(Optional.of(0));
        config3.setWebServicePortTls(Optional.of(0));
        config3.setZookeeperServers("127.0.0.1:" + bkEnsemble3.getZookeeperPort());
        config3.setConfigurationStoreServers("127.0.0.1:" + globalZkS.getZookeeperPort() + "/foo");
        config3.setBrokerDeleteInactiveTopicsEnabled(isBrokerServicePurgeInactiveTopic());
        config3.setBrokerDeleteInactiveTopicsFrequencySeconds(
                inSec(getBrokerServicePurgeInactiveFrequency(), TimeUnit.SECONDS));
        config3.setBrokerServicePort(Optional.of(0));
        config3.setBrokerServicePortTls(Optional.of(0));
>>>>>>> f773c602c... Test pr 10 (#27)
        config3.setTlsEnabled(true);
        config3.setTlsCertificateFilePath(TLS_SERVER_CERT_FILE_PATH);
        config3.setTlsKeyFilePath(TLS_SERVER_KEY_FILE_PATH);
        config3.setTlsTrustCertsFilePath(TLS_SERVER_CERT_FILE_PATH);
        config3.setDefaultNumberOfNamespaceBundles(1);
<<<<<<< HEAD
=======
        config3.setAllowAutoTopicCreationType("non-partitioned");
>>>>>>> f773c602c... Test pr 10 (#27)
        pulsar3 = new PulsarService(config3);
        pulsar3.start();
        ns3 = pulsar3.getBrokerService();

<<<<<<< HEAD
        url3 = new URL("http://localhost:" + webServicePort3);
        urlTls3 = new URL("https://localhost:" + webServicePortTls3);
=======
        url3 = new URL(pulsar3.getWebServiceAddress());
        urlTls3 = new URL(pulsar3.getWebServiceAddressTls());
>>>>>>> f773c602c... Test pr 10 (#27)
        admin3 = PulsarAdmin.builder().serviceHttpUrl(url3.toString()).build();

        // Provision the global namespace
        admin1.clusters().createCluster("r1", new ClusterData(url1.toString(), urlTls1.toString(),
<<<<<<< HEAD
                pulsar1.getBrokerServiceUrl(), pulsar1.getBrokerServiceUrlTls()));
        admin1.clusters().createCluster("r2", new ClusterData(url2.toString(), urlTls2.toString(),
                pulsar2.getBrokerServiceUrl(), pulsar2.getBrokerServiceUrlTls()));
        admin1.clusters().createCluster("r3", new ClusterData(url3.toString(), urlTls3.toString(),
                pulsar3.getBrokerServiceUrl(), pulsar3.getBrokerServiceUrlTls()));

        admin1.clusters().createCluster("global", new ClusterData("http://global:8080", "https://global:8443"));
        admin1.tenants().createTenant("pulsar",
                new TenantInfo(Sets.newHashSet("appid1", "appid2", "appid3"), Sets.newHashSet("r1", "r2", "r3")));
        admin1.namespaces().createNamespace("pulsar/ns");
        admin1.namespaces().setNamespaceReplicationClusters("pulsar/ns", Sets.newHashSet("r1", "r2", "r3"));
        admin1.namespaces().createNamespace("pulsar/ns1");
        admin1.namespaces().setNamespaceReplicationClusters("pulsar/ns1", Sets.newHashSet("r1", "r2"));
=======
                pulsar1.getSafeBrokerServiceUrl(), pulsar1.getBrokerServiceUrlTls()));
        admin1.clusters().createCluster("r2", new ClusterData(url2.toString(), urlTls2.toString(),
                pulsar2.getSafeBrokerServiceUrl(), pulsar2.getBrokerServiceUrlTls()));
        admin1.clusters().createCluster("r3", new ClusterData(url3.toString(), urlTls3.toString(),
                pulsar3.getSafeBrokerServiceUrl(), pulsar3.getBrokerServiceUrlTls()));

        admin1.tenants().createTenant("pulsar",
                new TenantInfo(Sets.newHashSet("appid1", "appid2", "appid3"), Sets.newHashSet("r1", "r2", "r3")));
        admin1.namespaces().createNamespace("pulsar/ns", Sets.newHashSet("r1", "r2", "r3"));
        admin1.namespaces().createNamespace("pulsar/ns1", Sets.newHashSet("r1", "r2"));
>>>>>>> f773c602c... Test pr 10 (#27)

        assertEquals(admin2.clusters().getCluster("r1").getServiceUrl(), url1.toString());
        assertEquals(admin2.clusters().getCluster("r2").getServiceUrl(), url2.toString());
        assertEquals(admin2.clusters().getCluster("r3").getServiceUrl(), url3.toString());
<<<<<<< HEAD
        assertEquals(admin2.clusters().getCluster("r1").getBrokerServiceUrl(), pulsar1.getBrokerServiceUrl());
        assertEquals(admin2.clusters().getCluster("r2").getBrokerServiceUrl(), pulsar2.getBrokerServiceUrl());
        assertEquals(admin2.clusters().getCluster("r3").getBrokerServiceUrl(), pulsar3.getBrokerServiceUrl());
=======
        assertEquals(admin2.clusters().getCluster("r1").getBrokerServiceUrl(), pulsar1.getSafeBrokerServiceUrl());
        assertEquals(admin2.clusters().getCluster("r2").getBrokerServiceUrl(), pulsar2.getSafeBrokerServiceUrl());
        assertEquals(admin2.clusters().getCluster("r3").getBrokerServiceUrl(), pulsar3.getSafeBrokerServiceUrl());

        // Also create V1 namespace for compatibility check
        admin1.clusters().createCluster("global", new ClusterData("http://global:8080", "https://global:8443"));
        admin1.namespaces().createNamespace("pulsar/global/ns");
        admin1.namespaces().setNamespaceReplicationClusters("pulsar/global/ns", Sets.newHashSet("r1", "r2", "r3"));
>>>>>>> f773c602c... Test pr 10 (#27)

        Thread.sleep(100);
        log.info("--- ReplicatorTestBase::setup completed ---");

    }

    private int inSec(int time, TimeUnit unit) {
        return (int) TimeUnit.SECONDS.convert(time, unit);
    }

    void shutdown() throws Exception {
        log.info("--- Shutting down ---");
        executor.shutdown();

        admin1.close();
        admin2.close();
        admin3.close();

        pulsar3.close();
        pulsar2.close();
        pulsar1.close();

        bkEnsemble1.stop();
        bkEnsemble2.stop();
        bkEnsemble3.stop();
        globalZkS.stop();
    }

<<<<<<< HEAD
    static class MessageProducer {
=======
    static class MessageProducer implements AutoCloseable {
>>>>>>> f773c602c... Test pr 10 (#27)
        URL url;
        String namespace;
        String topicName;
        PulsarClient client;
        Producer<byte[]> producer;

        MessageProducer(URL url, final TopicName dest) throws Exception {
            this.url = url;
            this.namespace = dest.getNamespace();
            this.topicName = dest.toString();
            client = PulsarClient.builder().serviceUrl(url.toString()).statsInterval(0, TimeUnit.SECONDS).build();
            producer = client.newProducer()
                .topic(topicName)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        }

        MessageProducer(URL url, final TopicName dest, boolean batch) throws Exception {
            this.url = url;
            this.namespace = dest.getNamespace();
            this.topicName = dest.toString();
            client = PulsarClient.builder().serviceUrl(url.toString()).statsInterval(0, TimeUnit.SECONDS).build();
            ProducerBuilder<byte[]> producerBuilder = client.newProducer()
                .topic(topicName)
                .enableBatching(batch)
                .batchingMaxPublishDelay(1, TimeUnit.SECONDS)
                .batchingMaxMessages(5);
            producer = producerBuilder.create();

        }

        void produceBatch(int messages) throws Exception {
            log.info("Start sending batch messages");
<<<<<<< HEAD
            List<CompletableFuture<MessageId>> futureList = new ArrayList<>();

            for (int i = 0; i < messages; i++) {
                futureList.add(producer.sendAsync(("test-" + i).getBytes()));
                log.info("queued message {}", ("test-" + i));
            }
            FutureUtil.waitForAll(futureList).get();
=======

            for (int i = 0; i < messages; i++) {
                producer.sendAsync(("test-" + i).getBytes());
                log.info("queued message {}", ("test-" + i));
            }
            producer.flush();
>>>>>>> f773c602c... Test pr 10 (#27)
        }

        void produce(int messages) throws Exception {

            log.info("Start sending messages");
            for (int i = 0; i < messages; i++) {
                producer.send(("test-" + i).getBytes());
                log.info("Sent message {}", ("test-" + i));
            }

        }

        TypedMessageBuilder<byte[]> newMessage() {
            return producer.newMessage();
        }

        void produce(int messages, TypedMessageBuilder<byte[]> messageBuilder) throws Exception {
            log.info("Start sending messages");
            for (int i = 0; i < messages; i++) {
<<<<<<< HEAD
                final String m = new String("test-builder-" + i);
=======
                final String m = new String("test-" + i);
>>>>>>> f773c602c... Test pr 10 (#27)
                messageBuilder.value(m.getBytes()).send();
                log.info("Sent message {}", m);
            }
        }

<<<<<<< HEAD
        void close() throws Exception {
            client.close();
=======
        public void close() {
            try {
                client.close();
            } catch (PulsarClientException e) {
                log.warn("Failed to close client", e);
            }
>>>>>>> f773c602c... Test pr 10 (#27)
        }

    }

<<<<<<< HEAD
    static class MessageConsumer {
=======
    static class MessageConsumer implements AutoCloseable {
>>>>>>> f773c602c... Test pr 10 (#27)
        final URL url;
        final String namespace;
        final String topicName;
        final PulsarClient client;
        final Consumer<byte[]> consumer;

        MessageConsumer(URL url, final TopicName dest) throws Exception {
            this(url, dest, "sub-id");
        }

        MessageConsumer(URL url, final TopicName dest, String subId) throws Exception {
            this.url = url;
            this.namespace = dest.getNamespace();
            this.topicName = dest.toString();

            client = PulsarClient.builder().serviceUrl(url.toString()).statsInterval(0, TimeUnit.SECONDS).build();

            try {
                consumer = client.newConsumer().topic(topicName).subscriptionName(subId).subscribe();
            } catch (Exception e) {
                client.close();
                throw e;
            }
        }

        void receive(int messages) throws Exception {
            log.info("Start receiving messages");
<<<<<<< HEAD
            Message<byte[]> msg = null;

            for (int i = 0; i < messages; i++) {
                msg = consumer.receive();
                consumer.acknowledge(msg);
                String msgData = new String(msg.getData());
                assertEquals(msgData, "test-" + i);
                log.info("Received message {}", msgData);
=======
            Message<byte[]> msg;

            Set<String> receivedMessages = new TreeSet<>();

            int i = 0;
            while (i < messages) {
                msg = consumer.receive(10, TimeUnit.SECONDS);
                assertNotNull(msg);
                consumer.acknowledge(msg);

                String msgData = new String(msg.getData());
                log.info("Received message {}", msgData);

                boolean added = receivedMessages.add(msgData);
                if (added) {
                    assertEquals(msgData, "test-" + i);
                    i++;
                } else {
                    log.info("Ignoring duplicate {}", msgData);
                }
>>>>>>> f773c602c... Test pr 10 (#27)
            }
        }

        boolean drained() throws Exception {
            return consumer.receive(0, TimeUnit.MICROSECONDS) == null;
        }

<<<<<<< HEAD
        void close() throws Exception {
            client.close();
=======
        public void close() {
            try {
                client.close();
            } catch (PulsarClientException e) {
                log.warn("Failed to close client", e);
            }
>>>>>>> f773c602c... Test pr 10 (#27)
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ReplicatorTestBase.class);
}
