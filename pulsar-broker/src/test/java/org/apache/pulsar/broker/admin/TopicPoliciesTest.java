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
package org.apache.pulsar.broker.admin;

import org.apache.pulsar.broker.service.PublishRateLimiter;
import org.apache.pulsar.broker.service.PublishRateLimiterImpl;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import static org.testng.Assert.assertEquals;

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.BacklogQuotaManager;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.DispatchRateLimiter;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.UUID;

@Slf4j
public class TopicPoliciesTest extends MockedPulsarServiceBaseTest {

    private final String testTenant = "my-tenant";

    private final String testNamespace = "my-namespace";

    private final String myNamespace = testTenant + "/" + testNamespace;

    private final String testTopic = "persistent://" + myNamespace + "/test-set-backlog-quota";

    private final String persistenceTopic = "persistent://" + myNamespace + "/test-set-persistence";

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        this.conf.setSystemTopicEnabled(true);
        this.conf.setTopicLevelPoliciesEnabled(true);
        super.internalSetup();

        admin.clusters().createCluster("test", new ClusterData(pulsar.getWebServiceAddress()));
        TenantInfo tenantInfo = new TenantInfo(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant(this.testTenant, tenantInfo);
        admin.namespaces().createNamespace(testTenant + "/" + testNamespace, Sets.newHashSet("test"));
        admin.topics().createPartitionedTopic(testTopic, 2);
        Producer producer = pulsarClient.newProducer().topic(testTopic).create();
        producer.close();
        Thread.sleep(3000);
    }

    @AfterMethod
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testSetBacklogQuota() throws Exception {

        BacklogQuota backlogQuota = new BacklogQuota(1024, BacklogQuota.RetentionPolicy.consumer_backlog_eviction);
        log.info("Backlog quota: {} will set to the topic: {}", backlogQuota, testTopic);

        admin.topics().setBacklogQuota(testTopic, backlogQuota);
        log.info("Backlog quota set success on topic: {}", testTopic);

        Thread.sleep(3000);
        BacklogQuota getBacklogQuota = admin.topics().getBacklogQuotaMap(testTopic)
                .get(BacklogQuota.BacklogQuotaType.destination_storage);
        log.info("Backlog quota {} get on topic: {}", getBacklogQuota, testTopic);
        Assert.assertEquals(getBacklogQuota, backlogQuota);

        BacklogQuotaManager backlogQuotaManager = pulsar.getBrokerService().getBacklogQuotaManager();
        BacklogQuota backlogQuotaInManager = backlogQuotaManager.getBacklogQuota(TopicName.get(testTopic));
        log.info("Backlog quota {} in backlog quota manager on topic: {}", backlogQuotaInManager, testTopic);
        Assert.assertEquals(backlogQuotaInManager, backlogQuota);

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testRemoveBacklogQuota() throws Exception {
        BacklogQuota backlogQuota = new BacklogQuota(1024, BacklogQuota.RetentionPolicy.consumer_backlog_eviction);
        log.info("Backlog quota: {} will set to the topic: {}", backlogQuota, testTopic);
        admin.topics().setBacklogQuota(testTopic, backlogQuota);
        log.info("Backlog quota set success on topic: {}", testTopic);

        Thread.sleep(3000);
        BacklogQuota getBacklogQuota = admin.topics().getBacklogQuotaMap(testTopic)
                .get(BacklogQuota.BacklogQuotaType.destination_storage);
        log.info("Backlog quota {} get on topic: {}", getBacklogQuota, testTopic);
        Assert.assertEquals(backlogQuota, getBacklogQuota);

        BacklogQuotaManager backlogQuotaManager = pulsar.getBrokerService().getBacklogQuotaManager();
        BacklogQuota backlogQuotaInManager = backlogQuotaManager.getBacklogQuota(TopicName.get(testTopic));
        log.info("Backlog quota {} in backlog quota manager on topic: {}", backlogQuotaInManager, testTopic);
        Assert.assertEquals(backlogQuota, backlogQuotaInManager);

        admin.topics().removeBacklogQuota(testTopic);
        getBacklogQuota = admin.topics().getBacklogQuotaMap(testTopic)
                .get(BacklogQuota.BacklogQuotaType.destination_storage);
        log.info("Backlog quota {} get on topic: {} after remove", getBacklogQuota, testTopic);
        Assert.assertNull(getBacklogQuota);

        backlogQuotaInManager = backlogQuotaManager.getBacklogQuota(TopicName.get(testTopic));
        log.info("Backlog quota {} in backlog quota manager on topic: {} after remove", backlogQuotaInManager,
                testTopic);
        Assert.assertEquals(backlogQuotaManager.getDefaultQuota(), backlogQuotaInManager);

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testCheckBacklogQuota() throws Exception {
        RetentionPolicies retentionPolicies = new RetentionPolicies(10, 10);
        String namespace = TopicName.get(testTopic).getNamespace();
        admin.namespaces().setRetention(namespace, retentionPolicies);

        BacklogQuota backlogQuota =
                new BacklogQuota(10 * 1024 * 1024, BacklogQuota.RetentionPolicy.consumer_backlog_eviction);
        log.info("Backlog quota: {} will set to the topic: {}", backlogQuota, testTopic);
        try {
            admin.topics().setBacklogQuota(testTopic, backlogQuota);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 412);
        }
        Thread.sleep(3000);
        backlogQuota =
                new BacklogQuota(10 * 1024 * 1024 + 1, BacklogQuota.RetentionPolicy.consumer_backlog_eviction);
        log.info("Backlog quota: {} will set to the topic: {}", backlogQuota, testTopic);
        try {
            admin.topics().setBacklogQuota(testTopic, backlogQuota);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 412);
        }
        Thread.sleep(3000);
        backlogQuota =
                new BacklogQuota(10 * 1024 * 1024 - 1, BacklogQuota.RetentionPolicy.consumer_backlog_eviction);
        log.info("Backlog quota: {} will set to the topic: {}", backlogQuota, testTopic);
        admin.topics().setBacklogQuota(testTopic, backlogQuota);
        Thread.sleep(3000);
        BacklogQuota getBacklogQuota = admin.topics().getBacklogQuotaMap(testTopic)
                .get(BacklogQuota.BacklogQuotaType.destination_storage);
        log.info("Backlog quota {} get on topic: {} after remove", getBacklogQuota, testTopic);
        Assert.assertEquals(getBacklogQuota, backlogQuota);

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testCheckRetention() throws Exception {
        BacklogQuota backlogQuota =
                new BacklogQuota(10 * 1024 * 1024, BacklogQuota.RetentionPolicy.consumer_backlog_eviction);
        admin.topics().setBacklogQuota(testTopic, backlogQuota);
        Thread.sleep(3000);

        RetentionPolicies retention = new RetentionPolicies(10, 10);
        log.info("Retention: {} will set to the topic: {}", retention, testTopic);
        try {
            admin.topics().setRetention(testTopic, retention);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 412);
        }

        retention = new RetentionPolicies(10, 9);
        log.info("Retention: {} will set to the topic: {}", retention, testTopic);
        try {
            admin.topics().setRetention(testTopic, retention);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 412);
        }

        Thread.sleep(3000);
        retention = new RetentionPolicies(10, 12);
        log.info("Backlog quota: {} will set to the topic: {}", backlogQuota, testTopic);
        admin.topics().setRetention(testTopic, retention);
        Thread.sleep(3000);
        RetentionPolicies getRetention = admin.topics().getRetention(testTopic);
        log.info("Backlog quota {} get on topic: {}", getRetention, testTopic);
        Assert.assertEquals(getRetention, retention);

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testSetRetention() throws Exception {
        RetentionPolicies retention = new RetentionPolicies(60, 1024);
        log.info("Retention: {} will set to the topic: {}", retention, testTopic);

        admin.topics().setRetention(testTopic, retention);
        log.info("Retention set success on topic: {}", testTopic);

        Thread.sleep(3000);
        RetentionPolicies getRetention = admin.topics().getRetention(testTopic);
        log.info("Retention {} get on topic: {}", getRetention, testTopic);
        Assert.assertEquals(getRetention, retention);

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testRemoveRetention() throws Exception {

        RetentionPolicies retention = new RetentionPolicies(60, 1024);
        log.info("Retention: {} will set to the topic: {}", retention, testTopic);

        admin.topics().setRetention(testTopic, retention);
        log.info("Retention set success on topic: {}", testTopic);

        Thread.sleep(3000);
        RetentionPolicies getRetention = admin.topics().getRetention(testTopic);
        log.info("Retention {} get on topic: {}", getRetention, testTopic);
        Assert.assertEquals(getRetention, retention);

        admin.topics().removeRetention(testTopic);
        Thread.sleep(3000);
        log.info("Retention {} get on topic: {} after remove", getRetention, testTopic);
        getRetention = admin.topics().getRetention(testTopic);
        Assert.assertNull(getRetention);

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testCheckPersistence() throws Exception {
        PersistencePolicies persistencePolicies = new PersistencePolicies(6, 2, 2, 0.0);
        log.info("PersistencePolicies: {} will set to the topic: {}", persistencePolicies, testTopic);
        try {
            admin.topics().setPersistence(testTopic, persistencePolicies);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 400);
        }

        persistencePolicies = new PersistencePolicies(2, 6, 2, 0.0);
        log.info("PersistencePolicies: {} will set to the topic: {}", persistencePolicies, testTopic);
        try {
            admin.topics().setPersistence(testTopic, persistencePolicies);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 400);
        }

        persistencePolicies = new PersistencePolicies(2, 2, 6, 0.0);
        log.info("PersistencePolicies: {} will set to the topic: {}", persistencePolicies, testTopic);
        try {
            admin.topics().setPersistence(testTopic, persistencePolicies);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 400);
        }

        persistencePolicies = new PersistencePolicies(1, 2, 2, 0.0);
        log.info("PersistencePolicies: {} will set to the topic: {}", persistencePolicies, testTopic);
        try {
            admin.topics().setPersistence(testTopic, persistencePolicies);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 400);
        }

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testSetPersistence() throws Exception {
        PersistencePolicies persistencePoliciesForNamespace = new PersistencePolicies(2, 2, 2, 0.3);
        admin.namespaces().setPersistence(myNamespace, persistencePoliciesForNamespace);

        PersistencePolicies persistencePolicies = new PersistencePolicies(3, 3, 3, 0.1);
        log.info("PersistencePolicies: {} will set to the topic: {}", persistencePolicies, persistenceTopic);

        admin.topics().setPersistence(persistenceTopic, persistencePolicies);
        Thread.sleep(3000);

        admin.topics().createPartitionedTopic(persistenceTopic, 2);
        Producer producer = pulsarClient.newProducer().topic(persistenceTopic).create();
        producer.close();

        admin.lookups().lookupTopic(persistenceTopic);
        Topic t = pulsar.getBrokerService().getOrCreateTopic(persistenceTopic).get();
        PersistentTopic persistentTopic = (PersistentTopic) t;
        ManagedLedgerConfig managedLedgerConfig = persistentTopic.getManagedLedger().getConfig();
        assertEquals(managedLedgerConfig.getEnsembleSize(), 3);
        assertEquals(managedLedgerConfig.getWriteQuorumSize(), 3);
        assertEquals(managedLedgerConfig.getAckQuorumSize(), 3);
        assertEquals(managedLedgerConfig.getThrottleMarkDelete(), 0.1);

        PersistencePolicies getPersistencePolicies = admin.topics().getPersistence(persistenceTopic);
        log.info("PersistencePolicies: {} will set to the topic: {}", persistencePolicies, persistenceTopic);
        Assert.assertEquals(getPersistencePolicies, persistencePolicies);

        admin.topics().deletePartitionedTopic(persistenceTopic, true);
        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testRemovePersistence() throws Exception {
        PersistencePolicies persistencePoliciesForNamespace = new PersistencePolicies(2, 2, 2, 0.3);
        admin.namespaces().setPersistence(myNamespace, persistencePoliciesForNamespace);

        PersistencePolicies persistencePolicies = new PersistencePolicies(3, 3, 3, 0.1);
        log.info("PersistencePolicies: {} will set to the topic: {}", persistencePolicies, persistenceTopic);

        admin.topics().setPersistence(persistenceTopic, persistencePolicies);
        Thread.sleep(3000);
        admin.topics().createPartitionedTopic(persistenceTopic, 2);
        Producer producer = pulsarClient.newProducer().topic(persistenceTopic).create();
        producer.close();

        PersistencePolicies getPersistencePolicies = admin.topics().getPersistence(persistenceTopic);
        log.info("PersistencePolicies {} get on topic: {}", getPersistencePolicies, persistenceTopic);
        Assert.assertEquals(getPersistencePolicies, persistencePolicies);

        admin.topics().removePersistence(persistenceTopic);
        Thread.sleep(3000);
        log.info("PersistencePolicies {} get on topic: {} after remove", getPersistencePolicies, testTopic);
        getPersistencePolicies = admin.topics().getPersistence(testTopic);
        Assert.assertNull(getPersistencePolicies);

        admin.lookups().lookupTopic(persistenceTopic);
        Topic t = pulsar.getBrokerService().getOrCreateTopic(persistenceTopic).get();
        PersistentTopic persistentTopic = (PersistentTopic) t;
        ManagedLedgerConfig managedLedgerConfig = persistentTopic.getManagedLedger().getConfig();
        assertEquals(managedLedgerConfig.getEnsembleSize(), 2);
        assertEquals(managedLedgerConfig.getWriteQuorumSize(), 2);
        assertEquals(managedLedgerConfig.getAckQuorumSize(), 2);
        assertEquals(managedLedgerConfig.getThrottleMarkDelete(), 0.3);

        admin.topics().deletePartitionedTopic(persistenceTopic, true);
        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testCheckMaxProducers() throws Exception {
        Integer maxProducers = new Integer(-1);
        log.info("MaxProducers: {} will set to the topic: {}", maxProducers, testTopic);
        try {
            admin.topics().setMaxProducers(testTopic, maxProducers);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 412);
        }

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testSetMaxProducers() throws Exception {
        admin.namespaces().setMaxProducersPerTopic(myNamespace, 1);
        log.info("MaxProducers: {} will set to the namespace: {}", 1, myNamespace);
        Integer maxProducers = 2;
        log.info("MaxProducers: {} will set to the topic: {}", maxProducers, persistenceTopic);
        admin.topics().setMaxProducers(persistenceTopic, maxProducers);
        Thread.sleep(3000);

        admin.topics().createPartitionedTopic(persistenceTopic, 2);
        Producer producer1 = null;
        Producer producer2 = null;
        Producer producer3 = null;
        try {
            producer1 = pulsarClient.newProducer().topic(persistenceTopic).create();
        } catch (PulsarClientException e) {
            Assert.fail();
        }
        try {
            producer2 = pulsarClient.newProducer().topic(persistenceTopic).create();
        } catch (PulsarClientException e) {
            Assert.fail();
        }
        try {
            producer3 = pulsarClient.newProducer().topic(persistenceTopic).create();
            Assert.fail();
        } catch (PulsarClientException e) {
            log.info("Topic reached max producers limit");
        }
        Assert.assertNotNull(producer1);
        Assert.assertNotNull(producer2);
        Assert.assertNull(producer3);
        producer1.close();
        producer2.close();

        Integer getMaxProducers = admin.topics().getMaxProducers(persistenceTopic);
        log.info("MaxProducers {} get on topic: {}", getMaxProducers, persistenceTopic);
        Assert.assertEquals(getMaxProducers, maxProducers);

        admin.topics().deletePartitionedTopic(persistenceTopic, true);
        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testRemoveMaxProducers() throws Exception {
        Integer maxProducers = 2;
        log.info("MaxProducers: {} will set to the topic: {}", maxProducers, persistenceTopic);
        admin.topics().setMaxProducers(persistenceTopic, maxProducers);
        Thread.sleep(3000);

        admin.topics().createPartitionedTopic(persistenceTopic, 2);
        Producer producer1 = null;
        Producer producer2 = null;
        Producer producer3 = null;
        Producer producer4 = null;
        try {
            producer1 = pulsarClient.newProducer().topic(persistenceTopic).create();
        } catch (PulsarClientException e) {
            Assert.fail();
        }
        try {
            producer2 = pulsarClient.newProducer().topic(persistenceTopic).create();
        } catch (PulsarClientException e) {
            Assert.fail();
        }
        try {
            producer3 = pulsarClient.newProducer().topic(persistenceTopic).create();
            Assert.fail();
        } catch (PulsarClientException e) {
            log.info("Topic reached max producers limit on topic level.");
        }
        Assert.assertNotNull(producer1);
        Assert.assertNotNull(producer2);
        Assert.assertNull(producer3);

        admin.topics().removeMaxProducers(persistenceTopic);
        Thread.sleep(3000);
        Integer getMaxProducers = admin.topics().getMaxProducers(testTopic);
        log.info("MaxProducers: {} get on topic: {} after remove", getMaxProducers, testTopic);
        Assert.assertNull(getMaxProducers);
        try {
            producer3 = pulsarClient.newProducer().topic(persistenceTopic).create();
        } catch (PulsarClientException e) {
            Assert.fail();
        }
        Assert.assertNotNull(producer3);
        admin.namespaces().setMaxProducersPerTopic(myNamespace, 3);
        log.info("MaxProducers: {} will set to the namespace: {}", 3, myNamespace);
        try {
            producer4 = pulsarClient.newProducer().topic(persistenceTopic).create();
            Assert.fail();
        } catch (PulsarClientException e) {
            log.info("Topic reached max producers limit on namespace level.");
        }
        Assert.assertNull(producer4);

        producer1.close();
        producer2.close();
        producer3.close();
        admin.topics().deletePartitionedTopic(persistenceTopic, true);
        admin.topics().deletePartitionedTopic(testTopic, true);
    }


    @Test
    public void testGetSetDispatchRate() throws Exception {
        DispatchRate dispatchRate = new DispatchRate(100, 10000, 1, true);
        log.info("Dispatch Rate: {} will set to the topic: {}", dispatchRate, testTopic);

        admin.topics().setDispatchRate(testTopic, dispatchRate);
        log.info("Dispatch Rate set success on topic: {}", testTopic);

        Thread.sleep(3000);
        DispatchRate getDispatchRate = admin.topics().getDispatchRate(testTopic);
        log.info("Dispatch Rate: {} get on topic: {}", getDispatchRate, testTopic);
        Assert.assertEquals(getDispatchRate, dispatchRate);

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testRemoveDispatchRate() throws Exception {
        DispatchRate dispatchRate = new DispatchRate(100, 10000, 1, true);
        log.info("Dispatch Rate: {} will set to the topic: {}", dispatchRate, testTopic);

        admin.topics().setDispatchRate(testTopic, dispatchRate);
        log.info("Dispatch Rate set success on topic: {}", testTopic);

        Thread.sleep(3000);
        DispatchRate getDispatchRate = admin.topics().getDispatchRate(testTopic);
        log.info("Dispatch Rate: {} get on topic: {}", getDispatchRate, testTopic);
        Assert.assertEquals(getDispatchRate, dispatchRate);

        admin.topics().removeDispatchRate(testTopic);
        Thread.sleep(3000);
        log.info("Dispatch Rate get on topic: {} after remove", getDispatchRate, testTopic);
        getDispatchRate = admin.topics().getDispatchRate(testTopic);
        Assert.assertNull(getDispatchRate);

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test(timeOut = 20000)
    public void testPolicyOverwrittenByNamespaceLevel() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topic);
        //wait for cache init
        Thread.sleep(2000);
        DispatchRate dispatchRate = new DispatchRate(200, 20000, 1, true);
        admin.namespaces().setDispatchRate(myNamespace, dispatchRate);
        //wait for zk
        Thread.sleep(2000);
        dispatchRate = new DispatchRate(100, 10000, 1, true);
        admin.topics().setDispatchRate(topic, dispatchRate);
        for (int i = 0; i < 10; i++) {
            if (admin.topics().getDispatchRate(topic) != null) {
                break;
            }
            Thread.sleep(500);
        }
        //1 Set ns level policy, topic level should not be overwritten
        dispatchRate = new DispatchRate(300, 30000, 2, true);
        admin.namespaces().setDispatchRate(myNamespace, dispatchRate);
        //wait for zk
        Thread.sleep(1000);
        DispatchRateLimiter limiter = pulsar.getBrokerService().getTopicIfExists(topic).get().get().getDispatchRateLimiter().get();
        Assert.assertEquals(limiter.getDispatchRateOnByte(), 10000);
        Assert.assertEquals(limiter.getDispatchRateOnMsg(), 100);
        admin.topics().removeDispatchRate(topic);
        for (int i = 0; i < 10; i++) {
            Thread.sleep(500);
            if (admin.topics().getDispatchRate(topic) == null) {
                break;
            }
        }
        //2 Remove level policy ,DispatchRateLimiter should us ns level policy
        limiter = pulsar.getBrokerService().getTopicIfExists(topic).get().get().getDispatchRateLimiter().get();
        Assert.assertEquals(limiter.getDispatchRateOnByte(), 30000);
        Assert.assertEquals(limiter.getDispatchRateOnMsg(), 300);
    }

    @Test(timeOut = 20000)
    public void testRestart() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topic);
        //wait for cache init
        Thread.sleep(1000);
        InactiveTopicPolicies inactiveTopicPolicies =
                new InactiveTopicPolicies(InactiveTopicDeleteMode.delete_when_subscriptions_caught_up,100,true);
        admin.namespaces().setInactiveTopicPolicies(myNamespace, inactiveTopicPolicies);
        //wait for zk
        Thread.sleep(500);
        inactiveTopicPolicies =
                new InactiveTopicPolicies(InactiveTopicDeleteMode.delete_when_no_subscriptions,200,false);
        admin.topics().setInactiveTopicPolicies(topic, inactiveTopicPolicies);
        for (int i = 0; i < 10; i++) {
            Thread.sleep(500);
            if (admin.topics().getInactiveTopicPolicies(topic) != null) {
                break;
            }
        }
        // restart broker, policy should still take effect
        stopBroker();
        Thread.sleep(500);
        startBroker();

        //wait for cache
        pulsarClient.newProducer().topic(topic).create().close();
        Thread.sleep(2000);
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topic).get().get();
        Assert.assertEquals(persistentTopic.getInactiveTopicPolicies(), inactiveTopicPolicies);
    }

    @Test
    public void testGetSetSubscriptionDispatchRate() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topic);
        Producer producer = pulsarClient.newProducer().topic(topic).create();
        producer.close();
        Thread.sleep(3000);

        DispatchRate dispatchRate = new DispatchRate(1000,
                1024 * 1024, 1);
        log.info("Subscription Dispatch Rate: {} will set to the topic: {}", dispatchRate, topic);

        admin.topics().setSubscriptionDispatchRate(topic, dispatchRate);
        log.info("Subscription dispatch rate set success on topic: {}", topic);

        Thread.sleep(3000);

        String subscriptionName = "test_subscription_rate";
        Consumer consumer = pulsarClient.newConsumer().subscriptionName(subscriptionName).topic(topic).subscribe();
        Thread.sleep(3000);

        DispatchRateLimiter dispatchRateLimiter = pulsar.getBrokerService().getTopicIfExists(topic)
                .get().get().getSubscription(subscriptionName).getDispatcher().getRateLimiter().get();
        Assert.assertNotNull(dispatchRateLimiter);
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnByte(), dispatchRate.dispatchThrottlingRateInByte);
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnMsg(), dispatchRate.dispatchThrottlingRateInMsg);


        DispatchRate getDispatchRate = admin.topics().getSubscriptionDispatchRate(topic);
        log.info("Subscription dispatch rate: {} get on topic: {}", getDispatchRate, topic);
        Assert.assertEquals(getDispatchRate, dispatchRate);

        producer.close();
        admin.topics().delete(topic, true);
    }

    @Test
    public void testRemoveSubscriptionDispatchRate() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topic);
        Producer producer = pulsarClient.newProducer().topic(topic).create();
        producer.close();
        Thread.sleep(3000);

        DispatchRate dispatchRate = new DispatchRate(1000,
                1024 * 1024, 1);
        log.info("Subscription Dispatch Rate: {} will set to the topic: {}", dispatchRate, topic);

        admin.topics().setSubscriptionDispatchRate(topic, dispatchRate);
        log.info("Subscription dispatch rate set success on topic: {}", topic);

        Thread.sleep(3000);

        String subscriptionName = "test_subscription_rate";
        Consumer consumer = pulsarClient.newConsumer().subscriptionName(subscriptionName).topic(topic).subscribe();
        Thread.sleep(3000);

        DispatchRateLimiter dispatchRateLimiter = pulsar.getBrokerService().getTopicIfExists(topic)
                .get().get().getSubscription(subscriptionName).getDispatcher().getRateLimiter().get();
        Assert.assertNotNull(dispatchRateLimiter);
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnByte(), dispatchRate.dispatchThrottlingRateInByte);
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnMsg(), dispatchRate.dispatchThrottlingRateInMsg);

        DispatchRate getDispatchRate = admin.topics().getSubscriptionDispatchRate(topic);
        log.info("Subscription dispatch rate: {} get on topic: {}", getDispatchRate, topic);

        // remove subscription dispatch rate
        admin.topics().removeSubscriptionDispatchRate(topic);
        Thread.sleep(3000);
        getDispatchRate = admin.topics().getSubscriptionDispatchRate(topic);
        log.info("Subscription dispatch rate get on topic is {} after remove", getDispatchRate);
        Assert.assertNull(getDispatchRate);

        dispatchRateLimiter = pulsar.getBrokerService().getTopicIfExists(topic)
                .get().get().getSubscription(subscriptionName).getDispatcher().getRateLimiter().get();
        Assert.assertNotEquals(dispatchRateLimiter.getDispatchRateOnMsg(), dispatchRate.dispatchThrottlingRateInMsg);
        Assert.assertNotEquals(dispatchRateLimiter.getDispatchRateOnByte(), dispatchRate.dispatchThrottlingRateInByte);

        producer.close();
        admin.topics().delete(topic, true);
    }

    @Test
    public void testSubscriptionDispatchRatePolicyOverwrittenNamespaceLevel() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topic);
        Producer producer = pulsarClient.newProducer().topic(topic).create();
        producer.close();
        Thread.sleep(3000);

        // set namespace level subscription dispatch rate
        DispatchRate namespaceDispatchRate = new DispatchRate(100, 1024 * 1024, 1);
        admin.namespaces().setSubscriptionDispatchRate(myNamespace, namespaceDispatchRate);
        Thread.sleep(3000);

        String subscriptionName = "test_subscription_rate";
        Consumer consumer = pulsarClient.newConsumer().subscriptionName(subscriptionName).topic(topic).subscribe();

        // get subscription dispatch Rate limiter
        DispatchRateLimiter dispatchRateLimiter = pulsar.getBrokerService().getTopicIfExists(topic)
                .get().get().getSubscription(subscriptionName).getDispatcher().getRateLimiter().get();
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnMsg(), namespaceDispatchRate.dispatchThrottlingRateInMsg);
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnByte(), namespaceDispatchRate.dispatchThrottlingRateInByte);

        // set topic level subscription dispatch rate
        DispatchRate topicDispatchRate = new DispatchRate(200, 2 * 1024 * 1024, 1);
        admin.topics().setSubscriptionDispatchRate(topic, topicDispatchRate);
        Thread.sleep(3000);

        // get subscription dispatch rate limiter
        dispatchRateLimiter = pulsar.getBrokerService().getTopicIfExists(topic).get().get()
                .getSubscription(subscriptionName).getDispatcher().getRateLimiter().get();
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnByte(), topicDispatchRate.dispatchThrottlingRateInByte);
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnMsg(), topicDispatchRate.dispatchThrottlingRateInMsg);

        // remove topic level subscription dispatch rate limiter
        admin.topics().removeSubscriptionDispatchRate(topic);
        Thread.sleep(3000);

        // get subscription dispatch rate limiter
        dispatchRateLimiter = pulsar.getBrokerService().getTopicIfExists(topic).get().get()
                .getSubscription(subscriptionName).getDispatcher().getRateLimiter().get();
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnByte(), namespaceDispatchRate.dispatchThrottlingRateInByte);
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnMsg(), namespaceDispatchRate.dispatchThrottlingRateInMsg);

        admin.topics().delete(topic, true);
    }

    @Test
    public void testGetSetCompactionThreshold() throws Exception {
        long compactionThreshold = 100000;
        log.info("Compaction threshold: {} will set to the topic: {}", compactionThreshold, testTopic);

        admin.topics().setCompactionThreshold(testTopic, compactionThreshold);
        log.info("Compaction threshold set success on topic: {}", testTopic);

        Thread.sleep(3000);
        long getCompactionThreshold = admin.topics().getCompactionThreshold(testTopic);
        log.info("Compaction threshold: {} get on topic: {}", getCompactionThreshold, testTopic);
        Assert.assertEquals(getCompactionThreshold, compactionThreshold);

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testRemoveCompactionThreshold() throws Exception {
        Long compactionThreshold = 100000L;
        log.info("Compaction threshold: {} will set to the topic: {}", compactionThreshold, testTopic);

        admin.topics().setCompactionThreshold(testTopic, compactionThreshold);
        log.info("Compaction threshold set success on topic: {}", testTopic);

        Thread.sleep(3000);
        Long getCompactionThreshold = admin.topics().getCompactionThreshold(testTopic);
        log.info("Compaction threshold: {} get on topic: {}", getCompactionThreshold, testTopic);
        Assert.assertEquals(getCompactionThreshold, compactionThreshold);

        admin.topics().removeCompactionThreshold(testTopic);
        Thread.sleep(3000);
        getCompactionThreshold = admin.topics().getCompactionThreshold(testTopic);
        log.info("Compaction threshold get on topic: {} after remove", getCompactionThreshold, testTopic);
        Assert.assertNull(getCompactionThreshold);

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testGetSetMaxConsumersPerSubscription() throws Exception {
        Integer maxConsumersPerSubscription = 10;
        log.info("MaxConsumersPerSubscription: {} will set to the topic: {}", maxConsumersPerSubscription, testTopic);

        admin.topics().setMaxConsumersPerSubscription(testTopic, maxConsumersPerSubscription);
        log.info("MaxConsumersPerSubscription set success on topic: {}", testTopic);

        Thread.sleep(3000);
        Integer getMaxConsumersPerSubscription = admin.topics().getMaxConsumersPerSubscription(testTopic);
        log.info("MaxConsumersPerSubscription: {} get on topic: {}", getMaxConsumersPerSubscription, testTopic);
        Assert.assertEquals(getMaxConsumersPerSubscription, maxConsumersPerSubscription);

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testRemoveMaxConsumersPerSubscription() throws Exception {
        Integer maxConsumersPerSubscription = 10;
        log.info("MaxConsumersPerSubscription: {} will set to the topic: {}", maxConsumersPerSubscription, testTopic);

        admin.topics().setMaxConsumersPerSubscription(testTopic, maxConsumersPerSubscription);
        log.info("MaxConsumersPerSubscription set success on topic: {}", testTopic);

        Thread.sleep(3000);
        Integer getMaxConsumersPerSubscription = admin.topics().getMaxConsumersPerSubscription(testTopic);
        log.info("MaxConsumersPerSubscription: {} get on topic: {}", getMaxConsumersPerSubscription, testTopic);
        Assert.assertEquals(getMaxConsumersPerSubscription, maxConsumersPerSubscription);

        admin.topics().removeMaxConsumersPerSubscription(testTopic);
        Thread.sleep(3000);
        getMaxConsumersPerSubscription = admin.topics().getMaxConsumersPerSubscription(testTopic);
        log.info("MaxConsumersPerSubscription get on topic: {} after remove", getMaxConsumersPerSubscription, testTopic);
        Assert.assertNull(getMaxConsumersPerSubscription);

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testGetSetPublishRate() throws Exception {
        PublishRate publishRate = new PublishRate(10000, 1024 * 1024 * 5);
        log.info("Publish Rate: {} will set to the topic: {}", publishRate, testTopic);

        admin.topics().setPublishRate(testTopic, publishRate);
        log.info("Publish Rate set success on topic: {}", testTopic);

        Thread.sleep(3000);
        PublishRate getPublishRate = admin.topics().getPublishRate(testTopic);
        log.info("Publish Rate: {} get on topic: {}", getPublishRate, testTopic);
        Assert.assertEquals(getPublishRate, publishRate);

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testRemovePublishRate() throws Exception {
        PublishRate publishRate = new PublishRate(10000, 1024 * 1024 * 5);
        log.info("Publish Rate: {} will set to the topic: {}", publishRate, testTopic);

        admin.topics().setPublishRate(testTopic, publishRate);
        log.info("Publish Rate set success on topic: {}", testTopic);

        Thread.sleep(3000);
        PublishRate getPublishRate = admin.topics().getPublishRate(testTopic);
        log.info("Publish Rate: {} get on topic: {}", getPublishRate, testTopic);
        Assert.assertEquals(getPublishRate, publishRate);

        admin.topics().removePublishRate(testTopic);
        Thread.sleep(3000);
        getPublishRate = admin.topics().getPublishRate(testTopic);
        log.info("Publish Rate get on topic: {} after remove", getPublishRate, testTopic);
        Assert.assertNull(getPublishRate);

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testCheckMaxConsumers() throws Exception {
        Integer maxProducers = new Integer(-1);
        log.info("MaxConsumers: {} will set to the topic: {}", maxProducers, testTopic);
        try {
            admin.topics().setMaxConsumers(testTopic, maxProducers);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 412);
        }

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testSetMaxConsumers() throws Exception {
        admin.namespaces().setMaxConsumersPerTopic(myNamespace, 1);
        log.info("MaxConsumers: {} will set to the namespace: {}", 1, myNamespace);
        Integer maxConsumers = 2;
        log.info("MaxConsumers: {} will set to the topic: {}", maxConsumers, persistenceTopic);
        admin.topics().setMaxConsumers(persistenceTopic, maxConsumers);
        Thread.sleep(3000);

        admin.topics().createPartitionedTopic(persistenceTopic, 2);
        Consumer consumer1 = null;
        Consumer consumer2 = null;
        Consumer consumer3 = null;
        try {
            consumer1 = pulsarClient.newConsumer().subscriptionName("sub1").topic(persistenceTopic).subscribe();
        } catch (PulsarClientException e) {
            Assert.fail();
        }
        try {
            consumer2 = pulsarClient.newConsumer().subscriptionName("sub2").topic(persistenceTopic).subscribe();
        } catch (PulsarClientException e) {
            Assert.fail();
        }
        try {
            consumer3 = pulsarClient.newConsumer().subscriptionName("sub3").topic(persistenceTopic).subscribe();
            Assert.fail();
        } catch (PulsarClientException e) {
            log.info("Topic reached max consumers limit");
        }
        Assert.assertNotNull(consumer1);
        Assert.assertNotNull(consumer2);
        Assert.assertNull(consumer3);
        consumer1.close();
        consumer2.close();

        Integer getMaxConsumers = admin.topics().getMaxConsumers(persistenceTopic);
        log.info("MaxConsumers {} get on topic: {}", getMaxConsumers, persistenceTopic);
        Assert.assertEquals(getMaxConsumers, maxConsumers);

        admin.topics().deletePartitionedTopic(persistenceTopic, true);
        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testRemoveMaxConsumers() throws Exception {
        Integer maxConsumers = 2;
        log.info("maxConsumers: {} will set to the topic: {}", maxConsumers, persistenceTopic);
        admin.topics().setMaxConsumers(persistenceTopic, maxConsumers);
        Thread.sleep(3000);

        admin.topics().createPartitionedTopic(persistenceTopic, 2);
        Consumer consumer1 = null;
        Consumer consumer2 = null;
        Consumer consumer3 = null;
        Consumer consumer4 = null;
        try {
            consumer1 = pulsarClient.newConsumer().subscriptionName("sub1").topic(persistenceTopic).subscribe();
        } catch (PulsarClientException e) {
            Assert.fail();
        }
        try {
            consumer2 = pulsarClient.newConsumer().subscriptionName("sub2").topic(persistenceTopic).subscribe();
        } catch (PulsarClientException e) {
            Assert.fail();
        }
        try {
            consumer3 = pulsarClient.newConsumer().subscriptionName("sub3").topic(persistenceTopic).subscribe();
            Assert.fail();
        } catch (PulsarClientException e) {
            log.info("Topic reached max consumers limit on topic level.");
        }
        Assert.assertNotNull(consumer1);
        Assert.assertNotNull(consumer2);
        Assert.assertNull(consumer3);

        admin.topics().removeMaxConsumers(persistenceTopic);
        Thread.sleep(3000);
        Integer getMaxConsumers = admin.topics().getMaxConsumers(testTopic);
        log.info("MaxConsumers: {} get on topic: {} after remove", getMaxConsumers, testTopic);
        Assert.assertNull(getMaxConsumers);
        try {
            consumer3 = pulsarClient.newConsumer().subscriptionName("sub3").topic(persistenceTopic).subscribe();
        } catch (PulsarClientException e) {
            Assert.fail();
        }
        Assert.assertNotNull(consumer3);
        admin.namespaces().setMaxConsumersPerTopic(myNamespace, 3);
        log.info("MaxConsumers: {} will set to the namespace: {}", 3, myNamespace);
        try {
            consumer4 = pulsarClient.newConsumer().subscriptionName("sub4").topic(persistenceTopic).subscribe();
            Assert.fail();
        } catch (PulsarClientException e) {
            log.info("Topic reached max consumers limit on namespace level.");
        }
        Assert.assertNull(consumer4);

        consumer1.close();
        consumer2.close();
        consumer3.close();
        admin.topics().deletePartitionedTopic(persistenceTopic, true);
        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testGetSetSubscribeRate() throws Exception {
        admin.topics().createPartitionedTopic(persistenceTopic, 2);
        Producer producer = pulsarClient.newProducer().topic(testTopic).create();
        producer.close();

        SubscribeRate subscribeRate = new SubscribeRate(1, 30);
        log.info("Subscribe Rate: {} will be set to the namespace: {}", subscribeRate, myNamespace);
        admin.namespaces().setSubscribeRate(myNamespace, subscribeRate);
        log.info("Subscribe Rate set success on namespace: {}", myNamespace);
        Thread.sleep(3000);

        subscribeRate =  new SubscribeRate(2, 30);
        log.info("Subscribe Rate: {} will set to the topic: {}", subscribeRate, persistenceTopic);
        admin.topics().setSubscribeRate(persistenceTopic, subscribeRate);
        log.info("Subscribe Rate set success on topic: {}", persistenceTopic);

        Thread.sleep(3000);

        PulsarClient pulsarClient1 = newPulsarClient(lookupUrl.toString(), 0);
        PulsarClient pulsarClient2 = newPulsarClient(lookupUrl.toString(), 0);
        PulsarClient pulsarClient3 = newPulsarClient(lookupUrl.toString(), 0);

        Consumer consumer1 = null;
        Consumer consumer2 = null;
        Consumer consumer3 = null;

        try {
            consumer1 = pulsarClient1.newConsumer().subscriptionName("sub1")
                    .topic(persistenceTopic).consumerName("test").subscribe();
            Assert.assertNotNull(consumer1);
            consumer1.close();
            pulsarClient1.shutdown();
        } catch (PulsarClientException e) {
            Assert.fail();
        }

        try {
            consumer2 = pulsarClient2.newConsumer().subscriptionName("sub1")
                    .topic(persistenceTopic).consumerName("test").subscribe();
            Assert.assertNotNull(consumer2);
            consumer2.close();
            pulsarClient2.shutdown();
        } catch (PulsarClientException e) {
            Assert.fail();
        }

        try {
            consumer3 = pulsarClient3.newConsumer().subscriptionName("sub1")
                    .topic(persistenceTopic).consumerName("test").subscribe();
            Assert.fail();
        } catch (PulsarClientException e) {
            log.info("subscribe rate reached max subscribe rate limit");
        }

        Assert.assertNull(consumer3);
        pulsarClient3.shutdown();

        SubscribeRate getSubscribeRate = admin.topics().getSubscribeRate(persistenceTopic);
        log.info("Subscribe Rate: {} get on topic: {}", getSubscribeRate, persistenceTopic);
        Assert.assertEquals(getSubscribeRate, subscribeRate);

        admin.topics().deletePartitionedTopic(testTopic, true);
        admin.topics().deletePartitionedTopic(persistenceTopic, true);
    }

    // @Test
    public void testRemoveSubscribeRate() throws Exception {
        admin.topics().createPartitionedTopic(persistenceTopic, 2);
        Producer producer = pulsarClient.newProducer().topic(testTopic).create();
        producer.close();

        SubscribeRate subscribeRate = new SubscribeRate(2, 30);
        log.info("Subscribe Rate: {} will set to the topic: {}", subscribeRate, persistenceTopic);
        admin.topics().setSubscribeRate(persistenceTopic, subscribeRate);
        log.info("Subscribe Rate set success on topic: {}", persistenceTopic);

        Thread.sleep(3000);

        PulsarClient pulsarClient1 = newPulsarClient(lookupUrl.toString(), 0);
        PulsarClient pulsarClient2 = newPulsarClient(lookupUrl.toString(), 0);
        PulsarClient pulsarClient3 = newPulsarClient(lookupUrl.toString(), 0);

        Consumer consumer1 = null;
        Consumer consumer2 = null;
        Consumer consumer3 = null;

        try {
            consumer1 = pulsarClient1.newConsumer().subscriptionName("sub1")
                    .topic(persistenceTopic).consumerName("test").subscribe();
            Assert.assertNotNull(consumer1);
            consumer1.close();
            pulsarClient1.shutdown();
        } catch (PulsarClientException e) {
            Assert.fail();
        }

        try {
            consumer2 = pulsarClient2.newConsumer().subscriptionName("sub1")
                    .topic(persistenceTopic).consumerName("test").subscribe();
            Assert.assertNotNull(consumer2);
            consumer2.close();
            pulsarClient2.shutdown();
        } catch (PulsarClientException e) {
            Assert.fail();
        }

        try {
            consumer3 = pulsarClient3.newConsumer().subscriptionName("sub1")
                    .topic(persistenceTopic).consumerName("test").subscribe();
            Assert.fail();
        } catch (PulsarClientException e) {
            log.info("subscribe rate reached max subscribe rate limit");
        }
        Assert.assertNull(consumer3);

        SubscribeRate getSubscribeRate = admin.topics().getSubscribeRate(persistenceTopic);
        log.info("Subscribe Rate: {} get on topic: {}", getSubscribeRate, persistenceTopic);
        Assert.assertEquals(getSubscribeRate, subscribeRate);

        admin.topics().removeSubscribeRate(persistenceTopic);
        Thread.sleep(3000);
        log.info("Subscribe Rate get on topic: {} after remove", getSubscribeRate, persistenceTopic);
        getSubscribeRate = admin.topics().getSubscribeRate(persistenceTopic);
        Assert.assertNull(getSubscribeRate);

        PulsarClient pulsarClient4 = newPulsarClient(lookupUrl.toString(), 0);
        PulsarClient pulsarClient5 = newPulsarClient(lookupUrl.toString(), 0);
        PulsarClient pulsarClient6 = newPulsarClient(lookupUrl.toString(), 0);

        Consumer consumer4 = null;
        Consumer consumer5 = null;
        Consumer consumer6 = null;

        try {
            consumer3 = pulsarClient3.newConsumer().subscriptionName("sub2")
                    .topic(persistenceTopic).consumerName("test").subscribe();
            Assert.assertNotNull(consumer3);
            consumer3.close();
            pulsarClient3.shutdown();
        } catch (PulsarClientException e) {
            Assert.fail();
        }

        try {
            consumer4 = pulsarClient4.newConsumer().subscriptionName("sub2")
                    .topic(persistenceTopic).consumerName("test").subscribe();
            Assert.assertNotNull(consumer4);
            consumer4.close();
            pulsarClient4.shutdown();
        } catch (PulsarClientException e) {
            Assert.fail();
        }
        try {
            consumer5 = pulsarClient5.newConsumer().subscriptionName("sub2")
                    .topic(persistenceTopic).consumerName("test").subscribe();
            Assert.assertNotNull(consumer5);
            consumer5.close();
            pulsarClient5.shutdown();
        } catch (PulsarClientException e) {
            Assert.fail();
        }

        try {
            consumer6 = pulsarClient6.newConsumer().subscriptionName("sub2")
                    .topic(persistenceTopic).consumerName("test").subscribe();
            Assert.assertNotNull(consumer6);
            consumer6.close();
            pulsarClient6.shutdown();
        } catch (PulsarClientException e) {
            Assert.fail();
        }

        admin.topics().deletePartitionedTopic(persistenceTopic, true);
        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testPublishRateInDifferentLevelPolicy() throws Exception {
        cleanup();
        conf.setMaxPublishRatePerTopicInMessages(5);
        conf.setMaxPublishRatePerTopicInBytes(50L);
        setup();
        //wait for cache init
        Thread.sleep(3000);
        final String topicName = "persistent://" + myNamespace + "/test-" + UUID.randomUUID();
        pulsarClient.newProducer().topic(topicName).create().close();
        Field publishMaxMessageRate = PublishRateLimiterImpl.class.getDeclaredField("publishMaxMessageRate");
        publishMaxMessageRate.setAccessible(true);
        Field publishMaxByteRate = PublishRateLimiterImpl.class.getDeclaredField("publishMaxByteRate");
        publishMaxByteRate.setAccessible(true);

        //1 use broker-level policy by default
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topicName).get().get();
        PublishRateLimiterImpl publishRateLimiter = (PublishRateLimiterImpl) topic.getTopicPublishRateLimiter();
        Assert.assertEquals(publishMaxMessageRate.get(publishRateLimiter), 5);
        Assert.assertEquals(publishMaxByteRate.get(publishRateLimiter), 50L);

        //2 set namespace-level policy
        PublishRate publishMsgRate = new PublishRate(10, 100L);
        admin.namespaces().setPublishRate(myNamespace, publishMsgRate);
        retryStrategically((x) -> {
            PublishRateLimiterImpl limiter = (PublishRateLimiterImpl) topic.getTopicPublishRateLimiter();
            try {
                return (int)publishMaxMessageRate.get(limiter) == 10;
            } catch (Exception e) {
                return false;
            }
        }, 5, 200);
        publishRateLimiter = (PublishRateLimiterImpl) topic.getTopicPublishRateLimiter();
        Assert.assertEquals(publishMaxMessageRate.get(publishRateLimiter), 10);
        Assert.assertEquals(publishMaxByteRate.get(publishRateLimiter), 100L);

        //3 set topic-level policy, namespace-level policy should be overwritten
        PublishRate publishMsgRate2 = new PublishRate(11, 101L);
        admin.topics().setPublishRate(topicName, publishMsgRate2);
        retryStrategically((x) -> {
            try {
                return admin.topics().getPublishRate(topicName) != null;
            } catch (Exception e) {
                return false;
            }
        }, 5, 200);
        publishRateLimiter = (PublishRateLimiterImpl) topic.getTopicPublishRateLimiter();
        Assert.assertEquals(publishMaxMessageRate.get(publishRateLimiter), 11);
        Assert.assertEquals(publishMaxByteRate.get(publishRateLimiter), 101L);

        //4 remove topic-level policy, namespace-level policy will take effect
        admin.topics().removePublishRate(topicName);
        retryStrategically((x) -> {
            try {
                return admin.topics().getPublishRate(topicName) == null;
            } catch (Exception e) {
                return false;
            }
        }, 5, 200);
        publishRateLimiter = (PublishRateLimiterImpl) topic.getTopicPublishRateLimiter();
        Assert.assertEquals(publishMaxMessageRate.get(publishRateLimiter), 10);
        Assert.assertEquals(publishMaxByteRate.get(publishRateLimiter), 100L);

        //5 remove namespace-level policy, broker-level policy will take effect
        admin.namespaces().removePublishRate(myNamespace);
        retryStrategically((x) -> {
            PublishRateLimiterImpl limiter = (PublishRateLimiterImpl) topic.getTopicPublishRateLimiter();
            try {
                return (int)publishMaxMessageRate.get(limiter) == 5;
            } catch (Exception e) {
                return false;
            }
        }, 5, 200);
        publishRateLimiter = (PublishRateLimiterImpl) topic.getTopicPublishRateLimiter();
        Assert.assertEquals(publishMaxMessageRate.get(publishRateLimiter), 5);
        Assert.assertEquals(publishMaxByteRate.get(publishRateLimiter), 50L);
    }

}
