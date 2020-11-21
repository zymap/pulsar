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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.Sets;

import java.lang.reflect.Field;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.transaction.TransactionTestBase;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClient;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.apache.pulsar.client.internal.DefaultImplementation;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * End to end transaction test.
 */
@Slf4j
public class TransactionEndToEndTest extends TransactionTestBase {

    private final static int TOPIC_PARTITION = 3;

    private final static String TENANT = "tnx";
    private final static String NAMESPACE1 = TENANT + "/ns1";
    private final static String TOPIC_OUTPUT = NAMESPACE1 + "/output";
    private final static String TOPIC_MESSAGE_ACK_TEST = NAMESPACE1 + "/message-ack-test";

    @BeforeMethod
    protected void setup() throws Exception {
        internalSetup();

        String[] brokerServiceUrlArr = getPulsarServiceList().get(0).getBrokerServiceUrl().split(":");
        String webServicePort = brokerServiceUrlArr[brokerServiceUrlArr.length -1];
        admin.clusters().createCluster(CLUSTER_NAME, new ClusterData("http://localhost:" + webServicePort));
        admin.tenants().createTenant(TENANT,
                new TenantInfo(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NAMESPACE1);
        admin.topics().createPartitionedTopic(TOPIC_OUTPUT, TOPIC_PARTITION);
        admin.topics().createPartitionedTopic(TOPIC_MESSAGE_ACK_TEST, TOPIC_PARTITION);

        admin.tenants().createTenant(NamespaceName.SYSTEM_NAMESPACE.getTenant(),
                new TenantInfo(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NamespaceName.SYSTEM_NAMESPACE.toString());
        admin.topics().createPartitionedTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString(), 16);

        pulsarClient = PulsarClient.builder()
                .serviceUrl(getPulsarServiceList().get(0).getBrokerServiceUrl())
                .statsInterval(0, TimeUnit.SECONDS)
                .enableTransaction(true)
                .build();

        Thread.sleep(1000 * 3);
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanup() {
        super.internalCleanup();
    }

    @Test
    public void noBatchProduceCommitTest() throws Exception {
        produceCommitTest(false);
    }

    @Test
    public void batchProduceCommitTest() throws Exception {
        produceCommitTest(true);
    }

    private void produceCommitTest(boolean enableBatch) throws Exception {
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient
                .newConsumer()
                .topic(TOPIC_OUTPUT)
                .subscriptionName("test")
                .enableBatchIndexAcknowledgment(true)
                .subscribe();

        ProducerBuilder<byte[]> producerBuilder = pulsarClient
                .newProducer()
                .topic(TOPIC_OUTPUT)
                .enableBatching(enableBatch)
                .sendTimeout(0, TimeUnit.SECONDS);
        @Cleanup
        Producer<byte[]> producer = producerBuilder.create();

        Transaction txn1 = getTxn();
        Transaction txn2 = getTxn();

        int txn1MessageCnt = 0;
        int txn2MessageCnt = 0;
        int messageCnt = 1000;
        for (int i = 0; i < messageCnt; i++) {
            if (i % 5 == 0) {
                producer.newMessage(txn1).value(("Hello Txn - " + i).getBytes(UTF_8)).send();
                txn1MessageCnt ++;
            } else {
                producer.newMessage(txn2).value(("Hello Txn - " + i).getBytes(UTF_8)).sendAsync();
                txn2MessageCnt ++;
            }
        }

        // Can't receive transaction messages before commit.
        Message<byte[]> message = consumer.receive(5, TimeUnit.SECONDS);
        Assert.assertNull(message);

        txn1.commit().get();

        // txn1 messages could be received after txn1 committed
        int receiveCnt = 0;
        for (int i = 0; i < txn1MessageCnt; i++) {
            message = consumer.receive();
            Assert.assertNotNull(message);
            receiveCnt ++;
        }
        Assert.assertEquals(txn1MessageCnt, receiveCnt);

        message = consumer.receive(5, TimeUnit.SECONDS);
        Assert.assertNull(message);

        txn2.commit().get();

        // txn2 messages could be received after txn2 committed
        receiveCnt = 0;
        for (int i = 0; i < txn2MessageCnt; i++) {
            message = consumer.receive();
            Assert.assertNotNull(message);
            receiveCnt ++;
        }
        Assert.assertEquals(txn2MessageCnt, receiveCnt);

        message = consumer.receive(5, TimeUnit.SECONDS);
        Assert.assertNull(message);

        log.info("message commit test enableBatch {}", enableBatch);
    }

    @Test
    public void produceAbortTest() throws Exception {
        Transaction txn = getTxn();

        @Cleanup
        Producer<byte[]> producer = pulsarClient
                .newProducer()
                .topic(TOPIC_OUTPUT)
                .sendTimeout(0, TimeUnit.SECONDS)
                .enableBatching(false)
                .create();

        int messageCnt = 10;
        for (int i = 0; i < messageCnt; i++) {
            producer.newMessage(txn).value(("Hello Txn - " + i).getBytes(UTF_8)).sendAsync();
        }

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient
                .newConsumer()
                .topic(TOPIC_OUTPUT)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("test")
                .enableBatchIndexAcknowledgment(true)
                .subscribe();

        // Can't receive transaction messages before abort.
        Message<byte[]> message = consumer.receive(5, TimeUnit.SECONDS);
        Assert.assertNull(message);

        txn.abort().get();

        // Cant't receive transaction messages after abort.
        message = consumer.receive(5, TimeUnit.SECONDS);
        Assert.assertNull(message);

        Thread.sleep(1000);
        for (int i = 0; i < TOPIC_PARTITION; i++) {
            PersistentTopicInternalStats stats =
                    admin.topics().getInternalStats("persistent://" + TOPIC_OUTPUT + "-partition-" + i);
            // the transaction abort, the related messages and abort marke should be acked,
            // so all the entries in this topic should be acked
            // and the markDeletePosition is equals with the lastConfirmedEntry
            Assert.assertEquals(stats.cursors.get("test").markDeletePosition, stats.lastConfirmedEntry);
        }

        log.info("finished test partitionAbortTest");
    }

    @Test
    public void txnAckTestNoBatchAndSharedSub() throws Exception {
        txnAckTest(false, 1, SubscriptionType.Shared);
    }

    @Test
    public void txnAckTestBatchAndSharedSub() throws Exception {
        txnAckTest(true, 200, SubscriptionType.Shared);
    }


    private void txnAckTest(boolean batchEnable, int maxBatchSize,
                         SubscriptionType subscriptionType) throws Exception {
        String normalTopic = NAMESPACE1 + "/normal-topic";

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(normalTopic)
                .subscriptionName("test")
                .enableBatchIndexAcknowledgment(true)
                .subscriptionType(subscriptionType)
                .ackTimeout(2, TimeUnit.SECONDS)
                .subscribe();

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(normalTopic)
                .enableBatching(batchEnable)
                .batchingMaxMessages(maxBatchSize)
                .create();

        for (int retryCnt = 0; retryCnt < 2; retryCnt++) {
            Transaction txn = getTxn();

            int messageCnt = 1000;
            // produce normal messages
            for (int i = 0; i < messageCnt; i++){
                producer.newMessage().value("hello".getBytes()).sendAsync();
            }

            // consume and ack messages with txn
            for (int i = 0; i < messageCnt; i++) {
                Message<byte[]> message = consumer.receive();
                Assert.assertNotNull(message);
                log.info("receive msgId: {}, count : {}", message.getMessageId(), i);
                consumer.acknowledgeAsync(message.getMessageId(), txn).get();
            }
            Thread.sleep(2000L);

            // the messages are pending ack state and can't be received
            Message<byte[]> message = consumer.receive(2, TimeUnit.SECONDS);
            Assert.assertNull(message);

            // 1) txn abort
            txn.abort().get();

            // after transaction abort, the messages could be received
            Transaction commitTxn = getTxn();
            for (int i = 0; i < messageCnt; i++) {
                message = consumer.receive(2, TimeUnit.SECONDS);
                Assert.assertNotNull(message);
                consumer.acknowledgeAsync(message.getMessageId(), commitTxn).get();
                log.info("receive msgId: {}, count: {}", message.getMessageId(), i);
            }

            // 2) ack committed by a new txn
            commitTxn.commit().get();

            // after transaction commit, the messages can't be received
            message = consumer.receive(2, TimeUnit.SECONDS);
            Assert.assertNull(message);

            try {
                commitTxn.commit().get();
                fail("recommit one transaction should be failed.");
            } catch (Exception reCommitError) {
                // recommit one transaction should be failed
                log.info("expected exception for recommit one transaction.");
                Assert.assertNotNull(reCommitError);
                Assert.assertTrue(reCommitError.getCause() instanceof
                        TransactionCoordinatorClientException.InvalidTxnStatusException);
            }
        }
    }

    @Test
    public void txnMessageAckTest() throws Exception {
        String topic = TOPIC_MESSAGE_ACK_TEST;
        final String subName = "test";
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient
                .newConsumer()
                .topic(topic)
                .subscriptionName(subName)
                .enableBatchIndexAcknowledgment(true)
                .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS)
                .subscribe();

        @Cleanup
        Producer<byte[]> producer = pulsarClient
                .newProducer()
                .topic(topic)
                .sendTimeout(0, TimeUnit.SECONDS)
                .enableBatching(false)
                .create();

        Transaction txn = getTxn();

        int messageCnt = 10;
        for (int i = 0; i < messageCnt; i++) {
            producer.newMessage(txn).value(("Hello Txn - " + i).getBytes(UTF_8)).sendAsync();
        }
        log.info("produce transaction messages finished");

        // Can't receive transaction messages before commit.
        Message<byte[]> message = consumer.receive(5, TimeUnit.SECONDS);
        Assert.assertNull(message);
        log.info("transaction messages can't be received before transaction committed");

        txn.commit().get();

        int ackedMessageCount = 0;
        int receiveCnt = 0;
        for (int i = 0; i < messageCnt; i++) {
            message = consumer.receive();
            Assert.assertNotNull(message);
            receiveCnt ++;
            if (i % 2 == 0) {
                consumer.acknowledge(message);
                ackedMessageCount ++;
            }
        }
        Assert.assertEquals(messageCnt, receiveCnt);

        message = consumer.receive(5, TimeUnit.SECONDS);
        Assert.assertNull(message);

        markDeletePositionCheck(topic, subName, false);

        consumer.redeliverUnacknowledgedMessages();

        receiveCnt = 0;
        for (int i = 0; i < messageCnt - ackedMessageCount; i++) {
            message = consumer.receive(2, TimeUnit.SECONDS);
            Assert.assertNotNull(message);
            consumer.acknowledge(message);
            receiveCnt ++;
        }
        Assert.assertEquals(messageCnt - ackedMessageCount, receiveCnt);

        message = consumer.receive(2, TimeUnit.SECONDS);
        Assert.assertNull(message);
        for (int partition = 0; partition < TOPIC_PARTITION; partition ++) {
            topic = TopicName.get(topic).getPartition(partition).toString();
            boolean exist = false;
            for (int i = 0; i < getPulsarServiceList().size(); i++) {

                Field field = BrokerService.class.getDeclaredField("topics");
                field.setAccessible(true);
                ConcurrentOpenHashMap<String, CompletableFuture<Optional<Topic>>> topics =
                        (ConcurrentOpenHashMap<String, CompletableFuture<Optional<Topic>>>) field
                                .get(getPulsarServiceList().get(i).getBrokerService());
                CompletableFuture<Optional<Topic>> topicFuture = topics.get(topic);

                if (topicFuture != null) {
                    Optional<Topic> topicOptional = topicFuture.get();
                    if (topicOptional.isPresent()) {
                        PersistentSubscription persistentSubscription =
                                (PersistentSubscription) topicOptional.get().getSubscription(subName);
                        Position markDeletePosition = persistentSubscription.getCursor().getMarkDeletedPosition();
                        Position lastConfirmedEntry = persistentSubscription.getCursor()
                                .getManagedLedger().getLastConfirmedEntry();
                        exist = true;
                        if (!markDeletePosition.equals(lastConfirmedEntry)) {
                            //this because of the transaction commit marker have't delete
                            //delete commit marker after ack position
                            //when delete commit marker operation is processing, next delete operation will not do again
                            //when delete commit marker operation finish, it can run next delete commit marker operation
                            //so this test may not delete all the position in this manageLedger.
                            Position markerPosition = ((ManagedLedgerImpl) persistentSubscription.getCursor()
                                    .getManagedLedger()).getNextValidPosition((PositionImpl) markDeletePosition);
                            //marker is the lastConfirmedEntry, after commit the marker will only be write in
                            if (!markerPosition.equals(lastConfirmedEntry)) {
                                log.error("Mark delete position is not commit marker position!");
                                fail();
                            }
                        }
                    }
                }
            }
            assertTrue(exist);
        }

        log.info("receive transaction messages count: {}", receiveCnt);
    }

    @Test
    public void txnAckTestBatchAndCumulativeSub() throws Exception {
        txnCumulativeAckTest(true, 200, SubscriptionType.Failover);
    }

    @Test
    public void txnAckTestNoBatchAndCumulativeSub() throws Exception {
        txnCumulativeAckTest(false, 1, SubscriptionType.Failover);
    }

    public void txnCumulativeAckTest(boolean batchEnable, int maxBatchSize, SubscriptionType subscriptionType)
            throws Exception {
        String normalTopic = NAMESPACE1 + "/normal-topic";

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(normalTopic)
                .subscriptionName("test")
                .enableBatchIndexAcknowledgment(true)
                .subscriptionType(subscriptionType)
                .ackTimeout(1, TimeUnit.MINUTES)
                .subscribe();

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(normalTopic)
                .enableBatching(batchEnable)
                .batchingMaxMessages(maxBatchSize)
                .batchingMaxPublishDelay(1, TimeUnit.SECONDS)
                .create();

        for (int retryCnt = 0; retryCnt < 2; retryCnt++) {
            Transaction abortTxn = getTxn();
            int messageCnt = 1000;
            // produce normal messages
            for (int i = 0; i < messageCnt; i++){
                producer.newMessage().value("hello".getBytes()).sendAsync();
            }
            Message<byte[]> message = null;
            Thread.sleep(1000L);
            for (int i = 0; i < messageCnt; i++) {
                message = consumer.receive(1, TimeUnit.SECONDS);
                Assert.assertNotNull(message);
                if (i % 3 == 0) {
                    consumer.acknowledgeCumulativeAsync(message.getMessageId(), abortTxn).get();
                }
                log.info("receive msgId abort: {}, retryCount : {}, count : {}", message.getMessageId(), retryCnt, i);
            }
            try {
                consumer.acknowledgeCumulativeAsync(message.getMessageId(), abortTxn).get();
                fail("not ack conflict ");
            } catch (Exception e) {
                Assert.assertTrue(e.getCause() instanceof PulsarClientException.TransactionConflictException);
            }

            try {
                consumer.acknowledgeCumulativeAsync(DefaultImplementation
                        .newMessageId(((MessageIdImpl) message.getMessageId()).getLedgerId(),
                                ((MessageIdImpl) message.getMessageId()).getEntryId() - 1, -1),
                        abortTxn).get();
                fail("not ack conflict ");
            } catch (Exception e) {
                Assert.assertTrue(e.getCause() instanceof PulsarClientException.TransactionConflictException);
            }

            // the messages are pending ack state and can't be received
            message = consumer.receive(2, TimeUnit.SECONDS);
            Assert.assertNull(message);

            abortTxn.abort().get();
            Transaction commitTxn = getTxn();
            for (int i = 0; i < messageCnt; i++) {
                message = consumer.receive(1, TimeUnit.SECONDS);
                Assert.assertNotNull(message);
                if (i % 3 == 0) {
                    consumer.acknowledgeCumulativeAsync(message.getMessageId(), commitTxn).get();
                }
                log.info("receive msgId abort: {}, retryCount : {}, count : {}", message.getMessageId(), retryCnt, i);
            }

            commitTxn.commit().get();
            try {
                commitTxn.commit().get();
                fail("recommit one transaction should be failed.");
            } catch (Exception reCommitError) {
                // recommit one transaction should be failed
                log.info("expected exception for recommit one transaction.");
                Assert.assertNotNull(reCommitError);
                Assert.assertTrue(reCommitError.getCause() instanceof
                        TransactionCoordinatorClientException.InvalidTxnStatusException);
            }

            message = consumer.receive(1, TimeUnit.SECONDS);
            Assert.assertNull(message);
        }
    }

    private Transaction getTxn() throws Exception {
        return pulsarClient
                .newTransaction()
                .withTransactionTimeout(2, TimeUnit.SECONDS)
                .build()
                .get();
    }

    private void markDeletePositionCheck(String topic, String subName, boolean equalsWithLastConfirm) throws Exception {
        for (int i = 0; i < TOPIC_PARTITION; i++) {
            PersistentTopicInternalStats stats = null;
            String checkTopic = TopicName.get(topic).getPartition(i).toString();
            for (int j = 0; j < 10; j++) {
                stats = admin.topics().getInternalStats(checkTopic, false);
                if (stats.lastConfirmedEntry.equals(stats.cursors.get(subName).markDeletePosition)) {
                    break;
                }
                Thread.sleep(200);
            }
            if (equalsWithLastConfirm) {
                Assert.assertEquals(stats.cursors.get(subName).markDeletePosition, stats.lastConfirmedEntry);
            } else {
                Assert.assertNotEquals(stats.cursors.get(subName).markDeletePosition, stats.lastConfirmedEntry);
            }
        }
    }

    @Test
    public void txnMetadataHandlerRecoverTest() throws Exception {
        String topic = NAMESPACE1 + "/tc-metadata-handler-recover";
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .sendTimeout(0, TimeUnit.SECONDS)
                .create();

        Map<TxnID, List<MessageId>> txnIDListMap = new HashMap<>();

        int txnCnt = 20;
        int messageCnt = 10;
        for (int i = 0; i < txnCnt; i++) {
            TransactionImpl txn = (TransactionImpl) pulsarClient.newTransaction()
                    .withTransactionTimeout(5, TimeUnit.MINUTES)
                    .build().get();
            List<MessageId> messageIds = new ArrayList<>();
            for (int j = 0; j < messageCnt; j++) {
                MessageId messageId = producer.newMessage(txn).value("Hello".getBytes()).sendAsync().get();
                messageIds.add(messageId);
            }
            txnIDListMap.put(new TxnID(txn.getTxnIdMostBits(), txn.getTxnIdLeastBits()), messageIds);
        }

        pulsarClient.close();
        PulsarClientImpl recoverPulsarClient = (PulsarClientImpl) PulsarClient.builder()
                .serviceUrl(getPulsarServiceList().get(0).getBrokerServiceUrl())
                .statsInterval(0, TimeUnit.SECONDS)
                .enableTransaction(true)
                .build();

        TransactionCoordinatorClient tcClient = recoverPulsarClient.getTcClient();
        for (Map.Entry<TxnID, List<MessageId>> entry : txnIDListMap.entrySet()) {
            tcClient.commit(entry.getKey(), entry.getValue());
        }

        Consumer<byte[]> consumer = recoverPulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("test")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        for (int i = 0; i < txnCnt * messageCnt; i++) {
            Message<byte[]> message = consumer.receive();
            Assert.assertNotNull(message);
        }
    }

}
