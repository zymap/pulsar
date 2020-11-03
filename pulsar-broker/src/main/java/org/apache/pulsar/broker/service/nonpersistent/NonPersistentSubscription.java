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
package org.apache.pulsar.broker.service.nonpersistent;

<<<<<<< HEAD
import com.google.common.base.MoreObjects;

=======
import java.util.Collections;
>>>>>>> f773c602c... Test pr 10 (#27)
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

<<<<<<< HEAD
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
=======
import com.google.common.base.MoreObjects;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.ServiceConfiguration;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.BrokerServiceException.ServerMetadataException;
import org.apache.pulsar.broker.service.BrokerServiceException.SubscriptionBusyException;
import org.apache.pulsar.broker.service.BrokerServiceException.SubscriptionFencedException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Dispatcher;
<<<<<<< HEAD
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
=======
import org.apache.pulsar.broker.service.HashRangeAutoSplitStickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.ConsistentHashingStickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.HashRangeExclusiveStickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.StickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.common.api.proto.PulsarApi.KeySharedMeta;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ConsumerStats;
import org.apache.pulsar.common.policies.data.NonPersistentSubscriptionStats;
<<<<<<< HEAD
import org.apache.pulsar.utils.CopyOnWriteArrayList;
=======
>>>>>>> f773c602c... Test pr 10 (#27)
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NonPersistentSubscription implements Subscription {
    private final NonPersistentTopic topic;
    private volatile NonPersistentDispatcher dispatcher;
    private final String topicName;
    private final String subName;
<<<<<<< HEAD
=======
    private final String fullName;
>>>>>>> f773c602c... Test pr 10 (#27)

    private static final int FALSE = 0;
    private static final int TRUE = 1;
    private static final AtomicIntegerFieldUpdater<NonPersistentSubscription> IS_FENCED_UPDATER = AtomicIntegerFieldUpdater
            .newUpdater(NonPersistentSubscription.class, "isFenced");
    @SuppressWarnings("unused")
    private volatile int isFenced = FALSE;

<<<<<<< HEAD
=======
    // Timestamp of when this subscription was last seen active
    private volatile long lastActive;

>>>>>>> f773c602c... Test pr 10 (#27)
    public NonPersistentSubscription(NonPersistentTopic topic, String subscriptionName) {
        this.topic = topic;
        this.topicName = topic.getName();
        this.subName = subscriptionName;
<<<<<<< HEAD
        IS_FENCED_UPDATER.set(this, FALSE);
=======
        this.fullName = MoreObjects.toStringHelper(this).add("topic", topicName).add("name", subName).toString();
        IS_FENCED_UPDATER.set(this, FALSE);
        this.lastActive = System.currentTimeMillis();
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Override
    public String getName() {
        return this.subName;
    }

    @Override
    public Topic getTopic() {
        return topic;
    }

    @Override
<<<<<<< HEAD
    public synchronized void addConsumer(Consumer consumer) throws BrokerServiceException {
=======
    public boolean isReplicated() {
        return false;
    }

    @Override
    public synchronized void addConsumer(Consumer consumer) throws BrokerServiceException {
        updateLastActive();
>>>>>>> f773c602c... Test pr 10 (#27)
        if (IS_FENCED_UPDATER.get(this) == TRUE) {
            log.warn("Attempting to add consumer {} on a fenced subscription", consumer);
            throw new SubscriptionFencedException("Subscription is fenced");
        }

        if (dispatcher == null || !dispatcher.isConsumerConnected()) {
<<<<<<< HEAD
            switch (consumer.subType()) {
            case Exclusive:
                if (dispatcher == null || dispatcher.getType() != SubType.Exclusive) {
=======
            Dispatcher previousDispatcher = null;

            switch (consumer.subType()) {
            case Exclusive:
                if (dispatcher == null || dispatcher.getType() != SubType.Exclusive) {
                    previousDispatcher = dispatcher;
>>>>>>> f773c602c... Test pr 10 (#27)
                    dispatcher = new NonPersistentDispatcherSingleActiveConsumer(SubType.Exclusive, 0, topic, this);
                }
                break;
            case Shared:
                if (dispatcher == null || dispatcher.getType() != SubType.Shared) {
<<<<<<< HEAD
=======
                    previousDispatcher = dispatcher;
>>>>>>> f773c602c... Test pr 10 (#27)
                    dispatcher = new NonPersistentDispatcherMultipleConsumers(topic, this);
                }
                break;
            case Failover:
                int partitionIndex = TopicName.getPartitionIndex(topicName);
                if (partitionIndex < 0) {
                    // For non partition topics, assume index 0 to pick a predictable consumer
                    partitionIndex = 0;
                }

                if (dispatcher == null || dispatcher.getType() != SubType.Failover) {
<<<<<<< HEAD
=======
                    previousDispatcher = dispatcher;
>>>>>>> f773c602c... Test pr 10 (#27)
                    dispatcher = new NonPersistentDispatcherSingleActiveConsumer(SubType.Failover, partitionIndex,
                            topic, this);
                }
                break;
<<<<<<< HEAD
            default:
                throw new ServerMetadataException("Unsupported subscription type");
            }
=======
            case Key_Shared:
                if (dispatcher == null || dispatcher.getType() != SubType.Key_Shared) {
                    previousDispatcher = dispatcher;
                    KeySharedMeta ksm = consumer.getKeySharedMeta() != null ? consumer.getKeySharedMeta() : KeySharedMeta.getDefaultInstance();

                    switch (ksm.getKeySharedMode()) {
                        case STICKY:
                            dispatcher = new NonPersistentStickyKeyDispatcherMultipleConsumers(topic, this,
                                    new HashRangeExclusiveStickyKeyConsumerSelector());
                            break;

                        case AUTO_SPLIT:
                        default:
                            StickyKeyConsumerSelector selector;
                            ServiceConfiguration conf = topic.getBrokerService().getPulsar().getConfiguration();
                            if (conf.isSubscriptionKeySharedUseConsistentHashing()) {
                                selector = new ConsistentHashingStickyKeyConsumerSelector(
                                        conf.getSubscriptionKeySharedConsistentHashingReplicaPoints());
                            } else {
                                selector = new HashRangeAutoSplitStickyKeyConsumerSelector();
                            }

                            dispatcher = new NonPersistentStickyKeyDispatcherMultipleConsumers(topic, this, selector);
                            break;
                    }
                }
                break;
            default:
                throw new ServerMetadataException("Unsupported subscription type");
            }

            if (previousDispatcher != null) {
                previousDispatcher.close().thenRun(() -> {
                    log.info("[{}][{}] Successfully closed previous dispatcher", topicName, subName);
                }).exceptionally(ex -> {
                    log.error("[{}][{}] Failed to close previous dispatcher", topicName, subName, ex);
                    return null;
                });
            }
>>>>>>> f773c602c... Test pr 10 (#27)
        } else {
            if (consumer.subType() != dispatcher.getType()) {
                throw new SubscriptionBusyException("Subscription is of different type");
            }
        }

        dispatcher.addConsumer(consumer);
    }

    @Override
<<<<<<< HEAD
    public synchronized void removeConsumer(Consumer consumer) throws BrokerServiceException {
=======
    public synchronized void removeConsumer(Consumer consumer, boolean isResetCursor) throws BrokerServiceException {
        updateLastActive();
>>>>>>> f773c602c... Test pr 10 (#27)
        if (dispatcher != null) {
            dispatcher.removeConsumer(consumer);
        }

        // invalid consumer remove will throw an exception
        // decrement usage is triggered only for valid consumer close
        NonPersistentTopic.USAGE_COUNT_UPDATER.decrementAndGet(topic);
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] [{}] Removed consumer -- count: {}", topic.getName(), subName, consumer.consumerName(),
                    NonPersistentTopic.USAGE_COUNT_UPDATER.get(topic));
        }
    }

    @Override
    public void consumerFlow(Consumer consumer, int additionalNumberOfMessages) {
        dispatcher.consumerFlow(consumer, additionalNumberOfMessages);
    }

    @Override
    public void acknowledgeMessage(List<Position> position, AckType ackType, Map<String, Long> properties) {
        // No-op
    }

    @Override
    public String toString() {
<<<<<<< HEAD
        return MoreObjects.toStringHelper(this).add("topic", topicName).add("name", subName).toString();
=======
        return fullName;
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Override
    public String getTopicName() {
        return this.topicName;
    }

    @Override
    public SubType getType() {
        return dispatcher != null ? dispatcher.getType() : null;
    }

    @Override
    public String getTypeString() {
        SubType type = getType();
        if (type == null) {
            return "None";
        }

        switch (type) {
        case Exclusive:
            return "Exclusive";
        case Failover:
            return "Failover";
        case Shared:
            return "Shared";
<<<<<<< HEAD
=======
        case Key_Shared:
            return "Key_Shared";
>>>>>>> f773c602c... Test pr 10 (#27)
        }

        return "Null";
    }

    @Override
    public CompletableFuture<Void> clearBacklog() {
        // No-op
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> skipMessages(int numMessagesToSkip) {
        // No-op
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> resetCursor(long timestamp) {
        // No-op
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Entry> peekNthMessage(int messagePosition) {
        // No-op
        return CompletableFuture.completedFuture(null);// TODO: throw exception
    }

    @Override
<<<<<<< HEAD
    public long getNumberOfEntriesInBacklog() {
=======
    public long getNumberOfEntriesInBacklog(boolean getPreciseBacklog) {
>>>>>>> f773c602c... Test pr 10 (#27)
        // No-op
        return 0;
    }

    @Override
    public NonPersistentDispatcher getDispatcher() {
        return this.dispatcher;
    }

    @Override
    public CompletableFuture<Void> close() {
        IS_FENCED_UPDATER.set(this, TRUE);
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Disconnect all consumers attached to the dispatcher and close this subscription
     *
     * @return CompletableFuture indicating the completion of disconnect operation
     */
    @Override
    public synchronized CompletableFuture<Void> disconnect() {
        CompletableFuture<Void> disconnectFuture = new CompletableFuture<>();

        // block any further consumers on this subscription
        IS_FENCED_UPDATER.set(this, TRUE);

        (dispatcher != null ? dispatcher.close() : CompletableFuture.completedFuture(null)).thenCompose(v -> close())
                .thenRun(() -> {
                    log.info("[{}][{}] Successfully disconnected and closed subscription", topicName, subName);
                    disconnectFuture.complete(null);
                }).exceptionally(exception -> {
                    IS_FENCED_UPDATER.set(this, FALSE);
<<<<<<< HEAD
                    dispatcher.reset();
=======
                    if (dispatcher != null) {
                        dispatcher.reset();
                    }
>>>>>>> f773c602c... Test pr 10 (#27)
                    log.error("[{}][{}] Error disconnecting consumers from subscription", topicName, subName,
                            exception);
                    disconnectFuture.completeExceptionally(exception);
                    return null;
                });

        return disconnectFuture;
    }

    /**
     * Delete the subscription by closing and deleting its managed cursor if no consumers are connected to it. Handle
     * unsubscribe call from admin layer.
     *
     * @return CompletableFuture indicating the completion of delete operation
     */
    @Override
    public CompletableFuture<Void> delete() {
<<<<<<< HEAD
=======
        return delete(false);
    }

    /**
     * Forcefully close all consumers and deletes the subscription.
     * @return
     */
    @Override
    public CompletableFuture<Void> deleteForcefully() {
        return delete(true);
    }

    /**
     * Delete the subscription by closing and deleting its managed cursor. Handle unsubscribe call from admin layer.
     *
     * @param closeIfConsumersConnected
     *            Flag indicate whether explicitly close connected consumers before trying to delete subscription. If
     *            any consumer is connected to it and if this flag is disable then this operation fails.
     * @return CompletableFuture indicating the completion of delete operation
     */
    private CompletableFuture<Void> delete(boolean closeIfConsumersConnected) {
>>>>>>> f773c602c... Test pr 10 (#27)
        CompletableFuture<Void> deleteFuture = new CompletableFuture<>();

        log.info("[{}][{}] Unsubscribing", topicName, subName);

<<<<<<< HEAD
        // cursor close handles pending delete (ack) operations
        this.close().thenCompose(v -> topic.unsubscribe(subName)).thenAccept(v -> deleteFuture.complete(null))
                .exceptionally(exception -> {
                    IS_FENCED_UPDATER.set(this, FALSE);
                    log.error("[{}][{}] Error deleting subscription", topicName, subName, exception);
                    deleteFuture.completeExceptionally(exception);
                    return null;
                });
=======
        CompletableFuture<Void> closeSubscriptionFuture = new CompletableFuture<>();

        if (closeIfConsumersConnected) {
            this.disconnect().thenRun(() -> {
                closeSubscriptionFuture.complete(null);
            }).exceptionally(ex -> {
                log.error("[{}][{}] Error disconnecting and closing subscription", topicName, subName, ex);
                closeSubscriptionFuture.completeExceptionally(ex);
                return null;
            });
        } else {
            this.close().thenRun(() -> {
                closeSubscriptionFuture.complete(null);
            }).exceptionally(exception -> {
                log.error("[{}][{}] Error closing subscription", topicName, subName, exception);
                closeSubscriptionFuture.completeExceptionally(exception);
                return null;
            });
        }

        // cursor close handles pending delete (ack) operations
        closeSubscriptionFuture.thenCompose(v -> topic.unsubscribe(subName)).thenAccept(v -> {
            synchronized (this) {
                (dispatcher != null ? dispatcher.close() : CompletableFuture.completedFuture(null)).thenRun(() -> {
                    log.info("[{}][{}] Successfully deleted subscription", topicName, subName);
                    deleteFuture.complete(null);
                }).exceptionally(ex -> {
                    IS_FENCED_UPDATER.set(this, FALSE);
                    if (dispatcher != null) {
                        dispatcher.reset();
                    }
                    log.error("[{}][{}] Error deleting subscription", topicName, subName, ex);
                    deleteFuture.completeExceptionally(ex);
                    return null;
                });
            }
        }).exceptionally(exception -> {
            IS_FENCED_UPDATER.set(this, FALSE);
            log.error("[{}][{}] Error deleting subscription", topicName, subName, exception);
            deleteFuture.completeExceptionally(exception);
            return null;
        });
>>>>>>> f773c602c... Test pr 10 (#27)

        return deleteFuture;
    }

    /**
     * Handle unsubscribe command from the client API Check with the dispatcher is this consumer can proceed with
     * unsubscribe
     *
     * @param consumer
     *            consumer object that is initiating the unsubscribe operation
     * @return CompletableFuture indicating the completion of ubsubscribe operation
     */
    @Override
    public CompletableFuture<Void> doUnsubscribe(Consumer consumer) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            if (dispatcher.canUnsubscribe(consumer)) {
                consumer.close();
                return delete();
            }
            future.completeExceptionally(
                    new ServerMetadataException("Unconnected or shared consumer attempting to unsubscribe"));
        } catch (BrokerServiceException e) {
            log.warn("Error removing consumer {}", consumer);
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
<<<<<<< HEAD
    public CopyOnWriteArrayList<Consumer> getConsumers() {
=======
    public List<Consumer> getConsumers() {
>>>>>>> f773c602c... Test pr 10 (#27)
        Dispatcher dispatcher = this.dispatcher;
        if (dispatcher != null) {
            return dispatcher.getConsumers();
        } else {
<<<<<<< HEAD
            return CopyOnWriteArrayList.empty();
=======
            return Collections.emptyList();
>>>>>>> f773c602c... Test pr 10 (#27)
        }
    }

    @Override
    public void expireMessages(int messageTTLInSeconds) {
        // No-op
    }

    public NonPersistentSubscriptionStats getStats() {
        NonPersistentSubscriptionStats subStats = new NonPersistentSubscriptionStats();

        NonPersistentDispatcher dispatcher = this.dispatcher;
        if (dispatcher != null) {
            dispatcher.getConsumers().forEach(consumer -> {
                ConsumerStats consumerStats = consumer.getStats();
                subStats.consumers.add(consumerStats);
                subStats.msgRateOut += consumerStats.msgRateOut;
                subStats.msgThroughputOut += consumerStats.msgThroughputOut;
<<<<<<< HEAD
=======
                subStats.bytesOutCounter += consumerStats.bytesOutCounter;
                subStats.msgOutCounter += consumerStats.msgOutCounter;
>>>>>>> f773c602c... Test pr 10 (#27)
                subStats.msgRateRedeliver += consumerStats.msgRateRedeliver;
            });
        }

        subStats.type = getType();
<<<<<<< HEAD
        subStats.msgDropRate = dispatcher.getMesssageDropRate().getRate();
=======
        subStats.msgDropRate = dispatcher.getMessageDropRate().getValueRate();
>>>>>>> f773c602c... Test pr 10 (#27)
        return subStats;
    }

    @Override
    public synchronized void redeliverUnacknowledgedMessages(Consumer consumer) {
     // No-op
    }

    @Override
    public synchronized void redeliverUnacknowledgedMessages(Consumer consumer, List<PositionImpl> positions) {
        // No-op
    }

    @Override
    public void addUnAckedMessages(int unAckMessages) {
        // No-op
    }

    @Override
    public double getExpiredMessageRate() {
        // No-op
        return 0;
    }

    @Override
    public void markTopicWithBatchMessagePublished() {
        topic.markBatchMessagePublished();
    }

    @Override
    public CompletableFuture<Void> resetCursor(Position position) {
        return CompletableFuture.completedFuture(null);
    }

<<<<<<< HEAD
    private static final Logger log = LoggerFactory.getLogger(NonPersistentSubscription.class);

=======
    @Override
    public CompletableFuture<Void> endTxn(long txnidMostBits, long txnidLeastBits, int txnAction) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        completableFuture.completeExceptionally(new Exception("Unsupported operation end txn for NonPersistentSubscription"));
        return completableFuture;
    }

    private static final Logger log = LoggerFactory.getLogger(NonPersistentSubscription.class);

    public long getLastActive() {
        return lastActive;
    }

    public void updateLastActive() {
        this.lastActive = System.currentTimeMillis();
    }
>>>>>>> f773c602c... Test pr 10 (#27)
}
