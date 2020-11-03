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
package org.apache.pulsar.broker.service.persistent;

<<<<<<< HEAD
import static java.util.stream.Collectors.toSet;
import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;
import static org.apache.pulsar.broker.service.persistent.PersistentTopic.MESSAGE_RATE_BACKOFF_MS;

=======
import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;
import static org.apache.pulsar.broker.service.persistent.PersistentTopic.MESSAGE_RATE_BACKOFF_MS;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;

import java.util.Collections;
>>>>>>> f773c602c... Test pr 10 (#27)
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
<<<<<<< HEAD
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

=======
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import com.google.common.collect.Range;

>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.NoMoreEntriesToReadException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.TooManyRequestsException;
import org.apache.bookkeeper.mledger.Position;
<<<<<<< HEAD
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.admin.AdminResource;
=======
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.delayed.DelayedDeliveryTracker;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.broker.service.AbstractDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.BrokerServiceException.ConsumerBusyException;
import org.apache.pulsar.broker.service.Consumer;
<<<<<<< HEAD
import org.apache.pulsar.broker.service.Consumer.SendMessageInfo;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.RedeliveryTracker;
import org.apache.pulsar.broker.service.RedeliveryTrackerDisabled;
import org.apache.pulsar.broker.service.InMemoryRedeliveryTracker;
import org.apache.pulsar.client.impl.Backoff;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.collections.ConcurrentLongPairSet;
import org.apache.pulsar.utils.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;

/**
 */
public class PersistentDispatcherMultipleConsumers  extends AbstractDispatcherMultipleConsumers implements Dispatcher, ReadEntriesCallback {

    private static final int MaxReadBatchSize = 100;
    private static final int MaxRoundRobinBatchSize = 20;

    private final PersistentTopic topic;
    private final ManagedCursor cursor;

    private CompletableFuture<Void> closeFuture = null;
    private ConcurrentLongPairSet messagesToReplay;
    private final RedeliveryTracker redeliveryTracker;

    private boolean havePendingRead = false;
    private boolean havePendingReplayRead = false;
    private boolean shouldRewindBeforeReadingOrReplaying = false;
    private final String name;

    private int totalAvailablePermits = 0;
    private int readBatchSize;
=======
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.EntryBatchIndexesAcks;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.InMemoryRedeliveryTracker;
import org.apache.pulsar.broker.service.RedeliveryTracker;
import org.apache.pulsar.broker.service.RedeliveryTrackerDisabled;
import org.apache.pulsar.broker.service.SendMessageInfo;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.persistent.DispatchRateLimiter.Type;
import org.apache.pulsar.broker.transaction.buffer.exceptions.TransactionNotSealedException;
import org.apache.pulsar.client.impl.Backoff;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.collections.ConcurrentSortedLongPairSet;
import org.apache.pulsar.common.util.collections.LongPairSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class PersistentDispatcherMultipleConsumers extends AbstractDispatcherMultipleConsumers implements Dispatcher, ReadEntriesCallback {

    protected final PersistentTopic topic;
    protected final ManagedCursor cursor;
    protected volatile Range<PositionImpl> lastIndividualDeletedRangeFromCursorRecovery;

    private CompletableFuture<Void> closeFuture = null;
    LongPairSet messagesToRedeliver = new ConcurrentSortedLongPairSet(128, 2);
    protected final RedeliveryTracker redeliveryTracker;

    private Optional<DelayedDeliveryTracker> delayedDeliveryTracker = Optional.empty();

    private volatile boolean havePendingRead = false;
    private volatile boolean havePendingReplayRead = false;
    private boolean shouldRewindBeforeReadingOrReplaying = false;
    protected final String name;

    protected static final AtomicIntegerFieldUpdater<PersistentDispatcherMultipleConsumers> TOTAL_AVAILABLE_PERMITS_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(PersistentDispatcherMultipleConsumers.class, "totalAvailablePermits");
    protected volatile int totalAvailablePermits = 0;
    private volatile int readBatchSize;
>>>>>>> f773c602c... Test pr 10 (#27)
    private final Backoff readFailureBackoff = new Backoff(15, TimeUnit.SECONDS, 1, TimeUnit.MINUTES, 0, TimeUnit.MILLISECONDS);
    private static final AtomicIntegerFieldUpdater<PersistentDispatcherMultipleConsumers> TOTAL_UNACKED_MESSAGES_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(PersistentDispatcherMultipleConsumers.class, "totalUnackedMessages");
    private volatile int totalUnackedMessages = 0;
<<<<<<< HEAD
    private final int maxUnackedMessages;
    private volatile int blockedDispatcherOnUnackedMsgs = FALSE;
    private static final AtomicIntegerFieldUpdater<PersistentDispatcherMultipleConsumers> BLOCKED_DISPATCHER_ON_UNACKMSG_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(PersistentDispatcherMultipleConsumers.class, "blockedDispatcherOnUnackedMsgs");
    private final ServiceConfiguration serviceConfig;
    private Optional<DispatchRateLimiter> dispatchRateLimiter = Optional.empty();
=======
    private volatile int blockedDispatcherOnUnackedMsgs = FALSE;
    private static final AtomicIntegerFieldUpdater<PersistentDispatcherMultipleConsumers> BLOCKED_DISPATCHER_ON_UNACKMSG_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(PersistentDispatcherMultipleConsumers.class, "blockedDispatcherOnUnackedMsgs");
    protected final ServiceConfiguration serviceConfig;
    protected Optional<DispatchRateLimiter> dispatchRateLimiter = Optional.empty();
>>>>>>> f773c602c... Test pr 10 (#27)

    enum ReadType {
        Normal, Replay
    }

<<<<<<< HEAD
    public PersistentDispatcherMultipleConsumers(PersistentTopic topic, ManagedCursor cursor) {
        this.serviceConfig = topic.getBrokerService().pulsar().getConfiguration();
        this.cursor = cursor;
        this.name = topic.getName() + " / " + Codec.decode(cursor.getName());
        this.topic = topic;
        this.messagesToReplay = new ConcurrentLongPairSet(512, 2);
        this.redeliveryTracker = this.serviceConfig.isSubscriptionRedeliveryTrackerEnabled()
                ? new InMemoryRedeliveryTracker()
                : RedeliveryTrackerDisabled.REDELIVERY_TRACKER_DISABLED;
        this.readBatchSize = MaxReadBatchSize;
        this.maxUnackedMessages = topic.getBrokerService().pulsar().getConfiguration()
                .getMaxUnackedMessagesPerSubscription();
=======
    public PersistentDispatcherMultipleConsumers(PersistentTopic topic, ManagedCursor cursor, Subscription subscription) {
        super(subscription);
        this.serviceConfig = topic.getBrokerService().pulsar().getConfiguration();
        this.cursor = cursor;
        this.lastIndividualDeletedRangeFromCursorRecovery = cursor.getLastIndividualDeletedRange();
        this.name = topic.getName() + " / " + Codec.decode(cursor.getName());
        this.topic = topic;
        this.redeliveryTracker = this.serviceConfig.isSubscriptionRedeliveryTrackerEnabled()
                ? new InMemoryRedeliveryTracker()
                : RedeliveryTrackerDisabled.REDELIVERY_TRACKER_DISABLED;
        this.readBatchSize = serviceConfig.getDispatcherMaxReadBatchSize();
>>>>>>> f773c602c... Test pr 10 (#27)
        this.initializeDispatchRateLimiterIfNeeded(Optional.empty());
    }

    @Override
    public synchronized void addConsumer(Consumer consumer) throws BrokerServiceException {
        if (IS_CLOSED_UPDATER.get(this) == TRUE) {
<<<<<<< HEAD
            log.warn("[{}] Dispatcher is already closed. Closing consumer ", name, consumer);
=======
            log.warn("[{}] Dispatcher is already closed. Closing consumer {}", name, consumer);
>>>>>>> f773c602c... Test pr 10 (#27)
            consumer.disconnect();
            return;
        }
        if (consumerList.isEmpty()) {
            if (havePendingRead || havePendingReplayRead) {
                // There is a pending read from previous run. We must wait for it to complete and then rewind
                shouldRewindBeforeReadingOrReplaying = true;
            } else {
                cursor.rewind();
                shouldRewindBeforeReadingOrReplaying = false;
            }
<<<<<<< HEAD
            messagesToReplay.clear();
        }

        if (isConsumersExceededOnTopic()) {
            log.warn("[{}] Attempting to add consumer to topic which reached max consumers limit", name);
            throw new ConsumerBusyException("Topic reached max consumers limit");
=======
            messagesToRedeliver.clear();
>>>>>>> f773c602c... Test pr 10 (#27)
        }

        if (isConsumersExceededOnSubscription()) {
            log.warn("[{}] Attempting to add consumer to subscription which reached max consumers limit", name);
            throw new ConsumerBusyException("Subscription reached max consumers limit");
        }

        consumerList.add(consumer);
        consumerList.sort((c1, c2) -> c1.getPriorityLevel() - c2.getPriorityLevel());
        consumerSet.add(consumer);
    }

<<<<<<< HEAD
    private boolean isConsumersExceededOnTopic() {
        Policies policies;
        try {
            policies = topic.getBrokerService().pulsar().getConfigurationCache().policiesCache()
                    .get(AdminResource.path(POLICIES, TopicName.get(topic.getName()).getNamespace()))
                    .orElseGet(() -> new Policies());
        } catch (Exception e) {
            policies = new Policies();
        }
        final int maxConsumersPerTopic = policies.max_consumers_per_topic > 0 ?
                policies.max_consumers_per_topic :
                serviceConfig.getMaxConsumersPerTopic();
        if (maxConsumersPerTopic > 0 && maxConsumersPerTopic <= topic.getNumberOfConsumers()) {
            return true;
        }
        return false;
    }

    private boolean isConsumersExceededOnSubscription() {
        Policies policies;
        try {
            policies = topic.getBrokerService().pulsar().getConfigurationCache().policiesCache()
                    .get(AdminResource.path(POLICIES, TopicName.get(topic.getName()).getNamespace()))
                    .orElseGet(() -> new Policies());
        } catch (Exception e) {
            policies = new Policies();
        }
        final int maxConsumersPerSubscription = policies.max_consumers_per_subscription > 0 ?
                policies.max_consumers_per_subscription :
                serviceConfig.getMaxConsumersPerSubscription();
=======
    private boolean isConsumersExceededOnSubscription() {
        Policies policies = null;
        Integer maxConsumersPerSubscription = null;
        try {
            maxConsumersPerSubscription = Optional.ofNullable(topic.getBrokerService()
                    .getTopicPolicies(TopicName.get(topic.getName())))
                    .map(TopicPolicies::getMaxConsumersPerSubscription)
                    .orElse(null);

            if (maxConsumersPerSubscription == null) {
                // Use getDataIfPresent from zk cache to make the call non-blocking and prevent deadlocks in addConsumer
                policies = topic.getBrokerService().pulsar().getConfigurationCache().policiesCache()
                        .getDataIfPresent(AdminResource.path(POLICIES, TopicName.get(topic.getName()).getNamespace()));
                if (policies == null) {
                    policies = new Policies();
                }
            }
        } catch (Exception e) {
            policies = new Policies();
        }

        if (maxConsumersPerSubscription == null) {
            maxConsumersPerSubscription = policies.max_consumers_per_subscription > 0 ?
                    policies.max_consumers_per_subscription :
                    serviceConfig.getMaxConsumersPerSubscription();
        }

>>>>>>> f773c602c... Test pr 10 (#27)
        if (maxConsumersPerSubscription > 0 && maxConsumersPerSubscription <= consumerList.size()) {
            return true;
        }
        return false;
    }

    @Override
    public synchronized void removeConsumer(Consumer consumer) throws BrokerServiceException {
        // decrement unack-message count for removed consumer
        addUnAckedMessages(-consumer.getUnackedMessages());
        if (consumerSet.removeAll(consumer) == 1) {
            consumerList.remove(consumer);
            log.info("Removed consumer {} with pending {} acks", consumer, consumer.getPendingAcks().size());
            if (consumerList.isEmpty()) {
                if (havePendingRead && cursor.cancelPendingReadRequest()) {
                    havePendingRead = false;
                }

<<<<<<< HEAD
                messagesToReplay.clear();
=======
                messagesToRedeliver.clear();
                redeliveryTracker.clear();
>>>>>>> f773c602c... Test pr 10 (#27)
                if (closeFuture != null) {
                    log.info("[{}] All consumers removed. Subscription is disconnected", name);
                    closeFuture.complete(null);
                }
                totalAvailablePermits = 0;
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Consumer are left, reading more entries", name);
                }
                consumer.getPendingAcks().forEach((ledgerId, entryId, batchSize, none) -> {
<<<<<<< HEAD
                    messagesToReplay.add(ledgerId, entryId);
=======
                    messagesToRedeliver.add(ledgerId, entryId);
                    redeliveryTracker.addIfAbsent(PositionImpl.get(ledgerId, entryId));
>>>>>>> f773c602c... Test pr 10 (#27)
                });
                totalAvailablePermits -= consumer.getAvailablePermits();
                readMoreEntries();
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Trying to remove a non-connected consumer: {}", name, consumer);
            }
        }
    }

    @Override
    public synchronized void consumerFlow(Consumer consumer, int additionalNumberOfMessages) {
        if (!consumerSet.contains(consumer)) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Ignoring flow control from disconnected consumer {}", name, consumer);
            }
            return;
        }

        totalAvailablePermits += additionalNumberOfMessages;
<<<<<<< HEAD
=======

>>>>>>> f773c602c... Test pr 10 (#27)
        if (log.isDebugEnabled()) {
            log.debug("[{}-{}] Trigger new read after receiving flow control message with permits {}", name, consumer,
                    totalAvailablePermits);
        }
        readMoreEntries();
    }

    public void readMoreEntries() {
<<<<<<< HEAD
        if (totalAvailablePermits > 0 && isAtleastOneConsumerAvailable()) {
            int messagesToRead = Math.min(totalAvailablePermits, readBatchSize);
=======
        // totalAvailablePermits may be updated by other threads
        int currentTotalAvailablePermits = totalAvailablePermits;
        if (currentTotalAvailablePermits > 0 && isAtleastOneConsumerAvailable()) {
            int messagesToRead = Math.min(currentTotalAvailablePermits, readBatchSize);

            Consumer c = getRandomConsumer();
            // if turn on precise dispatcher flow control, adjust the record to read
            if (c != null && c.isPreciseDispatcherFlowControl()) {
                messagesToRead = Math.min((int) Math.ceil(currentTotalAvailablePermits * 1.0 / c.getAvgMessagesPerEntry()), readBatchSize);
            }

            if (!isConsumerWritable()) {
                // If the connection is not currently writable, we issue the read request anyway, but for a single
                // message. The intent here is to keep use the request as a notification mechanism while avoiding to
                // read and dispatch a big batch of messages which will need to wait before getting written to the
                // socket.
                messagesToRead = 1;
            }
>>>>>>> f773c602c... Test pr 10 (#27)

            // throttle only if: (1) cursor is not active (or flag for throttle-nonBacklogConsumer is enabled) bcz
            // active-cursor reads message from cache rather from bookkeeper (2) if topic has reached message-rate
            // threshold: then schedule the read after MESSAGE_RATE_BACKOFF_MS
            if (serviceConfig.isDispatchThrottlingOnNonBacklogConsumerEnabled() || !cursor.isActive()) {
                if (topic.getDispatchRateLimiter().isPresent() && topic.getDispatchRateLimiter().get().isDispatchRateLimitingEnabled()) {
                    DispatchRateLimiter topicRateLimiter = topic.getDispatchRateLimiter().get();
                    if (!topicRateLimiter.hasMessageDispatchPermit()) {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] message-read exceeded topic message-rate {}/{}, schedule after a {}", name,
                                    topicRateLimiter.getDispatchRateOnMsg(), topicRateLimiter.getDispatchRateOnByte(),
                                    MESSAGE_RATE_BACKOFF_MS);
                        }
                        topic.getBrokerService().executor().schedule(() -> readMoreEntries(), MESSAGE_RATE_BACKOFF_MS,
                                TimeUnit.MILLISECONDS);
                        return;
                    } else {
                        // if dispatch-rate is in msg then read only msg according to available permit
                        long availablePermitsOnMsg = topicRateLimiter.getAvailableDispatchRateLimitOnMsg();
                        if (availablePermitsOnMsg > 0) {
                            messagesToRead = Math.min(messagesToRead, (int) availablePermitsOnMsg);
                        }
                    }
                }

                if (dispatchRateLimiter.isPresent() && dispatchRateLimiter.get().isDispatchRateLimitingEnabled()) {
                    if (!dispatchRateLimiter.get().hasMessageDispatchPermit()) {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] message-read exceeded subscription message-rate {}/{}, schedule after a {}", name,
                                dispatchRateLimiter.get().getDispatchRateOnMsg(), dispatchRateLimiter.get().getDispatchRateOnByte(),
                                MESSAGE_RATE_BACKOFF_MS);
                        }
                        topic.getBrokerService().executor().schedule(() -> readMoreEntries(), MESSAGE_RATE_BACKOFF_MS,
                            TimeUnit.MILLISECONDS);
                        return;
                    } else {
                        // if dispatch-rate is in msg then read only msg according to available permit
                        long availablePermitsOnMsg = dispatchRateLimiter.get().getAvailableDispatchRateLimitOnMsg();
                        if (availablePermitsOnMsg > 0) {
                            messagesToRead = Math.min(messagesToRead, (int) availablePermitsOnMsg);
                        }
                    }
                }

            }

<<<<<<< HEAD
            if (!messagesToReplay.isEmpty()) {
                if (havePendingReplayRead) {
                    log.debug("[{}] Skipping replay while awaiting previous read to complete", name);
                    return;
                }

                Set<PositionImpl> messagesToReplayNow = messagesToReplay.items(messagesToRead).stream()
                        .map(pair -> new PositionImpl(pair.first, pair.second)).collect(toSet());

=======
            if (havePendingReplayRead) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Skipping replay while awaiting previous read to complete", name);
                }
                return;
            }

            // If messagesToRead is 0 or less, correct it to 1 to prevent IllegalArgumentException
            messagesToRead = Math.max(messagesToRead, 1);
            Set<PositionImpl> messagesToReplayNow = getMessagesToReplayNow(messagesToRead);

            if (!messagesToReplayNow.isEmpty()) {
>>>>>>> f773c602c... Test pr 10 (#27)
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Schedule replay of {} messages for {} consumers", name, messagesToReplayNow.size(),
                            consumerList.size());
                }

                havePendingReplayRead = true;
<<<<<<< HEAD
                Set<? extends Position> deletedMessages = cursor.asyncReplayEntries(messagesToReplayNow, this,
                        ReadType.Replay);
                // clear already acked positions from replay bucket

                deletedMessages.forEach(position -> messagesToReplay.remove(((PositionImpl) position).getLedgerId(),
                        ((PositionImpl) position).getEntryId()));
                // if all the entries are acked-entries and cleared up from messagesToReplay, try to read
=======
                Set<? extends Position> deletedMessages = topic.isDelayedDeliveryEnabled() ?
                        asyncReplayEntriesInOrder(messagesToReplayNow) : asyncReplayEntries(messagesToReplayNow);
                // clear already acked positions from replay bucket

                deletedMessages.forEach(position -> messagesToRedeliver.remove(((PositionImpl) position).getLedgerId(),
                        ((PositionImpl) position).getEntryId()));
                // if all the entries are acked-entries and cleared up from messagesToRedeliver, try to read
>>>>>>> f773c602c... Test pr 10 (#27)
                // next entries as readCompletedEntries-callback was never called
                if ((messagesToReplayNow.size() - deletedMessages.size()) == 0) {
                    havePendingReplayRead = false;
                    readMoreEntries();
                }
            } else if (BLOCKED_DISPATCHER_ON_UNACKMSG_UPDATER.get(this) == TRUE) {
                log.warn("[{}] Dispatcher read is blocked due to unackMessages {} reached to max {}", name,
<<<<<<< HEAD
                        totalUnackedMessages, maxUnackedMessages);
=======
                        totalUnackedMessages, topic.getMaxUnackedMessagesOnSubscription());
>>>>>>> f773c602c... Test pr 10 (#27)
            } else if (!havePendingRead) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Schedule read of {} messages for {} consumers", name, messagesToRead,
                            consumerList.size());
                }
                havePendingRead = true;
<<<<<<< HEAD
                cursor.asyncReadEntriesOrWait(messagesToRead, this, ReadType.Normal);
=======
                cursor.asyncReadEntriesOrWait(messagesToRead, serviceConfig.getDispatcherMaxReadSizeBytes(), this,
                        ReadType.Normal);
>>>>>>> f773c602c... Test pr 10 (#27)
            } else {
                log.debug("[{}] Cannot schedule next read until previous one is done", name);
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Consumer buffer is full, pause reading", name);
            }
        }
    }

<<<<<<< HEAD
=======
    protected Set<? extends Position> asyncReplayEntries(Set<? extends Position> positions) {
        return cursor.asyncReplayEntries(positions, this, ReadType.Replay);
    }

    protected Set<? extends Position> asyncReplayEntriesInOrder(Set<? extends Position> positions) {
        return cursor.asyncReplayEntries(positions, this, ReadType.Replay, true);
    }

>>>>>>> f773c602c... Test pr 10 (#27)
    @Override
    public boolean isConsumerConnected() {
        return !consumerList.isEmpty();
    }

    @Override
    public CopyOnWriteArrayList<Consumer> getConsumers() {
        return consumerList;
    }

    @Override
    public synchronized boolean canUnsubscribe(Consumer consumer) {
        return consumerList.size() == 1 && consumerSet.contains(consumer);
    }

    @Override
    public CompletableFuture<Void> close() {
        IS_CLOSED_UPDATER.set(this, TRUE);
<<<<<<< HEAD
=======

        Optional<DelayedDeliveryTracker> delayedDeliveryTracker;
        synchronized (this) {
            delayedDeliveryTracker = this.delayedDeliveryTracker;
            this.delayedDeliveryTracker = Optional.empty();
        }

        delayedDeliveryTracker.ifPresent(DelayedDeliveryTracker::close);

        dispatchRateLimiter.ifPresent(DispatchRateLimiter::close);

>>>>>>> f773c602c... Test pr 10 (#27)
        return disconnectAllConsumers();
    }

    @Override
<<<<<<< HEAD
    public synchronized CompletableFuture<Void> disconnectAllConsumers() {
=======
    public synchronized CompletableFuture<Void> disconnectAllConsumers(boolean isResetCursor) {
>>>>>>> f773c602c... Test pr 10 (#27)
        closeFuture = new CompletableFuture<>();
        if (consumerList.isEmpty()) {
            closeFuture.complete(null);
        } else {
<<<<<<< HEAD
            consumerList.forEach(Consumer::disconnect);
=======
            consumerList.forEach(consumer -> consumer.disconnect(isResetCursor));
>>>>>>> f773c602c... Test pr 10 (#27)
            if (havePendingRead && cursor.cancelPendingReadRequest()) {
                havePendingRead = false;
            }
        }
        return closeFuture;
    }

    @Override
<<<<<<< HEAD
    public void reset() {
=======
    public CompletableFuture<Void> disconnectActiveConsumers(boolean isResetCursor) {
        return disconnectAllConsumers(isResetCursor);
    }

    @Override
    public synchronized void resetCloseFuture() {
        closeFuture = null;
    }

    @Override
    public void reset() {
        resetCloseFuture();
>>>>>>> f773c602c... Test pr 10 (#27)
        IS_CLOSED_UPDATER.set(this, FALSE);
    }

    @Override
    public SubType getType() {
        return SubType.Shared;
    }

    @Override
    public synchronized void readEntriesComplete(List<Entry> entries, Object ctx) {
        ReadType readType = (ReadType) ctx;
<<<<<<< HEAD
        int start = 0;
        int entriesToDispatch = entries.size();

=======
>>>>>>> f773c602c... Test pr 10 (#27)
        if (readType == ReadType.Normal) {
            havePendingRead = false;
        } else {
            havePendingReplayRead = false;
        }

<<<<<<< HEAD
        if (readBatchSize < MaxReadBatchSize) {
            int newReadBatchSize = Math.min(readBatchSize * 2, MaxReadBatchSize);
=======
        if (readBatchSize < serviceConfig.getDispatcherMaxReadBatchSize()) {
            int newReadBatchSize = Math.min(readBatchSize * 2, serviceConfig.getDispatcherMaxReadBatchSize());
>>>>>>> f773c602c... Test pr 10 (#27)
            if (log.isDebugEnabled()) {
                log.debug("[{}] Increasing read batch size from {} to {}", name, readBatchSize, newReadBatchSize);
            }

            readBatchSize = newReadBatchSize;
        }

        readFailureBackoff.reduceToHalf();

        if (shouldRewindBeforeReadingOrReplaying && readType == ReadType.Normal) {
            // All consumers got disconnected before the completion of the read operation
            entries.forEach(Entry::release);
            cursor.rewind();
            shouldRewindBeforeReadingOrReplaying = false;
            readMoreEntries();
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}] Distributing {} messages to {} consumers", name, entries.size(), consumerList.size());
        }

<<<<<<< HEAD
        long totalMessagesSent = 0;
        long totalBytesSent = 0;
=======
        sendMessagesToConsumers(readType, entries);
    }

    protected void sendMessagesToConsumers(ReadType readType, List<Entry> entries) {

        if (needTrimAckedMessages()) {
            cursor.trimDeletedEntries(entries);
        }

        int entriesToDispatch = entries.size();
        // Trigger read more messages
        if (entriesToDispatch == 0) {
            readMoreEntries();
            return;
        }
        int start = 0;
        long totalMessagesSent = 0;
        long totalBytesSent = 0;

>>>>>>> f773c602c... Test pr 10 (#27)
        while (entriesToDispatch > 0 && totalAvailablePermits > 0 && isAtleastOneConsumerAvailable()) {
            Consumer c = getNextConsumer();
            if (c == null) {
                // Do nothing, cursor will be rewind at reconnection
                log.info("[{}] rewind because no available consumer found from total {}", name, consumerList.size());
                entries.subList(start, entries.size()).forEach(Entry::release);
                cursor.rewind();
                return;
            }

            // round-robin dispatch batch size for this consumer
<<<<<<< HEAD
            int messagesForC = Math.min(Math.min(entriesToDispatch, c.getAvailablePermits()), MaxRoundRobinBatchSize);
=======
            int availablePermits = c.isWritable() ? c.getAvailablePermits() : 1;
            if (log.isDebugEnabled() && !c.isWritable()) {
                log.debug("[{}-{}] consumer is not writable. dispatching only 1 message to {} ", topic.getName(), name,
                        c);
            }
            int messagesForC = Math.min(
                    Math.min(entriesToDispatch, availablePermits),
                    serviceConfig.getDispatcherMaxRoundRobinBatchSize());
>>>>>>> f773c602c... Test pr 10 (#27)

            if (messagesForC > 0) {

                // remove positions first from replay list first : sendMessages recycles entries
                if (readType == ReadType.Replay) {
                    entries.subList(start, start + messagesForC).forEach(entry -> {
<<<<<<< HEAD
                        messagesToReplay.remove(entry.getLedgerId(), entry.getEntryId());
                    });
                }

                SendMessageInfo sentMsgInfo = c.sendMessages(entries.subList(start, start + messagesForC));

                long msgSent = sentMsgInfo.getTotalSentMessages();
                start += messagesForC;
                entriesToDispatch -= messagesForC;
                totalAvailablePermits -= msgSent;
                totalMessagesSent += sentMsgInfo.getTotalSentMessages();
                totalBytesSent += sentMsgInfo.getTotalSentMessageBytes();
=======
                        messagesToRedeliver.remove(entry.getLedgerId(), entry.getEntryId());
                    });
                }

                SendMessageInfo sendMessageInfo = SendMessageInfo.getThreadLocal();
                List<Entry> entriesForThisConsumer = entries.subList(start, start + messagesForC);

                EntryBatchSizes batchSizes = EntryBatchSizes.get(entriesForThisConsumer.size());
                EntryBatchIndexesAcks batchIndexesAcks = EntryBatchIndexesAcks.get(entriesForThisConsumer.size());
                filterEntriesForConsumer(entriesForThisConsumer, batchSizes, sendMessageInfo, batchIndexesAcks, cursor,
                        readType == ReadType.Replay);

                c.sendMessages(entriesForThisConsumer, batchSizes, batchIndexesAcks, sendMessageInfo.getTotalMessages(),
                        sendMessageInfo.getTotalBytes(), sendMessageInfo.getTotalChunkedMessages(), redeliveryTracker);

                int msgSent = sendMessageInfo.getTotalMessages();
                start += messagesForC;
                entriesToDispatch -= messagesForC;
                TOTAL_AVAILABLE_PERMITS_UPDATER.addAndGet(this, -(msgSent - batchIndexesAcks.getTotalAckedIndexCount()));
                totalMessagesSent += sendMessageInfo.getTotalMessages();
                totalBytesSent += sendMessageInfo.getTotalBytes();
>>>>>>> f773c602c... Test pr 10 (#27)
            }
        }

        // acquire message-dispatch permits for already delivered messages
        if (serviceConfig.isDispatchThrottlingOnNonBacklogConsumerEnabled() || !cursor.isActive()) {
            if (topic.getDispatchRateLimiter().isPresent()) {
                topic.getDispatchRateLimiter().get().tryDispatchPermit(totalMessagesSent, totalBytesSent);
            }

            if (dispatchRateLimiter.isPresent()) {
                dispatchRateLimiter.get().tryDispatchPermit(totalMessagesSent, totalBytesSent);
            }
        }

        if (entriesToDispatch > 0) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] No consumers found with available permits, storing {} positions for later replay", name,
                        entries.size() - start);
            }
            entries.subList(start, entries.size()).forEach(entry -> {
<<<<<<< HEAD
                messagesToReplay.add(entry.getLedgerId(), entry.getEntryId());
                entry.release();
            });
        }

=======
                messagesToRedeliver.add(entry.getLedgerId(), entry.getEntryId());
                entry.release();
            });
        }
>>>>>>> f773c602c... Test pr 10 (#27)
        readMoreEntries();
    }

    @Override
    public synchronized void readEntriesFailed(ManagedLedgerException exception, Object ctx) {

        ReadType readType = (ReadType) ctx;
        long waitTimeMillis = readFailureBackoff.next();

        if (exception instanceof NoMoreEntriesToReadException) {
<<<<<<< HEAD
            if (cursor.getNumberOfEntriesInBacklog() == 0) {
=======
            if (cursor.getNumberOfEntriesInBacklog(false) == 0) {
>>>>>>> f773c602c... Test pr 10 (#27)
                // Topic has been terminated and there are no more entries to read
                // Notify the consumer only if all the messages were already acknowledged
                consumerList.forEach(Consumer::reachedEndOfTopic);
            }
<<<<<<< HEAD
=======
        } else if (exception.getCause() instanceof TransactionNotSealedException) {
            waitTimeMillis = 1;
            if (log.isDebugEnabled()) {
                log.debug("[{}] Error reading transaction entries : {}, Read Type {} - Retrying to read in {} seconds",
                        name, exception.getMessage(), readType, waitTimeMillis / 1000.0);
            }
>>>>>>> f773c602c... Test pr 10 (#27)
        } else if (!(exception instanceof TooManyRequestsException)) {
            log.error("[{}] Error reading entries at {} : {}, Read Type {} - Retrying to read in {} seconds", name,
                    cursor.getReadPosition(), exception.getMessage(), readType, waitTimeMillis / 1000.0);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Error reading entries at {} : {}, Read Type {} - Retrying to read in {} seconds", name,
                        cursor.getReadPosition(), exception.getMessage(), readType, waitTimeMillis / 1000.0);
            }
        }

        if (shouldRewindBeforeReadingOrReplaying) {
            shouldRewindBeforeReadingOrReplaying = false;
            cursor.rewind();
        }

        if (readType == ReadType.Normal) {
            havePendingRead = false;
        } else {
            havePendingReplayRead = false;
            if (exception instanceof ManagedLedgerException.InvalidReplayPositionException) {
                PositionImpl markDeletePosition = (PositionImpl) cursor.getMarkDeletedPosition();
<<<<<<< HEAD
                messagesToReplay.removeIf((ledgerId, entryId) -> {
=======

                messagesToRedeliver.removeIf((ledgerId, entryId) -> {
>>>>>>> f773c602c... Test pr 10 (#27)
                    return ComparisonChain.start().compare(ledgerId, markDeletePosition.getLedgerId())
                            .compare(entryId, markDeletePosition.getEntryId()).result() <= 0;
                });
            }
        }

<<<<<<< HEAD
        readBatchSize = 1;
=======
        readBatchSize = serviceConfig.getDispatcherMinReadBatchSize();
>>>>>>> f773c602c... Test pr 10 (#27)

        topic.getBrokerService().executor().schedule(() -> {
            synchronized (PersistentDispatcherMultipleConsumers.this) {
                if (!havePendingRead) {
                    log.info("[{}] Retrying read operation", name);
                    readMoreEntries();
                } else {
                    log.info("[{}] Skipping read retry: havePendingRead {}", name, havePendingRead, exception);
                }
            }
        }, waitTimeMillis, TimeUnit.MILLISECONDS);

    }

<<<<<<< HEAD

    /**
     * returns true only if {@link consumerList} has atleast one unblocked consumer and have available permits
     *
     * @return
     */
    private boolean isAtleastOneConsumerAvailable() {
=======
    private boolean needTrimAckedMessages() {
        if (lastIndividualDeletedRangeFromCursorRecovery == null) {
            return false;
        } else {
            return lastIndividualDeletedRangeFromCursorRecovery.upperEndpoint()
                    .compareTo((PositionImpl) cursor.getReadPosition()) > 0;
        }
    }

    /**
     * returns true only if {@link AbstractDispatcherMultipleConsumers#consumerList}
     * has atleast one unblocked consumer and have available permits
     *
     * @return
     */
    protected boolean isAtleastOneConsumerAvailable() {
>>>>>>> f773c602c... Test pr 10 (#27)
        if (consumerList.isEmpty() || IS_CLOSED_UPDATER.get(this) == TRUE) {
            // abort read if no consumers are connected or if disconnect is initiated
            return false;
        }
        for(Consumer consumer : consumerList) {
            if (isConsumerAvailable(consumer)) {
                return true;
            }
        }
        return false;
    }

<<<<<<< HEAD
=======
    private boolean isConsumerWritable() {
        for (Consumer consumer : consumerList) {
            if (consumer.isWritable()) {
                return true;
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("[{}-{}] consumer is not writable", topic.getName(), name);
        }
        return false;
    }

>>>>>>> f773c602c... Test pr 10 (#27)
    @Override
    public boolean isConsumerAvailable(Consumer consumer) {
        return consumer != null && !consumer.isBlocked() && consumer.getAvailablePermits() > 0;
    }

    @Override
    public synchronized void redeliverUnacknowledgedMessages(Consumer consumer) {
        consumer.getPendingAcks().forEach((ledgerId, entryId, batchSize, none) -> {
<<<<<<< HEAD
            messagesToReplay.add(ledgerId, entryId);
        });
        if (log.isDebugEnabled()) {
            log.debug("[{}-{}] Redelivering unacknowledged messages for consumer {}", name, consumer, messagesToReplay);
=======
            messagesToRedeliver.add(ledgerId, entryId);
        });
        if (log.isDebugEnabled()) {
            log.debug("[{}-{}] Redelivering unacknowledged messages for consumer {}", name, consumer,
                    messagesToRedeliver);
>>>>>>> f773c602c... Test pr 10 (#27)
        }
        readMoreEntries();
    }

    @Override
    public synchronized void redeliverUnacknowledgedMessages(Consumer consumer, List<PositionImpl> positions) {
        positions.forEach(position -> {
<<<<<<< HEAD
            messagesToReplay.add(position.getLedgerId(), position.getEntryId());
            redeliveryTracker.incrementAndGetRedeliveryCount(position);
=======
            messagesToRedeliver.add(position.getLedgerId(), position.getEntryId());
            redeliveryTracker.addIfAbsent(position);
>>>>>>> f773c602c... Test pr 10 (#27)
        });
        if (log.isDebugEnabled()) {
            log.debug("[{}-{}] Redelivering unacknowledged messages for consumer {}", name, consumer, positions);
        }
        readMoreEntries();
    }

    @Override
    public void addUnAckedMessages(int numberOfMessages) {
<<<<<<< HEAD
=======
        int maxUnackedMessages = topic.getMaxUnackedMessagesOnSubscription();
        if (maxUnackedMessages == -1) {
            maxUnackedMessages = topic.getBrokerService().pulsar().getConfiguration()
                    .getMaxUnackedMessagesPerSubscription();
        }
>>>>>>> f773c602c... Test pr 10 (#27)
        // don't block dispatching if maxUnackedMessages = 0
        if (maxUnackedMessages <= 0) {
            return;
        }
<<<<<<< HEAD
=======

>>>>>>> f773c602c... Test pr 10 (#27)
        int unAckedMessages = TOTAL_UNACKED_MESSAGES_UPDATER.addAndGet(this, numberOfMessages);
        if (unAckedMessages >= maxUnackedMessages
                && BLOCKED_DISPATCHER_ON_UNACKMSG_UPDATER.compareAndSet(this, FALSE, TRUE)) {
            // block dispatcher if it reaches maxUnAckMsg limit
            log.info("[{}] Dispatcher is blocked due to unackMessages {} reached to max {}", name,
                    TOTAL_UNACKED_MESSAGES_UPDATER.get(this), maxUnackedMessages);
        } else if (topic.getBrokerService().isBrokerDispatchingBlocked()
                && blockedDispatcherOnUnackedMsgs == TRUE) {
            // unblock dispatcher: if dispatcher is blocked due to broker-unackMsg limit and if it ack back enough
            // messages
            if (totalUnackedMessages < (topic.getBrokerService().maxUnackedMsgsPerDispatcher / 2)) {
                if (BLOCKED_DISPATCHER_ON_UNACKMSG_UPDATER.compareAndSet(this, TRUE, FALSE)) {
                    // it removes dispatcher from blocked list and unblocks dispatcher by scheduling read
                    topic.getBrokerService().unblockDispatchersOnUnAckMessages(Lists.newArrayList(this));
                }
            }
        } else if (blockedDispatcherOnUnackedMsgs == TRUE && unAckedMessages < maxUnackedMessages / 2) {
            // unblock dispatcher if it acks back enough messages
            if (BLOCKED_DISPATCHER_ON_UNACKMSG_UPDATER.compareAndSet(this, TRUE, FALSE)) {
                log.info("[{}] Dispatcher is unblocked", name);
                topic.getBrokerService().executor().execute(() -> readMoreEntries());
            }
        }
        // increment broker-level count
        topic.getBrokerService().addUnAckedMessages(this, numberOfMessages);
    }

    public boolean isBlockedDispatcherOnUnackedMsgs() {
        return blockedDispatcherOnUnackedMsgs == TRUE;
    }

    public void blockDispatcherOnUnackedMsgs() {
        blockedDispatcherOnUnackedMsgs = TRUE;
    }

    public void unBlockDispatcherOnUnackedMsgs() {
        blockedDispatcherOnUnackedMsgs = FALSE;
    }

    public int getTotalUnackedMessages() {
        return totalUnackedMessages;
    }

    public String getName() {
        return name;
    }

    @Override
    public RedeliveryTracker getRedeliveryTracker() {
        return redeliveryTracker;
    }

    @Override
    public Optional<DispatchRateLimiter> getRateLimiter() {
        return dispatchRateLimiter;
    }

    @Override
    public void initializeDispatchRateLimiterIfNeeded(Optional<Policies> policies) {
        if (!dispatchRateLimiter.isPresent() && DispatchRateLimiter
<<<<<<< HEAD
                .isDispatchRateNeeded(topic.getBrokerService(), policies, topic.getName(), name)) {
            this.dispatchRateLimiter = Optional.of(new DispatchRateLimiter(topic, name));
        }
    }
    
=======
                .isDispatchRateNeeded(topic.getBrokerService(), policies, topic.getName(), Type.SUBSCRIPTION)) {
            this.dispatchRateLimiter = Optional.of(new DispatchRateLimiter(topic, Type.SUBSCRIPTION));
        }
    }

    @Override
    public boolean trackDelayedDelivery(long ledgerId, long entryId, MessageMetadata msgMetadata) {
        if (!topic.isDelayedDeliveryEnabled()) {
            // If broker has the feature disabled, always deliver messages immediately
            return false;
        }

        synchronized (this) {
            if (!delayedDeliveryTracker.isPresent()) {
                // Initialize the tracker the first time we need to use it
                delayedDeliveryTracker = Optional
                        .of(topic.getBrokerService().getDelayedDeliveryTrackerFactory().newTracker(this));
            }

            delayedDeliveryTracker.get().resetTickTime(topic.getDelayedDeliveryTickTimeMillis());
            return delayedDeliveryTracker.get().addMessage(ledgerId, entryId, msgMetadata.getDeliverAtTime());
        }
    }

    protected synchronized Set<PositionImpl> getMessagesToReplayNow(int maxMessagesToRead) {
        if (!messagesToRedeliver.isEmpty()) {
            return messagesToRedeliver.items(maxMessagesToRead,
                    (ledgerId, entryId) -> new PositionImpl(ledgerId, entryId));
        } else if (delayedDeliveryTracker.isPresent() && delayedDeliveryTracker.get().hasMessageAvailable()) {
            delayedDeliveryTracker.get().resetTickTime(topic.getDelayedDeliveryTickTimeMillis());
            return delayedDeliveryTracker.get().getScheduledMessages(maxMessagesToRead);
        } else {
            return Collections.emptySet();
        }
    }

    @Override
    public synchronized long getNumberOfDelayedMessages() {
        return delayedDeliveryTracker.map(DelayedDeliveryTracker::getNumberOfDelayedMessages).orElse(0L);
    }

    @Override
    public void cursorIsReset() {
        if (this.lastIndividualDeletedRangeFromCursorRecovery != null) {
            this.lastIndividualDeletedRangeFromCursorRecovery = null;
        }
    }

    @Override
    public void addMessageToReplay(long ledgerId, long entryId) {
        this.messagesToRedeliver.add(ledgerId, entryId);
    }

    public PersistentTopic getTopic() {
        return topic;
    }

>>>>>>> f773c602c... Test pr 10 (#27)
    private static final Logger log = LoggerFactory.getLogger(PersistentDispatcherMultipleConsumers.class);
}
