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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerStats;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.NotSupportedException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ConsumerImpl.SubscriptionMode;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.util.ConsumerName;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiTopicsConsumerImpl<T> extends ConsumerBase<T> {

    public static final String DUMMY_TOPIC_NAME_PREFIX = "MultiTopicsConsumer-";

    // All topics should be in same namespace
    protected NamespaceName namespaceName;

    // Map <topic+partition, consumer>, when get do ACK, consumer will by find by topic name
    private final ConcurrentHashMap<String, ConsumerImpl<T>> consumers;

    // Map <topic, numPartitions>, store partition number for each topic
    protected final ConcurrentHashMap<String, Integer> topics;

    // Queue of partition consumers on which we have stopped calling receiveAsync() because the
    // shared incoming queue was full
    private final ConcurrentLinkedQueue<ConsumerImpl<T>> pausedConsumers;

    // Threshold for the shared queue. When the size of the shared queue goes below the threshold, we are going to
    // resume receiving from the paused consumer partitions
    private final int sharedQueueResumeThreshold;

    // sum of topicPartitions, simple topic has 1, partitioned topic equals to partition number.
    AtomicInteger allTopicPartitionsNumber;

    // timeout related to auto check and subscribe partition increasement
    private volatile Timeout partitionsAutoUpdateTimeout = null;
    TopicsPartitionChangedListener topicsPartitionChangedListener;
    CompletableFuture<Void> partitionsAutoUpdateFuture = null;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final ConsumerStatsRecorder stats;
    private final UnAckedMessageTracker unAckedMessageTracker;
    private final ConsumerConfigurationData<T> internalConfig;

    MultiTopicsConsumerImpl(PulsarClientImpl client, ConsumerConfigurationData<T> conf,
            ExecutorService listenerExecutor, CompletableFuture<Consumer<T>> subscribeFuture, Schema<T> schema,
            ConsumerInterceptors<T> interceptors) {
        this(client, DUMMY_TOPIC_NAME_PREFIX + ConsumerName.generateRandomName(), conf, listenerExecutor,
                subscribeFuture, schema, interceptors);
    }

    MultiTopicsConsumerImpl(PulsarClientImpl client, String singleTopic, ConsumerConfigurationData<T> conf,
            ExecutorService listenerExecutor, CompletableFuture<Consumer<T>> subscribeFuture, Schema<T> schema,
            ConsumerInterceptors<T> interceptors) {
        super(client, singleTopic, conf, Math.max(2, conf.getReceiverQueueSize()), listenerExecutor, subscribeFuture,
                schema, interceptors);

        checkArgument(conf.getReceiverQueueSize() > 0,
            "Receiver queue size needs to be greater than 0 for Topics Consumer");

        this.topics = new ConcurrentHashMap<>();
        this.consumers = new ConcurrentHashMap<>();
        this.pausedConsumers = new ConcurrentLinkedQueue<>();
        this.sharedQueueResumeThreshold = maxReceiverQueueSize / 2;
        this.allTopicPartitionsNumber = new AtomicInteger(0);

        if (conf.getAckTimeoutMillis() != 0) {
            if (conf.getTickDurationMillis() > 0) {
                this.unAckedMessageTracker = new UnAckedTopicMessageTracker(client, this, conf.getAckTimeoutMillis(), conf.getTickDurationMillis());
            } else {
                this.unAckedMessageTracker = new UnAckedTopicMessageTracker(client, this, conf.getAckTimeoutMillis());
            }
        } else {
            this.unAckedMessageTracker = UnAckedMessageTracker.UNACKED_MESSAGE_TRACKER_DISABLED;
        }

        this.internalConfig = getInternalConsumerConfig();
        this.stats = client.getConfiguration().getStatsIntervalSeconds() > 0 ? new ConsumerStatsRecorderImpl() : null;

        // start track and auto subscribe partition increasement
        if (conf.isAutoUpdatePartitions()) {
            topicsPartitionChangedListener = new TopicsPartitionChangedListener();
            partitionsAutoUpdateTimeout = client.timer()
                .newTimeout(partitionsAutoUpdateTimerTask, 1, TimeUnit.MINUTES);
        }

        if (conf.getTopicNames().isEmpty()) {
            this.namespaceName = null;
            setState(State.Ready);
            subscribeFuture().complete(MultiTopicsConsumerImpl.this);
            return;
        }

        checkArgument(conf.getTopicNames().isEmpty() || topicNamesValid(conf.getTopicNames()), "Topics should have same namespace.");
        this.namespaceName = conf.getTopicNames().stream().findFirst()
                .flatMap(s -> Optional.of(TopicName.get(s).getNamespaceObject())).get();

        List<CompletableFuture<Void>> futures = conf.getTopicNames().stream().map(this::subscribeAsync)
                .collect(Collectors.toList());
        FutureUtil.waitForAll(futures)
            .thenAccept(finalFuture -> {
                if (allTopicPartitionsNumber.get() > maxReceiverQueueSize) {
                    setMaxReceiverQueueSize(allTopicPartitionsNumber.get());
                }
                setState(State.Ready);
                // We have successfully created N consumers, so we can start receiving messages now
                startReceivingMessages(new ArrayList<>(consumers.values()));
                log.info("[{}] [{}] Created topics consumer with {} sub-consumers",
                    topic, subscription, allTopicPartitionsNumber.get());
                subscribeFuture().complete(MultiTopicsConsumerImpl.this);
            })
            .exceptionally(ex -> {
                log.warn("[{}] Failed to subscribe topics: {}", topic, ex.getMessage());
                subscribeFuture.completeExceptionally(ex);
                return null;
            });
    }

    // Check topics are valid.
    // - each topic is valid,
    // - every topic has same namespace,
    // - topic names are unique.
    private static boolean topicNamesValid(Collection<String> topics) {
        checkState(topics != null && topics.size() >= 1,
            "topics should contain more than 1 topic");

        final String namespace = TopicName.get(topics.stream().findFirst().get()).getNamespace();

        Optional<String> result = topics.stream()
            .filter(topic -> {
                boolean topicInvalid = !TopicName.isValid(topic);
                if (topicInvalid) {
                    return true;
                }

                String newNamespace =  TopicName.get(topic).getNamespace();
                if (!namespace.equals(newNamespace)) {
                    return true;
                } else {
                    return false;
                }
            }).findFirst();

        if (result.isPresent()) {
            log.warn("Received invalid topic name: {}", result.get());
            return false;
        }

        // check topic names are unique
        HashSet<String> set = new HashSet<>(topics);
        if (set.size() == topics.size()) {
            return true;
        } else {
            log.warn("Topic names not unique. unique/all : {}/{}", set.size(), topics.size());
            return false;
        }
    }

    private void startReceivingMessages(List<ConsumerImpl<T>> newConsumers) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] startReceivingMessages for {} new consumers in topics consumer, state: {}",
                topic, newConsumers.size(), getState());
        }
        if (getState() == State.Ready) {
            newConsumers.forEach(consumer -> {
                consumer.sendFlowPermitsToBroker(consumer.getConnectionHandler().cnx(), conf.getReceiverQueueSize());
                receiveMessageFromConsumer(consumer);
            });
        }
    }

    private void receiveMessageFromConsumer(ConsumerImpl<T> consumer) {
        consumer.receiveAsync().thenAccept(message -> {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Receive message from sub consumer:{}",
                    topic, subscription, consumer.getTopic());
            }
            // Process the message, add to the queue and trigger listener or async callback
            messageReceived(consumer, message);

            // we're modifying pausedConsumers
            lock.writeLock().lock();
            try {
                int size = incomingMessages.size();
                if (size >= maxReceiverQueueSize
                        || (size > sharedQueueResumeThreshold && !pausedConsumers.isEmpty())) {
                    // mark this consumer to be resumed later: if No more space left in shared queue,
                    // or if any consumer is already paused (to create fair chance for already paused consumers)
                    pausedConsumers.add(consumer);
                } else {
                    // Schedule next receiveAsync() if the incoming queue is not full. Use a different thread to avoid
                    // recursion and stack overflow
                    client.eventLoopGroup().execute(() -> {
                        receiveMessageFromConsumer(consumer);
                    });
                }
            } finally {
                lock.writeLock().unlock();
            }
        });
    }

    private void messageReceived(ConsumerImpl<T> consumer, Message<T> message) {
        checkArgument(message instanceof MessageImpl);
        lock.writeLock().lock();
        try {
            TopicMessageImpl<T> topicMessage = new TopicMessageImpl<>(
                consumer.getTopic(), consumer.getTopicNameWithoutPartition(), message);

            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Received message from topics-consumer {}",
                    topic, subscription, message.getMessageId());
            }

            // if asyncReceive is waiting : return message to callback without adding to incomingMessages queue
            if (!pendingReceives.isEmpty()) {
                CompletableFuture<Message<T>> receivedFuture = pendingReceives.poll();
                unAckedMessageTracker.add(topicMessage.getMessageId());
                listenerExecutor.execute(() -> receivedFuture.complete(topicMessage));
            } else {
                // Enqueue the message so that it can be retrieved when application calls receive()
                // Waits for the queue to have space for the message
                // This should never block cause MultiTopicsConsumerImpl should always use GrowableArrayBlockingQueue
                incomingMessages.put(topicMessage);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            lock.writeLock().unlock();
        }

        if (listener != null) {
            // Trigger the notification on the message listener in a separate thread to avoid blocking the networking
            // thread while the message processing happens
            listenerExecutor.execute(() -> {
                Message<T> msg;
                try {
                    msg = internalReceive();
                } catch (PulsarClientException e) {
                    log.warn("[{}] [{}] Failed to dequeue the message for listener", topic, subscription, e);
                    return;
                }

                try {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}][{}] Calling message listener for message {}",
                            topic, subscription, message.getMessageId());
                    }
                    listener.received(MultiTopicsConsumerImpl.this, msg);
                } catch (Throwable t) {
                    log.error("[{}][{}] Message listener error in processing message: {}",
                        topic, subscription, message, t);
                }
            });
        }
    }

    private void resumeReceivingFromPausedConsumersIfNeeded() {
        lock.readLock().lock();
        try {
            if (incomingMessages.size() <= sharedQueueResumeThreshold && !pausedConsumers.isEmpty()) {
                while (true) {
                    ConsumerImpl<T> consumer = pausedConsumers.poll();
                    if (consumer == null) {
                        break;
                    }

                    // if messages are readily available on consumer we will attempt to writeLock on the same thread
                    client.eventLoopGroup().execute(() -> {
                        receiveMessageFromConsumer(consumer);
                    });
                }
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    protected Message<T> internalReceive() throws PulsarClientException {
        Message<T> message;
        try {
            message = incomingMessages.take();
            checkState(message instanceof TopicMessageImpl);
            unAckedMessageTracker.add(message.getMessageId());
            resumeReceivingFromPausedConsumersIfNeeded();
            return message;
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    protected Message<T> internalReceive(int timeout, TimeUnit unit) throws PulsarClientException {
        Message<T> message;
        try {
            message = incomingMessages.poll(timeout, unit);
            if (message != null) {
                checkArgument(message instanceof TopicMessageImpl);
                unAckedMessageTracker.add(message.getMessageId());
            }
            resumeReceivingFromPausedConsumersIfNeeded();
            return message;
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    protected CompletableFuture<Message<T>> internalReceiveAsync() {
        CompletableFuture<Message<T>> result = new CompletableFuture<>();
        Message<T> message;
        try {
            lock.writeLock().lock();
            message = incomingMessages.poll(0, TimeUnit.SECONDS);
            if (message == null) {
                pendingReceives.add(result);
            } else {
                checkState(message instanceof TopicMessageImpl);
                unAckedMessageTracker.add(message.getMessageId());
                resumeReceivingFromPausedConsumersIfNeeded();
                result.complete(message);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            result.completeExceptionally(new PulsarClientException(e));
        } finally {
            lock.writeLock().unlock();
        }

        return result;
    }

    @Override
    protected CompletableFuture<Void> doAcknowledge(MessageId messageId, AckType ackType,
                                                    Map<String,Long> properties) {
        checkArgument(messageId instanceof TopicMessageIdImpl);
        TopicMessageIdImpl topicMessageId = (TopicMessageIdImpl) messageId;

        if (getState() != State.Ready) {
            return FutureUtil.failedFuture(new PulsarClientException("Consumer already closed"));
        }

        if (ackType == AckType.Cumulative) {
            Consumer individualConsumer = consumers.get(topicMessageId.getTopicPartitionName());
            if (individualConsumer != null) {
                MessageId innerId = topicMessageId.getInnerMessageId();
                return individualConsumer.acknowledgeCumulativeAsync(innerId);
            } else {
                return FutureUtil.failedFuture(new PulsarClientException.NotConnectedException());
            }
        } else {
            ConsumerImpl<T> consumer = consumers.get(topicMessageId.getTopicPartitionName());

            MessageId innerId = topicMessageId.getInnerMessageId();
            return consumer.doAcknowledge(innerId, ackType, properties)
                .thenRun(() ->
                    unAckedMessageTracker.remove(topicMessageId));
        }
    }

    @Override
    public void negativeAcknowledge(MessageId messageId) {
        checkArgument(messageId instanceof TopicMessageIdImpl);
        TopicMessageIdImpl topicMessageId = (TopicMessageIdImpl) messageId;

        ConsumerImpl<T> consumer = consumers.get(topicMessageId.getTopicPartitionName());
        consumer.negativeAcknowledge(topicMessageId.getInnerMessageId());
    }

    @Override
    public CompletableFuture<Void> unsubscribeAsync() {
        if (getState() == State.Closing || getState() == State.Closed) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.AlreadyClosedException("Topics Consumer was already closed"));
        }
        setState(State.Closing);

        CompletableFuture<Void> unsubscribeFuture = new CompletableFuture<>();
        List<CompletableFuture<Void>> futureList = consumers.values().stream()
            .map(c -> c.unsubscribeAsync()).collect(Collectors.toList());

        FutureUtil.waitForAll(futureList)
            .whenComplete((r, ex) -> {
                if (ex == null) {
                    setState(State.Closed);
                    unAckedMessageTracker.close();
                    unsubscribeFuture.complete(null);
                    log.info("[{}] [{}] [{}] Unsubscribed Topics Consumer",
                        topic, subscription, consumerName);
                } else {
                    setState(State.Failed);
                    unsubscribeFuture.completeExceptionally(ex);
                    log.error("[{}] [{}] [{}] Could not unsubscribe Topics Consumer",
                        topic, subscription, consumerName, ex.getCause());
                }
            });

        return unsubscribeFuture;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        if (getState() == State.Closing || getState() == State.Closed) {
            unAckedMessageTracker.close();
            return CompletableFuture.completedFuture(null);
        }
        setState(State.Closing);

        if (partitionsAutoUpdateTimeout != null) {
            partitionsAutoUpdateTimeout.cancel();
            partitionsAutoUpdateTimeout = null;
        }

        CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        List<CompletableFuture<Void>> futureList = consumers.values().stream()
            .map(c -> c.closeAsync()).collect(Collectors.toList());

        FutureUtil.waitForAll(futureList)
            .whenComplete((r, ex) -> {
                if (ex == null) {
                    setState(State.Closed);
                    unAckedMessageTracker.close();
                    closeFuture.complete(null);
                    log.info("[{}] [{}] Closed Topics Consumer", topic, subscription);
                    client.cleanupConsumer(this);
                    // fail all pending-receive futures to notify application
                    failPendingReceive();
                } else {
                    setState(State.Failed);
                    closeFuture.completeExceptionally(ex);
                    log.error("[{}] [{}] Could not close Topics Consumer", topic, subscription,
                        ex.getCause());
                }
            });

        return closeFuture;
    }

    private void failPendingReceive() {
        lock.readLock().lock();
        try {
            if (listenerExecutor != null && !listenerExecutor.isShutdown()) {
                while (!pendingReceives.isEmpty()) {
                    CompletableFuture<Message<T>> receiveFuture = pendingReceives.poll();
                    if (receiveFuture != null) {
                        receiveFuture.completeExceptionally(
                                new PulsarClientException.AlreadyClosedException("Consumer is already closed"));
                    } else {
                        break;
                    }
                }
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public boolean isConnected() {
        return consumers.values().stream().allMatch(consumer -> consumer.isConnected());
    }

    @Override
    protected String getHandlerName() {
        return subscription;
    }

    private ConsumerConfigurationData<T> getInternalConsumerConfig() {
        ConsumerConfigurationData<T> internalConsumerConfig = conf.clone();
        internalConsumerConfig.setSubscriptionName(subscription);
        internalConsumerConfig.setConsumerName(consumerName);
        internalConsumerConfig.setMessageListener(null);
        return internalConsumerConfig;
    }

    @Override
    public void redeliverUnacknowledgedMessages() {
        lock.writeLock().lock();
        try {
            consumers.values().stream().forEach(consumer -> consumer.redeliverUnacknowledgedMessages());
            incomingMessages.clear();
            unAckedMessageTracker.clear();
        } finally {
            lock.writeLock().unlock();
        }
        resumeReceivingFromPausedConsumersIfNeeded();
    }

    @Override
    public void redeliverUnacknowledgedMessages(Set<MessageId> messageIds) {
        if (messageIds.isEmpty()) {
            return;
        }

        checkArgument(messageIds.stream().findFirst().get() instanceof TopicMessageIdImpl);

        if (conf.getSubscriptionType() != SubscriptionType.Shared) {
            // We cannot redeliver single messages if subscription type is not Shared
            redeliverUnacknowledgedMessages();
            return;
        }
        removeExpiredMessagesFromQueue(messageIds);
        messageIds.stream().map(messageId -> (TopicMessageIdImpl)messageId)
            .collect(Collectors.groupingBy(TopicMessageIdImpl::getTopicPartitionName, Collectors.toSet()))
            .forEach((topicName, messageIds1) ->
                consumers.get(topicName)
                    .redeliverUnacknowledgedMessages(messageIds1.stream()
                        .map(mid -> mid.getInnerMessageId()).collect(Collectors.toSet())));
        resumeReceivingFromPausedConsumersIfNeeded();
    }

    @Override
    public void seek(MessageId messageId) throws PulsarClientException {
        try {
            seekAsync(messageId).get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public void seek(long timestamp) throws PulsarClientException {
        try {
            seekAsync(timestamp).get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<Void> seekAsync(MessageId messageId) {
        return FutureUtil.failedFuture(new PulsarClientException("Seek operation not supported on topics consumer"));
    }

    @Override
    public CompletableFuture<Void> seekAsync(long timestamp) {
        return FutureUtil.failedFuture(new PulsarClientException("Seek operation not supported on topics consumer"));
    }

    @Override
    public int getAvailablePermits() {
        return consumers.values().stream().mapToInt(ConsumerImpl::getAvailablePermits).sum();
    }

    @Override
    public boolean hasReachedEndOfTopic() {
        return consumers.values().stream().allMatch(Consumer::hasReachedEndOfTopic);
    }

    @Override
    public int numMessagesInQueue() {
        return incomingMessages.size() + consumers.values().stream().mapToInt(ConsumerImpl::numMessagesInQueue).sum();
    }

    @Override
    public synchronized ConsumerStats getStats() {
        if (stats == null) {
            return null;
        }
        stats.reset();

        consumers.values().stream().forEach(consumer -> stats.updateCumulativeStats(consumer.getStats()));
        return stats;
    }

    public UnAckedMessageTracker getUnAckedMessageTracker() {
        return unAckedMessageTracker;
    }

    private void removeExpiredMessagesFromQueue(Set<MessageId> messageIds) {
        Message<T> peek = incomingMessages.peek();
        if (peek != null) {
            if (!messageIds.contains(peek.getMessageId())) {
                // first message is not expired, then no message is expired in queue.
                return;
            }

            // try not to remove elements that are added while we remove
            Message<T> message = incomingMessages.poll();
            checkState(message instanceof TopicMessageImpl);
            while (message != null) {
                MessageId messageId = message.getMessageId();
                if (!messageIds.contains(messageId)) {
                    messageIds.add(messageId);
                    break;
                }
                message = incomingMessages.poll();
            }
        }
    }

    private boolean topicNameValid(String topicName) {
        checkArgument(TopicName.isValid(topicName), "Invalid topic name:" + topicName);
        checkArgument(!topics.containsKey(topicName), "Topics already contains topic:" + topicName);

        if (this.namespaceName != null) {
            checkArgument(TopicName.get(topicName).getNamespace().toString().equals(this.namespaceName.toString()),
                "Topic " + topicName + " not in same namespace with Topics");
        }

        return true;
    }

    // subscribe one more given topic
    public CompletableFuture<Void> subscribeAsync(String topicName) {
        if (!topicNameValid(topicName)) {
            return FutureUtil.failedFuture(
                new PulsarClientException.AlreadyClosedException("Topic name not valid"));
        }

        if (getState() == State.Closing || getState() == State.Closed) {
            return FutureUtil.failedFuture(
                new PulsarClientException.AlreadyClosedException("Topics Consumer was already closed"));
        }

        CompletableFuture<Void> subscribeResult = new CompletableFuture<>();

        client.getPartitionedTopicMetadata(topicName)
            .thenAccept(metadata -> subscribeTopicPartitions(subscribeResult, topicName, metadata.partitions))
            .exceptionally(ex1 -> {
                log.warn("[{}] Failed to get partitioned topic metadata: {}", topicName, ex1.getMessage());
                subscribeResult.completeExceptionally(ex1);
                return null;
            });

        return subscribeResult;
    }

    // create consumer for a single topic with already known partitions.
    // first create a consumer with no topic, then do subscription for already know partitionedTopic.
    public static <T> MultiTopicsConsumerImpl<T> createPartitionedConsumer(PulsarClientImpl client,
                                                                           ConsumerConfigurationData<T> conf,
                                                                           ExecutorService listenerExecutor,
                                                                           CompletableFuture<Consumer<T>> subscribeFuture,
                                                                           int numPartitions,
                                                                           Schema<T> schema, ConsumerInterceptors<T> interceptors) {
        checkArgument(conf.getTopicNames().size() == 1, "Should have only 1 topic for partitioned consumer");

        // get topic name, then remove it from conf, so constructor will create a consumer with no topic.
        ConsumerConfigurationData cloneConf = conf.clone();
        String topicName = cloneConf.getSingleTopic();
        cloneConf.getTopicNames().remove(topicName);

        CompletableFuture<Consumer> future = new CompletableFuture<>();
        MultiTopicsConsumerImpl consumer = new MultiTopicsConsumerImpl(client, topicName, cloneConf, listenerExecutor,
                future, schema, interceptors);

        future.thenCompose(c -> ((MultiTopicsConsumerImpl)c).subscribeAsync(topicName, numPartitions))
            .thenRun(()-> subscribeFuture.complete(consumer))
            .exceptionally(e -> {
                log.warn("Failed subscription for createPartitionedConsumer: {} {}, e:{}",
                    topicName, numPartitions,  e);
                subscribeFuture.completeExceptionally(((Throwable)e).getCause());
                return null;
            });;
        return consumer;
    }

    // subscribe one more given topic, but already know the numberPartitions
    private CompletableFuture<Void> subscribeAsync(String topicName, int numberPartitions) {
        if (!topicNameValid(topicName)) {
            return FutureUtil.failedFuture(
                new PulsarClientException.AlreadyClosedException("Topic name not valid"));
        }

        if (getState() == State.Closing || getState() == State.Closed) {
            return FutureUtil.failedFuture(
                new PulsarClientException.AlreadyClosedException("Topics Consumer was already closed"));
        }

        CompletableFuture<Void> subscribeResult = new CompletableFuture<>();
        subscribeTopicPartitions(subscribeResult, topicName, numberPartitions);

        return subscribeResult;
    }

    private void subscribeTopicPartitions(CompletableFuture<Void> subscribeResult, String topicName, int numPartitions) {
        client.preProcessSchemaBeforeSubscribe(client, schema, topicName).whenComplete((ignored, cause) -> {
            if (null == cause) {
                doSubscribeTopicPartitions(subscribeResult, topicName, numPartitions);
            } else {
                subscribeResult.completeExceptionally(cause);
            }
        });
    }

    private void doSubscribeTopicPartitions(CompletableFuture<Void> subscribeResult, String topicName, int numPartitions) {
        if (log.isDebugEnabled()) {
            log.debug("Subscribe to topic {} metadata.partitions: {}", topicName, numPartitions);
        }

        List<CompletableFuture<Consumer<T>>> futureList;
        if (numPartitions > 0) {
            this.topics.putIfAbsent(topicName, numPartitions);
            allTopicPartitionsNumber.addAndGet(numPartitions);

            int receiverQueueSize = Math.min(conf.getReceiverQueueSize(),
                conf.getMaxTotalReceiverQueueSizeAcrossPartitions() / numPartitions);
            ConsumerConfigurationData<T> configurationData = getInternalConsumerConfig();
            configurationData.setReceiverQueueSize(receiverQueueSize);

            futureList = IntStream
                .range(0, numPartitions)
                .mapToObj(
                    partitionIndex -> {
                        String partitionName = TopicName.get(topicName).getPartition(partitionIndex).toString();
                        CompletableFuture<Consumer<T>> subFuture = new CompletableFuture<>();
                        ConsumerImpl<T> newConsumer = ConsumerImpl.newConsumerImpl(client, partitionName,
                                configurationData, client.externalExecutorProvider().getExecutor(),
                                partitionIndex, true, subFuture,
                                SubscriptionMode.Durable, null, schema, interceptors,
                                client.getConfiguration().getDefaultBackoffIntervalNanos(),
                                client.getConfiguration().getMaxBackoffIntervalNanos());
                        consumers.putIfAbsent(newConsumer.getTopic(), newConsumer);
                        return subFuture;
                    })
                .collect(Collectors.toList());
        } else {
            this.topics.putIfAbsent(topicName, 1);
            allTopicPartitionsNumber.incrementAndGet();

            CompletableFuture<Consumer<T>> subFuture = new CompletableFuture<>();
            ConsumerImpl<T> newConsumer = ConsumerImpl.newConsumerImpl(client, topicName, internalConfig,
                    client.externalExecutorProvider().getExecutor(), -1, true, subFuture, SubscriptionMode.Durable, null,
                    schema, interceptors, client.getConfiguration().getDefaultBackoffIntervalNanos(),
                    client.getConfiguration().getMaxBackoffIntervalNanos());
            consumers.putIfAbsent(newConsumer.getTopic(), newConsumer);

            futureList = Collections.singletonList(subFuture);
        }

        FutureUtil.waitForAll(futureList)
            .thenAccept(finalFuture -> {
                if (allTopicPartitionsNumber.get() > maxReceiverQueueSize) {
                    setMaxReceiverQueueSize(allTopicPartitionsNumber.get());
                }
                int numTopics = this.topics.values().stream().mapToInt(Integer::intValue).sum();
                checkState(allTopicPartitionsNumber.get() == numTopics,
                    "allTopicPartitionsNumber " + allTopicPartitionsNumber.get()
                        + " not equals expected: " + numTopics);

                // We have successfully created new consumers, so we can start receiving messages for them
                startReceivingMessages(
                    consumers.values().stream()
                        .filter(consumer1 -> {
                            String consumerTopicName = consumer1.getTopic();
                            if (TopicName.get(consumerTopicName).getPartitionedTopicName().equals(
                                TopicName.get(topicName).getPartitionedTopicName().toString())) {
                                return true;
                            } else {
                                return false;
                            }
                        })
                        .collect(Collectors.toList()));

                subscribeResult.complete(null);
                log.info("[{}] [{}] Success subscribe new topic {} in topics consumer, partitions: {}, allTopicPartitionsNumber: {}",
                    topic, subscription, topicName, numPartitions, allTopicPartitionsNumber.get());
                if (this.namespaceName == null) {
                    this.namespaceName = TopicName.get(topicName).getNamespaceObject();
                }
                return;
            })
            .exceptionally(ex -> {
                handleSubscribeOneTopicError(topicName, ex, subscribeResult);
                return null;
            });
    }

    // handling failure during subscribe new topic, unsubscribe success created partitions
    private void handleSubscribeOneTopicError(String topicName, Throwable error, CompletableFuture<Void> subscribeFuture) {
        log.warn("[{}] Failed to subscribe for topic [{}] in topics consumer {}", topic, topicName, error.getMessage());

        client.externalExecutorProvider().getExecutor().submit(() -> {
            AtomicInteger toCloseNum = new AtomicInteger(0);
            consumers.values().stream().filter(consumer1 -> {
                String consumerTopicName = consumer1.getTopic();
                if (TopicName.get(consumerTopicName).getPartitionedTopicName().equals(topicName)) {
                    toCloseNum.incrementAndGet();
                    return true;
                } else {
                    return false;
                }
            }).collect(Collectors.toList()).forEach(consumer2 -> {
                consumer2.closeAsync().whenComplete((r, ex) -> {
                    consumer2.subscribeFuture().completeExceptionally(error);
                    allTopicPartitionsNumber.decrementAndGet();
                    consumers.remove(consumer2.getTopic());
                    if (toCloseNum.decrementAndGet() == 0) {
                        log.warn("[{}] Failed to subscribe for topic [{}] in topics consumer, subscribe error: {}",
                            topic, topicName, error.getMessage());
                        topics.remove(topicName);
                        checkState(allTopicPartitionsNumber.get() == consumers.values().size());
                        subscribeFuture.completeExceptionally(error);
                    }
                    return;
                });
            });
        });
    }

    // un-subscribe a given topic
    public CompletableFuture<Void> unsubscribeAsync(String topicName) {
        checkArgument(TopicName.isValid(topicName), "Invalid topic name:" + topicName);

        if (getState() == State.Closing || getState() == State.Closed) {
            return FutureUtil.failedFuture(
                new PulsarClientException.AlreadyClosedException("Topics Consumer was already closed"));
        }

        if (partitionsAutoUpdateTimeout != null) {
            partitionsAutoUpdateTimeout.cancel();
            partitionsAutoUpdateTimeout = null;
        }

        CompletableFuture<Void> unsubscribeFuture = new CompletableFuture<>();
        String topicPartName = TopicName.get(topicName).getPartitionedTopicName();

        List<ConsumerImpl<T>> consumersToUnsub = consumers.values().stream()
            .filter(consumer -> {
                String consumerTopicName = consumer.getTopic();
                if (TopicName.get(consumerTopicName).getPartitionedTopicName().equals(topicPartName)) {
                    return true;
                } else {
                    return false;
                }
            }).collect(Collectors.toList());

        List<CompletableFuture<Void>> futureList = consumersToUnsub.stream()
            .map(ConsumerImpl::unsubscribeAsync).collect(Collectors.toList());

        FutureUtil.waitForAll(futureList)
            .whenComplete((r, ex) -> {
                if (ex == null) {
                    consumersToUnsub.forEach(consumer1 -> {
                        consumers.remove(consumer1.getTopic());
                        pausedConsumers.remove(consumer1);
                        allTopicPartitionsNumber.decrementAndGet();
                    });

                    topics.remove(topicName);
                    ((UnAckedTopicMessageTracker) unAckedMessageTracker).removeTopicMessages(topicName);

                    unsubscribeFuture.complete(null);
                    log.info("[{}] [{}] [{}] Unsubscribed Topics Consumer, allTopicPartitionsNumber: {}",
                        topicName, subscription, consumerName, allTopicPartitionsNumber);
                } else {
                    unsubscribeFuture.completeExceptionally(ex);
                    setState(State.Failed);
                    log.error("[{}] [{}] [{}] Could not unsubscribe Topics Consumer",
                        topicName, subscription, consumerName, ex.getCause());
                }
            });

        return unsubscribeFuture;
    }

    // get topics name
    public List<String> getTopics() {
        return topics.keySet().stream().collect(Collectors.toList());
    }

    // get partitioned topics name
    public List<String> getPartitionedTopics() {
        return consumers.keySet().stream().collect(Collectors.toList());
    }

    // get partitioned consumers
    public List<ConsumerImpl<T>> getConsumers() {
        return consumers.values().stream().collect(Collectors.toList());
    }

    @Override
    public void pause() {
        consumers.forEach((name, consumer) -> consumer.pause());
    }

    @Override
    public void resume() {
        consumers.forEach((name, consumer) -> consumer.resume());
    }

    // This listener is triggered when topics partitions are updated.
    private class TopicsPartitionChangedListener implements PartitionsChangedListener {
        // Check partitions changes of passed in topics, and subscribe new added partitions.
        @Override
        public CompletableFuture<Void> onTopicsExtended(Collection<String> topicsExtended) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            if (topicsExtended.isEmpty()) {
                future.complete(null);
                return future;
            }

            if (log.isDebugEnabled()) {
                log.debug("[{}]  run onTopicsExtended: {}, size: {}",
                    topic, topicsExtended.toString(), topicsExtended.size());
            }

            List<CompletableFuture<Void>> futureList = Lists.newArrayListWithExpectedSize(topicsExtended.size());
            topicsExtended.forEach(topic -> futureList.add(subscribeIncreasedTopicPartitions(topic)));
            FutureUtil.waitForAll(futureList)
                .thenAccept(finalFuture -> future.complete(null))
                .exceptionally(ex -> {
                    log.warn("[{}] Failed to subscribe increased topics partitions: {}", topic, ex.getMessage());
                    future.completeExceptionally(ex);
                    return null;
                });

            return future;
        }
    }

    // subscribe increased partitions for a given topic
    private CompletableFuture<Void> subscribeIncreasedTopicPartitions(String topicName) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        client.getPartitionsForTopic(topicName).thenCompose(list -> {
            int oldPartitionNumber = topics.get(topicName.toString());
            int currentPartitionNumber = list.size();

            if (log.isDebugEnabled()) {
                log.debug("[{}] partitions number. old: {}, new: {}",
                    topicName.toString(), oldPartitionNumber, currentPartitionNumber);
            }

            if (oldPartitionNumber == currentPartitionNumber) {
                // topic partition number not changed
                future.complete(null);
                return future;
            } else if (oldPartitionNumber < currentPartitionNumber) {
                List<String> newPartitions = list.subList(oldPartitionNumber, currentPartitionNumber);
                // subscribe new added partitions
                List<CompletableFuture<Consumer<T>>> futureList = newPartitions
                    .stream()
                    .map(partitionName -> {
                        int partitionIndex = TopicName.getPartitionIndex(partitionName);
                        CompletableFuture<Consumer<T>> subFuture = new CompletableFuture<>();
                        ConsumerConfigurationData<T> configurationData = getInternalConsumerConfig();
                        ConsumerImpl<T> newConsumer = ConsumerImpl.newConsumerImpl(
                            client, partitionName, configurationData,
                            client.externalExecutorProvider().getExecutor(),
                            partitionIndex, true, subFuture, SubscriptionMode.Durable, null, schema, interceptors,
                            client.getConfiguration().getDefaultBackoffIntervalNanos(),
                            client.getConfiguration().getMaxBackoffIntervalNanos());
                        consumers.putIfAbsent(newConsumer.getTopic(), newConsumer);
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] create consumer {} for partitionName: {}",
                                topicName.toString(), newConsumer.getTopic(), partitionName);
                        }
                        return subFuture;
                    })
                    .collect(Collectors.toList());

                // wait for all partitions subscribe future complete, then startReceivingMessages
                FutureUtil.waitForAll(futureList)
                    .thenAccept(finalFuture -> {
                        List<ConsumerImpl<T>> newConsumerList = newPartitions.stream()
                            .map(partitionTopic -> consumers.get(partitionTopic))
                            .collect(Collectors.toList());

                        startReceivingMessages(newConsumerList);
                        future.complete(null);
                    })
                    .exceptionally(ex -> {
                        log.warn("[{}] Failed to subscribe {} partition: {} - {}",
                            topic, topicName.toString(), oldPartitionNumber, currentPartitionNumber, ex.getMessage());
                        future.completeExceptionally(ex);
                        return null;
                    });
            } else {
                log.error("[{}] not support shrink topic partitions. old: {}, new: {}",
                    topicName.toString(), oldPartitionNumber, currentPartitionNumber);
                future.completeExceptionally(new NotSupportedException("not support shrink topic partitions"));
            }
            return future;
        });

        return future;
    }

    private TimerTask partitionsAutoUpdateTimerTask = new TimerTask() {
        @Override
        public void run(Timeout timeout) throws Exception {
            if (timeout.isCancelled() || getState() != State.Ready) {
                return;
            }

            if (log.isDebugEnabled()) {
                log.debug("[{}]  run partitionsAutoUpdateTimerTask for multiTopicsConsumer: {}", topic);
            }

            // if last auto update not completed yet, do nothing.
            if (partitionsAutoUpdateFuture == null || partitionsAutoUpdateFuture.isDone()) {
                partitionsAutoUpdateFuture = topicsPartitionChangedListener.onTopicsExtended(topics.keySet());
            }

            // schedule the next re-check task
            partitionsAutoUpdateTimeout = client.timer()
                .newTimeout(partitionsAutoUpdateTimerTask, 1, TimeUnit.MINUTES);
        }
    };

    @VisibleForTesting
    public Timeout getPartitionsAutoUpdateTimeout() {
        return partitionsAutoUpdateTimeout;
    }

    private static final Logger log = LoggerFactory.getLogger(MultiTopicsConsumerImpl.class);
}
