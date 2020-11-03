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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
=======
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
>>>>>>> f773c602c... Test pr 10 (#27)

import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.service.persistent.DispatchRateLimiter;
import org.apache.pulsar.broker.stats.ClusterReplicationMetrics;
import org.apache.pulsar.broker.stats.NamespaceStats;
<<<<<<< HEAD
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.schema.SchemaData;
import org.apache.pulsar.common.schema.SchemaVersion;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;
=======
import org.apache.pulsar.broker.transaction.buffer.TransactionBuffer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageIdData;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.apache.pulsar.utils.StatsOutputStream;

import io.netty.buffer.ByteBuf;

public interface Topic {

    interface PublishContext {

        default String getProducerName() {
            return null;
        }

        default long getSequenceId() {
<<<<<<< HEAD
            return -1;
=======
            return -1L;
>>>>>>> f773c602c... Test pr 10 (#27)
        }

        default void setOriginalProducerName(String originalProducerName) {
        }

        default void setOriginalSequenceId(long originalSequenceId) {
        }

        /**
         * Return the producer name for the original producer.
         *
         * For messages published locally, this will return the same local producer name, though in case of replicated
         * messages, the original producer name will differ
         */
        default String getOriginalProducerName() {
            return null;
        }

        default long getOriginalSequenceId() {
<<<<<<< HEAD
            return -1;
        }

        void completed(Exception e, long ledgerId, long entryId);
=======
            return -1L;
        }

        void completed(Exception e, long ledgerId, long entryId);

        default long getHighestSequenceId() {
            return  -1L;
        }

        default void setOriginalHighestSequenceId(long originalHighestSequenceId) {

        }

        default long getOriginalHighestSequenceId() {
            return  -1L;
        }
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    void publishMessage(ByteBuf headersAndPayload, PublishContext callback);

    void addProducer(Producer producer) throws BrokerServiceException;

    void removeProducer(Producer producer);

<<<<<<< HEAD
    CompletableFuture<Consumer> subscribe(ServerCnx cnx, String subscriptionName, long consumerId, SubType subType,
            int priorityLevel, String consumerName, boolean isDurable, MessageId startMessageId,
            Map<String, String> metadata, boolean readCompacted, InitialPosition initialPosition);

    CompletableFuture<Subscription> createSubscription(String subscriptionName, InitialPosition initialPosition);
=======
    /**
     * record add-latency
     */
    void recordAddLatency(long latency, TimeUnit unit);

    CompletableFuture<Consumer> subscribe(ServerCnx cnx, String subscriptionName, long consumerId, SubType subType,
            int priorityLevel, String consumerName, boolean isDurable, MessageId startMessageId,
            Map<String, String> metadata, boolean readCompacted, InitialPosition initialPosition,
            long startMessageRollbackDurationSec, boolean replicateSubscriptionState, PulsarApi.KeySharedMeta keySharedMeta);

    CompletableFuture<Subscription> createSubscription(String subscriptionName, InitialPosition initialPosition,
            boolean replicateSubscriptionState);
>>>>>>> f773c602c... Test pr 10 (#27)

    CompletableFuture<Void> unsubscribe(String subName);

    ConcurrentOpenHashMap<String, ? extends Subscription> getSubscriptions();

    CompletableFuture<Void> delete();

<<<<<<< HEAD
    ConcurrentOpenHashSet<Producer> getProducers();
=======
    Map<String, Producer> getProducers();
>>>>>>> f773c602c... Test pr 10 (#27)

    String getName();

    CompletableFuture<Void> checkReplication();

<<<<<<< HEAD
    CompletableFuture<Void> close();

    void checkGC(int gcInterval);

    void checkInactiveSubscriptions();

=======
    CompletableFuture<Void> close(boolean closeWithoutWaitingClientDisconnect);

    void checkGC();

    void checkInactiveSubscriptions();

    /**
     * Activate cursors those caught up backlog-threshold entries and deactivate slow cursors which are creating
     * backlog.
     */
    void checkBackloggedCursors();

>>>>>>> f773c602c... Test pr 10 (#27)
    void checkMessageExpiry();

    void checkMessageDeduplicationInfo();

<<<<<<< HEAD
=======
    void checkTopicPublishThrottlingRate();

    void incrementPublishCount(int numOfMessages, long msgSizeInBytes);

    void resetTopicPublishCountAndEnableReadIfRequired();

    void resetBrokerPublishCountAndEnableReadIfRequired(boolean doneReset);

    boolean isPublishRateExceeded();

    boolean isTopicPublishRateExceeded(int msgSize, int numMessages);

    boolean isBrokerPublishRateExceeded();

    void disableCnxAutoRead();

    void enableCnxAutoRead();

>>>>>>> f773c602c... Test pr 10 (#27)
    CompletableFuture<Void> onPoliciesUpdate(Policies data);

    boolean isBacklogQuotaExceeded(String producerName);

    boolean isEncryptionRequired();

<<<<<<< HEAD
=======
    boolean getSchemaValidationEnforced();

>>>>>>> f773c602c... Test pr 10 (#27)
    boolean isReplicated();

    BacklogQuota getBacklogQuota();

    void updateRates(NamespaceStats nsStats, NamespaceBundleStats currentBundleStats,
            StatsOutputStream topicStatsStream, ClusterReplicationMetrics clusterReplicationMetrics,
            String namespaceName, boolean hydratePublishers);

    Subscription getSubscription(String subscription);

    ConcurrentOpenHashMap<String, ? extends Replicator> getReplicators();

<<<<<<< HEAD
    TopicStats getStats();

    PersistentTopicInternalStats getInternalStats();

    Position getLastMessageId();
=======
    TopicStats getStats(boolean getPreciseBacklog);

    CompletableFuture<PersistentTopicInternalStats> getInternalStats(boolean includeLedgerMetadata);

    Position getLastPosition();

    CompletableFuture<MessageId> getLastMessageId();
>>>>>>> f773c602c... Test pr 10 (#27)

    /**
     * Whether a topic has had a schema defined for it.
     */
    CompletableFuture<Boolean> hasSchema();

    /**
     * Add a schema to the topic. This will fail if the new schema is incompatible with the current
     * schema.
     */
    CompletableFuture<SchemaVersion> addSchema(SchemaData schema);

    /**
<<<<<<< HEAD
     * Check if schema is compatible with current topic schema.
     */
    CompletableFuture<Boolean> isSchemaCompatible(SchemaData schema);
=======
     * Delete the schema if this topic has a schema defined for it.
     */
    CompletableFuture<SchemaVersion> deleteSchema();

    /**
     * Check if schema is compatible with current topic schema.
     */
    CompletableFuture<Void> checkSchemaCompatibleForConsumer(SchemaData schema);
>>>>>>> f773c602c... Test pr 10 (#27)

    /**
     * If the topic is idle (no producers, no entries, no subscribers and no existing schema),
     * add the passed schema to the topic. Otherwise, check that the passed schema is compatible
     * with what the topic already has.
     */
<<<<<<< HEAD
    CompletableFuture<Boolean> addSchemaIfIdleOrCheckCompatible(SchemaData schema);
=======
    CompletableFuture<Void> addSchemaIfIdleOrCheckCompatible(SchemaData schema);
>>>>>>> f773c602c... Test pr 10 (#27)

    CompletableFuture<Void> deleteForcefully();

    default Optional<DispatchRateLimiter> getDispatchRateLimiter() {
        return Optional.empty();
    }
<<<<<<< HEAD
=======

    default boolean isSystemTopic() {
        return false;
    }

    /* ------ Transaction related ------ */

    /**
     * Get the ${@link TransactionBuffer} of this Topic.
     *
     * @param createIfMissing Create the TransactionBuffer if missing.
     * @return TransactionBuffer CompletableFuture
     */
    CompletableFuture<TransactionBuffer> getTransactionBuffer(boolean createIfMissing);

    /**
     * Publish Transaction message to this Topic's TransactionBuffer
     *
     * @param txnID Transaction Id
     * @param headersAndPayload Message data
     * @param publishContext Publish context
     */
    void publishTxnMessage(TxnID txnID, ByteBuf headersAndPayload, PublishContext publishContext);

    /**
     * End the transaction in this topic.
     *
     * @param txnID Transaction id
     * @param txnAction Transaction action.
     * @return
     */
    CompletableFuture<Void> endTxn(TxnID txnID, int txnAction, List<MessageIdData> sendMessageIdList);

>>>>>>> f773c602c... Test pr 10 (#27)
}
