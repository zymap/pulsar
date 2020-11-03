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

import static org.apache.pulsar.client.util.MathUtils.signSafeMod;

import java.time.Clock;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.TopicMetadata;

/**
 * The routing strategy here:
 * <ul>
 * <li>If a key is present, choose a partition based on a hash of the key.
 * <li>If no key is present, choose a partition in a "round-robin" fashion. Batching-Awareness is built-in to improve
 * batching locality.
 * </ul>
 */
public class RoundRobinPartitionMessageRouterImpl extends MessageRouterBase {

    private static final long serialVersionUID = 1L;

    private static final AtomicIntegerFieldUpdater<RoundRobinPartitionMessageRouterImpl> PARTITION_INDEX_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(RoundRobinPartitionMessageRouterImpl.class, "partitionIndex");
    @SuppressWarnings("unused")
    private volatile int partitionIndex = 0;

    private final int startPtnIdx;
    private final boolean isBatchingEnabled;
<<<<<<< HEAD
    private final long maxBatchingDelayMs;
=======
    private final long partitionSwitchMs;
>>>>>>> f773c602c... Test pr 10 (#27)

    private final Clock clock;

    private static final Clock SYSTEM_CLOCK = Clock.systemUTC();

    public RoundRobinPartitionMessageRouterImpl(HashingScheme hashingScheme,
                                                int startPtnIdx,
                                                boolean isBatchingEnabled,
<<<<<<< HEAD
                                                long maxBatchingDelayMs) {
        this(hashingScheme, startPtnIdx, isBatchingEnabled, maxBatchingDelayMs, SYSTEM_CLOCK);
=======
                                                long partitionSwitchMs) {
        this(hashingScheme, startPtnIdx, isBatchingEnabled, partitionSwitchMs, SYSTEM_CLOCK);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    public RoundRobinPartitionMessageRouterImpl(HashingScheme hashingScheme,
                                                int startPtnIdx,
                                                boolean isBatchingEnabled,
<<<<<<< HEAD
                                                long maxBatchingDelayMs,
=======
                                                long partitionSwitchMs,
>>>>>>> f773c602c... Test pr 10 (#27)
                                                Clock clock) {
        super(hashingScheme);
        PARTITION_INDEX_UPDATER.set(this, startPtnIdx);
        this.startPtnIdx = startPtnIdx;
        this.isBatchingEnabled = isBatchingEnabled;
<<<<<<< HEAD
        this.maxBatchingDelayMs = Math.max(1, maxBatchingDelayMs);
=======
        this.partitionSwitchMs = Math.max(1, partitionSwitchMs);
>>>>>>> f773c602c... Test pr 10 (#27)
        this.clock = clock;
    }

    @Override
    public int choosePartition(Message<?> msg, TopicMetadata topicMetadata) {
        // If the message has a key, it supersedes the round robin routing policy
        if (msg.hasKey()) {
            return signSafeMod(hash.makeHash(msg.getKey()), topicMetadata.numPartitions());
        }

<<<<<<< HEAD
        if (isBatchingEnabled) { // if batching is enabled, choose partition on `maxBatchingDelayMs` boundary.
            long currentMs = clock.millis();
            return signSafeMod(currentMs / maxBatchingDelayMs + startPtnIdx, topicMetadata.numPartitions());
=======
        if (isBatchingEnabled) { // if batching is enabled, choose partition on `partitionSwitchMs` boundary.
            long currentMs = clock.millis();
            return signSafeMod(currentMs / partitionSwitchMs + startPtnIdx, topicMetadata.numPartitions());
>>>>>>> f773c602c... Test pr 10 (#27)
        } else {
            return signSafeMod(PARTITION_INDEX_UPDATER.getAndIncrement(this), topicMetadata.numPartitions());
        }
    }

}
