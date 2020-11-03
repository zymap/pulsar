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
package org.apache.pulsar.client.api;

/**
<<<<<<< HEAD
 * Types of subscription supported by Pulsar
 *
 *
=======
 * Types of subscription supported by Pulsar.
>>>>>>> f773c602c... Test pr 10 (#27)
 */
public enum SubscriptionType {
    /**
     * There can be only 1 consumer on the same topic with the same subscription name.
     */
    Exclusive,

    /**
<<<<<<< HEAD
     * Multiple consumer will be able to use the same subscription name and the messages will be dispatched according to
     * a round-robin rotation between the connected consumers.
     * <p>
     * In this mode, the consumption order is not guaranteed.
=======
     * Multiple consumer will be able to use the same subscription name and the messages will be dispatched
     * according to a round-robin rotation between the connected consumers.
     *
     * <p>In this mode, the consumption order is not guaranteed.
>>>>>>> f773c602c... Test pr 10 (#27)
     */
    Shared,

    /**
     * Multiple consumer will be able to use the same subscription name but only 1 consumer will receive the messages.
     * If that consumer disconnects, one of the other connected consumers will start receiving messages.
<<<<<<< HEAD
     * <p>
     * In failover mode, the consumption ordering is guaranteed.
     * <p>
     * In case of partitioned topics, the ordering is guaranteed on a per-partition basis. The partitions assignments will
     * be split across the available consumers. On each partition, at most one consumer will be active at a given point
     * in time.
     */
    Failover
=======
     *
     * <p>In failover mode, the consumption ordering is guaranteed.
     *
     * <p>In case of partitioned topics, the ordering is guaranteed on a per-partition basis.
     * The partitions assignments will be split across the available consumers. On each partition,
     * at most one consumer will be active at a given point in time.
     */
    Failover,

    /**
     * Multiple consumer will be able to use the same subscription and all messages with the same key
     * will be dispatched to only one consumer.
     *
     * <p>Use ordering_key to overwrite the message key for message ordering.
     */
    Key_Shared
>>>>>>> f773c602c... Test pr 10 (#27)
}