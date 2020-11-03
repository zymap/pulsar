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
package org.apache.pulsar.common.policies;

import java.net.URL;
import java.util.List;
import java.util.SortedSet;

import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.BrokerStatus;

<<<<<<< HEAD
public interface NamespaceIsolationPolicy {

    /**
     * Get the list of regex for the set of primary brokers
=======
/**
 * Namespace isolation policy.
 */
public interface NamespaceIsolationPolicy {

    /**
     * Get the list of regex for the set of primary brokers.
>>>>>>> f773c602c... Test pr 10 (#27)
     *
     * @return
     */
    List<String> getPrimaryBrokers();

    /**
<<<<<<< HEAD
     * Get the list of regex for the set of secondary brokers
=======
     * Get the list of regex for the set of secondary brokers.
>>>>>>> f773c602c... Test pr 10 (#27)
     *
     * @return
     */
    List<String> getSecondaryBrokers();

    /**
<<<<<<< HEAD
     * Get the list of primary brokers for the namespace according to the policy
=======
     * Get the list of primary brokers for the namespace according to the policy.
>>>>>>> f773c602c... Test pr 10 (#27)
     *
     * @param availableBrokers
     * @param namespace
     * @return
     */
    List<URL> findPrimaryBrokers(List<URL> availableBrokers, NamespaceName namespace);

    /**
<<<<<<< HEAD
     * Get the list of secondary brokers for the namespace according to the policy
=======
     * Get the list of secondary brokers for the namespace according to the policy.
>>>>>>> f773c602c... Test pr 10 (#27)
     *
     * @param availableBrokers
     * @param namespace
     * @return
     */
    List<URL> findSecondaryBrokers(List<URL> availableBrokers, NamespaceName namespace);

    /**
<<<<<<< HEAD
     * Check to see whether the primary brokers can still handle a new namespace or has to failover
=======
     * Check to see whether the primary brokers can still handle a new namespace or has to failover.
>>>>>>> f773c602c... Test pr 10 (#27)
     *
     * @param primaryCandidates
     * @return
     */
    boolean shouldFailover(SortedSet<BrokerStatus> primaryCandidates);

    /**
<<<<<<< HEAD
     * Check to see whether the primary brokers can still handle a new namespace or has to failover
=======
     * Check to see whether the primary brokers can still handle a new namespace or has to failover.
>>>>>>> f773c602c... Test pr 10 (#27)
     *
     * @param totalPrimaryCandidates
     * @return
     */
    boolean shouldFailover(int totalPrimaryCandidates);

    /**
<<<<<<< HEAD
     * Check to see whether the namespace ownership should fallback to the primary brokers
=======
     * Check to see whether the namespace ownership should fallback to the primary brokers.
>>>>>>> f773c602c... Test pr 10 (#27)
     *
     * @param primaryBrokers
     * @return
     */
    boolean shouldFallback(SortedSet<BrokerStatus> primaryBrokers);

    /**
<<<<<<< HEAD
     * Check to see whether the specific host is a primary broker
=======
     * Check to see whether the specific host is a primary broker.
>>>>>>> f773c602c... Test pr 10 (#27)
     *
     * @param brokerAddress
     * @return
     */
    boolean isPrimaryBroker(String brokerAddress);

    /**
<<<<<<< HEAD
     * Check to see whether the specific host is a secondary broker
=======
     * Check to see whether the specific host is a secondary broker.
>>>>>>> f773c602c... Test pr 10 (#27)
     *
     * @param brokerAddress
     * @return
     */
    boolean isSecondaryBroker(String brokerAddress);

    /**
<<<<<<< HEAD
     * According to the namespace isolation policy, find the allowed available primary brokers
=======
     * According to the namespace isolation policy, find the allowed available primary brokers.
>>>>>>> f773c602c... Test pr 10 (#27)
     *
     * @param primaryCandidates
     * @return
     */
    SortedSet<BrokerStatus> getAvailablePrimaryBrokers(SortedSet<BrokerStatus> primaryCandidates);

    boolean isPrimaryBrokerAvailable(BrokerStatus brkStatus);
}
