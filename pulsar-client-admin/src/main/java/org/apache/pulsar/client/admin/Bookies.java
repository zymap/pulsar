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
package org.apache.pulsar.client.admin;

<<<<<<< HEAD
=======
import java.util.concurrent.CompletableFuture;

>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.common.policies.data.BookieInfo;
import org.apache.pulsar.common.policies.data.BookiesRackConfiguration;

/**
 * Admin interface for bookies rack placement management.
 */
public interface Bookies {

    /**
<<<<<<< HEAD
     * Gets the rack placement information for all the bookies in the cluster
=======
     * Gets the rack placement information for all the bookies in the cluster.
>>>>>>> f773c602c... Test pr 10 (#27)
     */
    BookiesRackConfiguration getBookiesRackInfo() throws PulsarAdminException;

    /**
<<<<<<< HEAD
     * Gets the rack placement information for a specific bookie in the cluster
=======
     * Gets the rack placement information for all the bookies in the cluster asynchronously.
     */
    CompletableFuture<BookiesRackConfiguration> getBookiesRackInfoAsync();

    /**
     * Gets the rack placement information for a specific bookie in the cluster.
>>>>>>> f773c602c... Test pr 10 (#27)
     */
    BookieInfo getBookieRackInfo(String bookieAddress) throws PulsarAdminException;

    /**
<<<<<<< HEAD
     * Remove rack placement information for a specific bookie in the cluster
=======
     * Gets the rack placement information for a specific bookie in the cluster asynchronously.
     */
    CompletableFuture<BookieInfo> getBookieRackInfoAsync(String bookieAddress);

    /**
     * Remove rack placement information for a specific bookie in the cluster.
>>>>>>> f773c602c... Test pr 10 (#27)
     */
    void deleteBookieRackInfo(String bookieAddress) throws PulsarAdminException;

    /**
<<<<<<< HEAD
     * Updates the rack placement information for a specific bookie in the cluster
     */
    void updateBookieRackInfo(String bookieAddress, String group, BookieInfo bookieInfo) throws PulsarAdminException;
=======
     * Remove rack placement information for a specific bookie in the cluster asynchronously.
     */
    CompletableFuture<Void> deleteBookieRackInfoAsync(String bookieAddress);

    /**
     * Updates the rack placement information for a specific bookie in the cluster.
     */
    void updateBookieRackInfo(String bookieAddress, String group, BookieInfo bookieInfo) throws PulsarAdminException;

    /**
     * Updates the rack placement information for a specific bookie in the cluster asynchronously.
     */
    CompletableFuture<Void> updateBookieRackInfoAsync(String bookieAddress, String group, BookieInfo bookieInfo);
>>>>>>> f773c602c... Test pr 10 (#27)
}
