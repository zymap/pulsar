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
package org.apache.pulsar.zookeeper;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory.SessionType;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per ZK client ZooKeeper cache supporting ZNode data and children list caches. A cache entry is identified, accessed
 * and invalidated by the ZNode path. For the data cache, ZNode data parsing is done at request time with the given
 * {@link Deserializer} argument.
 *
 */
public class GlobalZooKeeperCache extends ZooKeeperCache implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(GlobalZooKeeperCache.class);

    private final ZooKeeperClientFactory zlClientFactory;
    private final int zkSessionTimeoutMillis;
    private final String globalZkConnect;
    private final ScheduledExecutorService scheduledExecutor;

    public GlobalZooKeeperCache(ZooKeeperClientFactory zkClientFactory, int zkSessionTimeoutMillis,
                                int zkOperationTimeoutSeconds, String globalZkConnect, OrderedExecutor orderedExecutor,
                                ScheduledExecutorService scheduledExecutor, int cacheExpirySeconds,
                                MetadataStore metadataStore) {
        super("global-zk", null, zkOperationTimeoutSeconds,
                orderedExecutor, cacheExpirySeconds, metadataStore);
        this.zlClientFactory = zkClientFactory;
        this.zkSessionTimeoutMillis = zkSessionTimeoutMillis;
        this.globalZkConnect = globalZkConnect;
        this.scheduledExecutor = scheduledExecutor;
    }

    public void start() throws IOException {
    }

    public void close() throws IOException {
    }
}
