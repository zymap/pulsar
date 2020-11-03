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

import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.common.util.OrderedScheduler;
<<<<<<< HEAD
import org.apache.bookkeeper.test.PortManager;
import org.apache.pulsar.zookeeper.ZookeeperBkClientFactoryImpl;
=======
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory.SessionType;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

<<<<<<< HEAD
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory.SessionType;

@Test
=======
>>>>>>> f773c602c... Test pr 10 (#27)
public class ZookeeperBkClientFactoryImplTest {

    private ZookeeperServerTest localZkS;
    private ZooKeeper localZkc;
<<<<<<< HEAD
    private final int LOCAL_ZOOKEEPER_PORT = PortManager.nextFreePort();
=======
>>>>>>> f773c602c... Test pr 10 (#27)
    private final long ZOOKEEPER_SESSION_TIMEOUT_MILLIS = 1000;
    private OrderedScheduler executor;

    @BeforeMethod
    void setup() throws Exception {
        executor = OrderedScheduler.newSchedulerBuilder().build();
<<<<<<< HEAD
        localZkS = new ZookeeperServerTest(LOCAL_ZOOKEEPER_PORT);
=======
        localZkS = new ZookeeperServerTest(0);
>>>>>>> f773c602c... Test pr 10 (#27)
        localZkS.start();
    }

    @AfterMethod
    void teardown() throws Exception {
        localZkS.close();
        executor.shutdown();
    }

    @Test
<<<<<<< HEAD
    void testZKCreationRW() throws Exception {
        ZooKeeperClientFactory zkf = new ZookeeperBkClientFactoryImpl(executor);
        CompletableFuture<ZooKeeper> zkFuture = zkf.create("127.0.0.1:" + LOCAL_ZOOKEEPER_PORT, SessionType.ReadWrite,
=======
    public void testZKCreationRW() throws Exception {
        ZooKeeperClientFactory zkf = new ZookeeperBkClientFactoryImpl(executor);
        CompletableFuture<ZooKeeper> zkFuture = zkf.create("127.0.0.1:" + localZkS.getZookeeperPort(), SessionType.ReadWrite,
>>>>>>> f773c602c... Test pr 10 (#27)
                (int) ZOOKEEPER_SESSION_TIMEOUT_MILLIS);
        localZkc = zkFuture.get(ZOOKEEPER_SESSION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        assertTrue(localZkc.getState().isConnected());
        assertNotEquals(localZkc.getState(), States.CONNECTEDREADONLY);
        localZkc.close();
    }

    @Test
<<<<<<< HEAD
    void testZKCreationRO() throws Exception {
        ZooKeeperClientFactory zkf = new ZookeeperBkClientFactoryImpl(executor);
        CompletableFuture<ZooKeeper> zkFuture = zkf.create("127.0.0.1:" + LOCAL_ZOOKEEPER_PORT,
=======
    public void testZKCreationRO() throws Exception {
        ZooKeeperClientFactory zkf = new ZookeeperBkClientFactoryImpl(executor);
        CompletableFuture<ZooKeeper> zkFuture = zkf.create("127.0.0.1:" + localZkS.getZookeeperPort(),
>>>>>>> f773c602c... Test pr 10 (#27)
                SessionType.AllowReadOnly, (int) ZOOKEEPER_SESSION_TIMEOUT_MILLIS);
        localZkc = zkFuture.get(ZOOKEEPER_SESSION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        assertTrue(localZkc.getState().isConnected());
        localZkc.close();
    }

    @Test
<<<<<<< HEAD
    void testZKCreationFailure() throws Exception {
=======
    public void testZKCreationFailure() throws Exception {
>>>>>>> f773c602c... Test pr 10 (#27)
        ZooKeeperClientFactory zkf = new ZookeeperBkClientFactoryImpl(executor);
        CompletableFuture<ZooKeeper> zkFuture = zkf.create("invalid", SessionType.ReadWrite,
                (int) ZOOKEEPER_SESSION_TIMEOUT_MILLIS);

        try {
            zkFuture.get();
            fail("Creation should have failed");
        } catch (ExecutionException e) {
            // Expected
        }
    }
}
