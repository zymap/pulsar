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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.zookeeper.KeeperException.Code;
<<<<<<< HEAD
import org.apache.pulsar.zookeeper.ZooKeeperSessionWatcher;
=======
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

<<<<<<< HEAD
@Test
=======
>>>>>>> f773c602c... Test pr 10 (#27)
public class ZooKeeperSessionWatcherTest {

    private class MockShutdownService implements ZooKeeperSessionWatcher.ShutdownService {
        private int exitCode = 0;

        @Override
        public void shutdown(int exitCode) {
            this.exitCode = exitCode;
        }

        public int getExitCode() {
            return exitCode;
        }
    }

    private MockZooKeeper zkClient;
    private MockShutdownService shutdownService;
    private ZooKeeperSessionWatcher sessionWatcher;

    @BeforeMethod
    void setup() {
        zkClient = MockZooKeeper.newInstance();
        shutdownService = new MockShutdownService();
<<<<<<< HEAD
        sessionWatcher = new ZooKeeperSessionWatcher(zkClient, 1000, shutdownService);
=======
        sessionWatcher = new ZooKeeperSessionWatcher(zkClient, 1000, new ZookeeperSessionExpiredHandler() {

            private ZooKeeperSessionWatcher watcher;
            @Override
            public void onSessionExpired() {
                watcher.close();
                shutdownService.shutdown(-1);
            }

            @Override
            public void setWatcher(ZooKeeperSessionWatcher watcher) {
                this.watcher = watcher;
            }
        });
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @AfterMethod
    void teardown() throws Exception {
        sessionWatcher.close();
        zkClient.shutdown();
    }

    @Test
<<<<<<< HEAD
    void testProcess1() {
=======
    public void testProcess1() {
>>>>>>> f773c602c... Test pr 10 (#27)
        WatchedEvent event = new WatchedEvent(EventType.None, KeeperState.Expired, null);
        sessionWatcher.process(event);
        assertTrue(sessionWatcher.isShutdownStarted());
        assertEquals(shutdownService.getExitCode(), -1);
    }

    @Test
<<<<<<< HEAD
    void testProcess2() {
=======
    public void testProcess2() {
>>>>>>> f773c602c... Test pr 10 (#27)
        WatchedEvent event = new WatchedEvent(EventType.None, KeeperState.Disconnected, null);
        sessionWatcher.process(event);
        assertFalse(sessionWatcher.isShutdownStarted());
        assertEquals(shutdownService.getExitCode(), 0);
    }

    @Test
<<<<<<< HEAD
    void testProcess3() {
=======
    public void testProcess3() {
>>>>>>> f773c602c... Test pr 10 (#27)
        WatchedEvent event = new WatchedEvent(EventType.NodeCreated, KeeperState.Expired, null);
        sessionWatcher.process(event);
        assertFalse(sessionWatcher.isShutdownStarted());
        assertEquals(shutdownService.getExitCode(), 0);
    }

    @Test
<<<<<<< HEAD
    void testProcessResultConnectionLoss() {
=======
    public void testProcessResultConnectionLoss() {
>>>>>>> f773c602c... Test pr 10 (#27)
        sessionWatcher.processResult(Code.CONNECTIONLOSS.intValue(), null, null, null);
        assertEquals(sessionWatcher.getKeeperState(), KeeperState.Disconnected);
    }

    @Test
<<<<<<< HEAD
    void testProcessResultSessionExpired() {
=======
    public void testProcessResultSessionExpired() {
>>>>>>> f773c602c... Test pr 10 (#27)
        sessionWatcher.processResult(Code.SESSIONEXPIRED.intValue(), null, null, null);
        assertEquals(sessionWatcher.getKeeperState(), KeeperState.Expired);
    }

    @Test
<<<<<<< HEAD
    void testProcessResultOk() {
=======
    public void testProcessResultOk() {
>>>>>>> f773c602c... Test pr 10 (#27)
        sessionWatcher.processResult(Code.OK.intValue(), null, null, null);
        assertEquals(sessionWatcher.getKeeperState(), KeeperState.SyncConnected);
    }

    @Test
<<<<<<< HEAD
    void testProcessResultNoNode() {
=======
    public void testProcessResultNoNode() {
>>>>>>> f773c602c... Test pr 10 (#27)
        sessionWatcher.processResult(Code.NONODE.intValue(), null, null, null);
        assertEquals(sessionWatcher.getKeeperState(), KeeperState.SyncConnected);
    }

    @Test
    void testRun1() throws Exception {
<<<<<<< HEAD
        ZooKeeperSessionWatcher sessionWatcherZkNull = new ZooKeeperSessionWatcher(null, 1000, shutdownService);
=======
        ZooKeeperSessionWatcher sessionWatcherZkNull = new ZooKeeperSessionWatcher(null, 1000,
            new ZookeeperSessionExpiredHandler() {
                private ZooKeeperSessionWatcher watcher;
                @Override
                public void onSessionExpired() {
                    watcher.close();
                    shutdownService.shutdown(-1);
                }

                @Override
                public void setWatcher(ZooKeeperSessionWatcher watcher) {
                    this.watcher = watcher;
                }
            });
>>>>>>> f773c602c... Test pr 10 (#27)
        sessionWatcherZkNull.run();
        assertFalse(sessionWatcherZkNull.isShutdownStarted());
        assertEquals(sessionWatcherZkNull.getKeeperState(), KeeperState.Disconnected);
        assertEquals(shutdownService.getExitCode(), 0);
        sessionWatcherZkNull.close();
    }

    @Test
    void testRun2() throws Exception {
<<<<<<< HEAD
        ZooKeeperSessionWatcher sessionWatcherZkNull = new ZooKeeperSessionWatcher(null, 0, shutdownService);
=======
        ZooKeeperSessionWatcher sessionWatcherZkNull = new ZooKeeperSessionWatcher(null, 0,
            new ZookeeperSessionExpiredHandler() {

                private ZooKeeperSessionWatcher watcher;
                @Override
                public void onSessionExpired() {
                    watcher.close();
                    shutdownService.shutdown(-1);
                }

                @Override
                public void setWatcher(ZooKeeperSessionWatcher watcher) {
                    this.watcher = watcher;
                }
            });
>>>>>>> f773c602c... Test pr 10 (#27)
        sessionWatcherZkNull.run();
        assertTrue(sessionWatcherZkNull.isShutdownStarted());
        assertEquals(sessionWatcherZkNull.getKeeperState(), KeeperState.Disconnected);
        assertEquals(shutdownService.getExitCode(), -1);
        sessionWatcherZkNull.close();
    }

    @Test
<<<<<<< HEAD
    void testRun3() throws Exception {
=======
    public void testRun3() throws Exception {
>>>>>>> f773c602c... Test pr 10 (#27)
        zkClient.shutdown();
        sessionWatcher.run();
        assertFalse(sessionWatcher.isShutdownStarted());
        assertEquals(sessionWatcher.getKeeperState(), KeeperState.Disconnected);
        assertEquals(shutdownService.getExitCode(), 0);
    }

    @Test
<<<<<<< HEAD
    void testRun4() throws Exception {
=======
    public void testRun4() throws Exception {
>>>>>>> f773c602c... Test pr 10 (#27)
        sessionWatcher.run();
        assertFalse(sessionWatcher.isShutdownStarted());
        assertEquals(sessionWatcher.getKeeperState(), KeeperState.SyncConnected);
        assertEquals(shutdownService.getExitCode(), 0);
    }

    @Test
<<<<<<< HEAD
    void testRun5() throws Exception {
=======
    public void testRun5() throws Exception {
>>>>>>> f773c602c... Test pr 10 (#27)
        zkClient.create("/", new byte[0], null, null);
        sessionWatcher.run();
        assertFalse(sessionWatcher.isShutdownStarted());
        assertEquals(sessionWatcher.getKeeperState(), KeeperState.SyncConnected);
        assertEquals(shutdownService.getExitCode(), 0);
    }

    @Test
<<<<<<< HEAD
    void testRun6() throws Exception {
        zkClient.failAfter(0, Code.OK);
=======
    public void testRun6() throws Exception {
>>>>>>> f773c602c... Test pr 10 (#27)
        sessionWatcher.run();
        assertFalse(sessionWatcher.isShutdownStarted());
        assertEquals(sessionWatcher.getKeeperState(), KeeperState.SyncConnected);
        assertEquals(shutdownService.getExitCode(), 0);
    }

}
