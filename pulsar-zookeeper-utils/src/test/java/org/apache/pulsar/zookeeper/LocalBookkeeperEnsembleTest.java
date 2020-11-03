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
<<<<<<< HEAD
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;

import org.apache.bookkeeper.test.PortManager;
=======
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.util.Collections;
import java.util.List;

>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

<<<<<<< HEAD
@Test
=======
>>>>>>> f773c602c... Test pr 10 (#27)
public class LocalBookkeeperEnsembleTest {

    @BeforeMethod
    void setup() throws Exception {
    }

    @AfterMethod
    void teardown() throws Exception {
    }

    @Test
<<<<<<< HEAD
    void testAdvertisedAddress() throws Exception {
        final int numBk = 1;
        final int zkPort = PortManager.nextFreePort();
        final int bkPort = PortManager.nextFreePort();

        LocalBookkeeperEnsemble ensemble = new LocalBookkeeperEnsemble(
            numBk, zkPort, bkPort, null, null, true, "127.0.0.2");
        ensemble.startStandalone();

        assertNotNull(ensemble.getZkClient().exists("/ledgers/available/127.0.0.2:" + bkPort, false));
=======
    public void testAdvertisedAddress() throws Exception {
        final int numBk = 1;

        LocalBookkeeperEnsemble ensemble = new LocalBookkeeperEnsemble(
            numBk, 0, 0, null, null, true, "127.0.0.2");
        ensemble.startStandalone();

        List<String> bookies = ensemble.getZkClient().getChildren("/ledgers/available", false);
        Collections.sort(bookies);
        assertEquals(bookies.size(), 2);
        assertTrue(bookies.get(0).startsWith("127.0.0.2:"));
>>>>>>> f773c602c... Test pr 10 (#27)

        ensemble.stop();
    }

    @Test
<<<<<<< HEAD
    void testStartStop() throws Exception {

        final int numBk = 1;
        final int zkPort = PortManager.nextFreePort();

        // Start local Bookies/ZooKeepers and confirm that they are running at specified ports
        LocalBookkeeperEnsemble ensemble = new LocalBookkeeperEnsemble(numBk, zkPort, () -> PortManager.nextFreePort());
        ensemble.start();
        assertTrue(ensemble.getZkServer().isRunning());
        assertEquals(ensemble.getZkServer().getClientPort(), zkPort);
=======
    public void testStartStop() throws Exception {

        final int numBk = 1;

        // Start local Bookies/ZooKeepers and confirm that they are running at specified ports
        LocalBookkeeperEnsemble ensemble = new LocalBookkeeperEnsemble(numBk, 0, () -> 0);
        ensemble.start();
        assertTrue(ensemble.getZkServer().isRunning());
        assertEquals(ensemble.getZkServer().getClientPort(), ensemble.getZookeeperPort());
>>>>>>> f773c602c... Test pr 10 (#27)
        assertTrue(ensemble.getZkClient().getState().isConnected());
        assertTrue(ensemble.getBookies()[0].isRunning());

        // Stop local Bookies/ZooKeepers and confirm that they are correctly closed
        ensemble.stop();
        assertFalse(ensemble.getZkServer().isRunning());
        assertFalse(ensemble.getZkClient().getState().isConnected());
        assertFalse(ensemble.getBookies()[0].isRunning());
    }
<<<<<<< HEAD

    @Test
    void testDataDirectoryCreatingAndRemoving() throws Exception {

        final int numBk = 1;
        final int zkPort = PortManager.nextFreePort();
        final int bkPort = PortManager.nextFreePort();
        final String zkDirName = "/tmp/data/zookeeper";
        File zkDir = new File(zkDirName);
        final String bkDirName = "/tmp/data/bookkeeper";
        File bkDir = new File(bkDirName + "0");

        // At first delete existing data directories
        FileUtils.deleteDirectory(zkDir);
        FileUtils.deleteDirectory(bkDir);
        assertFalse(zkDir.exists());
        assertFalse(bkDir.exists());

        // Start local Bookies/ZooKeepers and confirm that specified data directories are created
        LocalBookkeeperEnsemble ensemble1 = new LocalBookkeeperEnsemble(numBk, zkPort, bkPort, zkDirName, bkDirName,
                true);
        ensemble1.start();
        assertTrue(zkDir.exists());
        assertTrue(bkDir.exists());
        ensemble1.stop();

        // Restart local Bookies/ZooKeepers without refreshing data
        LocalBookkeeperEnsemble ensemble2 = new LocalBookkeeperEnsemble(numBk, zkPort, bkPort, zkDirName, bkDirName,
                false);
        ensemble2.start();
        assertTrue(ensemble2.getZkServer().isRunning());
        assertEquals(ensemble2.getZkServer().getClientPort(), zkPort);
        assertTrue(ensemble2.getZkClient().getState().isConnected());
        assertTrue(ensemble2.getBookies()[0].isRunning());
        assertEquals(ensemble2.getBookies()[0].getLocalAddress().getPort(), bkPort);

        // Stop local Bookies/ZooKeepers and confirm that they are correctly closed
        ensemble2.stop();
        assertFalse(ensemble2.getZkServer().isRunning());
        assertFalse(ensemble2.getZkClient().getState().isConnected());
        assertFalse(ensemble2.getBookies()[0].isRunning());

        // Finaly delete data directories
        FileUtils.deleteDirectory(zkDir);
        FileUtils.deleteDirectory(bkDir);
    }
=======
>>>>>>> f773c602c... Test pr 10 (#27)
}
