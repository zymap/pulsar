/*
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
 *
 */

package org.apache.pulsar.packages.manager.service;

import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.distributedlog.exceptions.ZKException;
import org.apache.distributedlog.impl.metadata.BKDLConfig;
import org.apache.distributedlog.metadata.DLMetadata;
import org.apache.pulsar.packages.manager.PackageStorage;
import org.apache.pulsar.packages.manager.PackageStorageConfig;
import org.apache.pulsar.packages.manager.PackageStorageProvider;
import org.apache.pulsar.packages.manager.impl.PackageImpl;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutionException;

@Slf4j
@Getter
public class PackageManagerService {

    private PackageImpl packageImpl;
    private PackageStorage packageStorage;
    private PackageStorageConfig config;

    public PackageManagerService(PackageStorageConfig config) {
        this.config = config;
    }

    public void start() throws InterruptedException {
        try {
            URI dlogURI = initializeDlogNamespace(config.getZkServers(), config.getLedgersRootPath());
            config.setDlogUrl(dlogURI);
            PackageStorage storage = PackageStorageProvider.newProvider(config.getStorageProviderClassName())
                .getStorage(config).get();
            packageStorage = storage;
            packageImpl = new PackageImpl(storage);
        } catch (IOException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private URI initializeDlogNamespace(String zkServers, String ledgersRootPath) throws IOException {
        BKDLConfig bkdlConfig = new BKDLConfig(zkServers, ledgersRootPath);
        DLMetadata dlMetadata = DLMetadata.create(bkdlConfig);
        URI dlogUri = URI.create(String.format("distributedlog://%s/pulsar/package-manager", zkServers));
        try {
            dlMetadata.create(dlogUri);
        } catch (ZKException e) {
            if (e.getKeeperExceptionCode() == KeeperException.Code.NODEEXISTS) {
                return dlogUri;
            }
            throw e;
        }
        return dlogUri;
    }

    public void stop() {
        try {
            packageStorage.closeAsync().get();
        } catch (InterruptedException | ExecutionException e) {
            log.warn("Failed to close the package storage", e);
        }
    }
}
