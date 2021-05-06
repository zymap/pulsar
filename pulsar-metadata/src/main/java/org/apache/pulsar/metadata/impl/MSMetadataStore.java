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
package org.apache.pulsar.metadata.impl;

import com.alipay.sofa.jraft.rhea.client.DefaultRheaKVStore;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import lombok.Data;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.CreateOption;

import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class MSMetadataStore extends AbstractMetadataStore {
    private RheaKVStore kvStore;

    @Data
    private static class Value {
        final long version;
        final byte[] data;
        final long createdTimestamp;
        final long modifiedTimestamp;
    }

    public MSMetadataStore(String metadataURL, MetadataStoreConfig metadataStoreConfig) {
        kvStore = new DefaultRheaKVStore();
    }


    @Override
    public CompletableFuture<Optional<GetResult>> get(String path) {
        CompletableFuture<Optional<GetResult>> future = new CompletableFuture<>();
        kvStore.get(path).whenComplete((bytes, throwable) -> {
            if (throwable != null) {
                future.completeExceptionally(throwable);
            } else {
                future.complete(Optional.of(new GetResult(bytes, null)));
            }
        });
        return null;
    }

    @Override
    public CompletableFuture<Stat> put(String path, byte[] value, Optional<Long> expectedVersion) {
        put(path, value, expectedVersion, EnumSet.noneOf(CreateOption.class));
    }

    @Override
    protected CompletableFuture<List<String>> getChildrenFromStore(String path) {
        kvStore.get
    }

    @Override
    protected CompletableFuture<Boolean> existsFromStore(String path) {
        return kvStore.get(path).thenApply()
    }

    @Override
    protected CompletableFuture<Void> storeDelete(String path, Optional<Long> expectedVersion) {
        return kvStore.delete(path).thenApply(ignore -> null);
    }

    @Override
    protected CompletableFuture<Stat> storePut(String path, byte[] data, Optional<Long> optExpectedVersion, EnumSet<CreateOption> options) {
        return kvStore.put(path, data).thenApply(ignore -> new Stat(path, -1, -1, -1));
    }
}
