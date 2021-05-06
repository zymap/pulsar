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
package org.apache.pulsar.metadata;

import com.alipay.remoting.config.Configs;
import com.alipay.sofa.jraft.rhea.client.DefaultRheaKVStore;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.options.PlacementDriverOptions;
import com.alipay.sofa.jraft.rhea.options.RegionRouteTableOptions;
import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions;
import com.alipay.sofa.jraft.rhea.options.configured.MultiRegionRouteTableOptionsConfigured;
import com.alipay.sofa.jraft.rhea.options.configured.PlacementDriverOptionsConfigured;
import com.alipay.sofa.jraft.rhea.options.configured.RheaKVStoreOptionsConfigured;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.impl.MSMetadataStore;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class MSMetadataStoreTest {

    @Test
    public void testKV() throws ExecutionException, InterruptedException {
        RheaKVStore kvStore = new DefaultRheaKVStore();
        final List<RegionRouteTableOptions> regionRouteTableOptionsList = MultiRegionRouteTableOptionsConfigured
            .newConfigured() //
            .withInitialServerList(-1L /* default id */, "127.0.0.1:8181,127.0.0.1:8182,127.0.0.1:8183") //
            .config();
        final PlacementDriverOptions pdOpts = PlacementDriverOptionsConfigured.newConfigured() //
            .withFake(true) //
            .withRegionRouteTableOptionsList(regionRouteTableOptionsList) //
            .config();
        final RheaKVStoreOptions opts = RheaKVStoreOptionsConfigured.newConfigured() //
            .withClusterName("rhea_example") //
            .withPlacementDriverOptions(pdOpts) //
            .config();
        System.out.println(opts);
        kvStore.init(opts);
        byte[] value = kvStore.get("unknown").get();
        System.out.println(value == null);
    }

    @Test
    public void testMsMetedataStore() throws ExecutionException, InterruptedException {
        MSMetadataStore store = new MSMetadataStore("", MetadataStoreConfig.builder()
            .build());
        store.put("public/default", "hello".getBytes(StandardCharsets.UTF_8), Optional.empty()).get();
        GetResult result = store.get("public/default").get().get();
        System.out.println(new String(result.getValue(), StandardCharsets.UTF_8));


        store.getChildren("public").get().forEach(path -> System.out.println(path));
        boolean exists = store.exists("public/default").get();
        System.out.println(exists);
        store.delete("public/default", Optional.empty()).get();
        store.getChildren("public").get().forEach(path -> System.out.println(path));
        exists = store.exists("public/default").get();
        System.out.println(exists);
    }
}
