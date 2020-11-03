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
package org.apache.pulsar.functions.worker.rest;

import org.apache.pulsar.functions.worker.rest.api.FunctionsMetricsResource;
<<<<<<< HEAD
import org.apache.pulsar.functions.worker.rest.api.v2.FunctionApiV2Resource;
import org.apache.pulsar.functions.worker.rest.api.v3.FunctionApiV3Resource;
import org.apache.pulsar.functions.worker.rest.api.v3.SinkApiV3Resource;
import org.apache.pulsar.functions.worker.rest.api.v3.SourceApiV3Resource;
import org.apache.pulsar.functions.worker.rest.api.v2.WorkerApiV2Resource;
=======
import org.apache.pulsar.functions.worker.rest.api.v2.FunctionsApiV2Resource;
import org.apache.pulsar.functions.worker.rest.api.v2.WorkerApiV2Resource;
import org.apache.pulsar.functions.worker.rest.api.v2.WorkerStatsApiV2Resource;
import org.apache.pulsar.functions.worker.rest.api.v3.FunctionsApiV3Resource;
import org.apache.pulsar.functions.worker.rest.api.v3.SinkApiV3Resource;
import org.apache.pulsar.functions.worker.rest.api.v3.SinksApiV3Resource;
import org.apache.pulsar.functions.worker.rest.api.v3.SourceApiV3Resource;
import org.apache.pulsar.functions.worker.rest.api.v3.SourcesApiV3Resource;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public final class Resources {

    private Resources() {
    }

    public static Set<Class<?>> getApiV2Resources() {
        return new HashSet<>(
                Arrays.asList(
<<<<<<< HEAD
                        FunctionApiV2Resource.class,
                        WorkerApiV2Resource.class,
=======
                        FunctionsApiV2Resource.class,
                        WorkerApiV2Resource.class,
                        WorkerStatsApiV2Resource.class,
>>>>>>> f773c602c... Test pr 10 (#27)
                        MultiPartFeature.class
                ));
    }

    public static Set<Class<?>> getApiV3Resources() {
        return new HashSet<>(
                Arrays.asList(
                        MultiPartFeature.class,
<<<<<<< HEAD
                        SourceApiV3Resource.class,
                        SinkApiV3Resource.class,
                        FunctionApiV3Resource.class
=======
                        SourcesApiV3Resource.class,
                        SourceApiV3Resource.class,
                        SinksApiV3Resource.class,
                        SinkApiV3Resource.class,
                        FunctionsApiV3Resource.class
>>>>>>> f773c602c... Test pr 10 (#27)
                ));
    }

    public static Set<Class<?>> getRootResources() {
        return new HashSet<>(
                Arrays.asList(
                        ConfigurationResource.class,
<<<<<<< HEAD
                        FunctionsMetricsResource.class
=======
                        FunctionsMetricsResource.class,
                        WorkerReadinessResource.class
>>>>>>> f773c602c... Test pr 10 (#27)
                ));
    }
}