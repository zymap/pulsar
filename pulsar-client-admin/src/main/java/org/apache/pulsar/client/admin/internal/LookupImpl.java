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
package org.apache.pulsar.client.admin.internal;

<<<<<<< HEAD
=======
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.ws.rs.client.InvocationCallback;
>>>>>>> f773c602c... Test pr 10 (#27)
import javax.ws.rs.client.WebTarget;

import org.apache.pulsar.client.admin.Lookup;
import org.apache.pulsar.client.admin.PulsarAdminException;
<<<<<<< HEAD
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.lookup.data.LookupData;
import org.apache.pulsar.common.naming.TopicName;
=======
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.lookup.data.LookupData;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
>>>>>>> f773c602c... Test pr 10 (#27)

public class LookupImpl extends BaseResource implements Lookup {

    private final WebTarget v2lookup;
    private final boolean useTls;
<<<<<<< HEAD

    public LookupImpl(WebTarget web, Authentication auth, boolean useTls) {
        super(auth);
        this.useTls = useTls;
        v2lookup = web.path("/lookup/v2");
=======
    private final Topics topics;

    public LookupImpl(WebTarget web, Authentication auth, boolean useTls, long readTimeoutMs, Topics topics) {
        super(auth, readTimeoutMs);
        this.useTls = useTls;
        v2lookup = web.path("/lookup/v2");
        this.topics = topics;
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Override
    public String lookupTopic(String topic) throws PulsarAdminException {
<<<<<<< HEAD
        TopicName topicName = TopicName.get(topic);
        String prefix = topicName.isV2() ? "/topic" : "/destination";
        WebTarget target = v2lookup.path(prefix).path(topicName.getLookupName());

        try {
            return doTopicLookup(target);
        } catch (Exception e) {
            throw getApiException(e);
=======
        try {
            return lookupTopicAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
>>>>>>> f773c602c... Test pr 10 (#27)
        }
    }

    @Override
<<<<<<< HEAD
    public String getBundleRange(String topic) throws PulsarAdminException {
        TopicName topicName = TopicName.get(topic);
        String prefix = topicName.isV2() ? "/topic" : "/destination";
        WebTarget target = v2lookup.path(prefix).path(topicName.getLookupName()).path("bundle");

        try {
            return request(target).get(String.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    private String doTopicLookup(WebTarget lookupResource) throws PulsarAdminException {
        LookupData lookupData = request(lookupResource).get(LookupData.class);
        if (useTls) {
            return lookupData.getBrokerUrlTls();
        } else {
            return lookupData.getBrokerUrl();
        }
    }

=======
    public CompletableFuture<String> lookupTopicAsync(String topic) {
        TopicName topicName = TopicName.get(topic);
        String prefix = topicName.isV2() ? "/topic" : "/destination";
        WebTarget path = v2lookup.path(prefix).path(topicName.getLookupName());

        final CompletableFuture<String> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<LookupData>() {
                    @Override
                    public void completed(LookupData lookupData) {
                        if (useTls) {
                            future.complete(lookupData.getBrokerUrlTls());
                        } else {
                            future.complete(lookupData.getBrokerUrl());
                        }
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public Map<String, String> lookupPartitionedTopic(String topic) throws PulsarAdminException {
        try {
            return lookupPartitionedTopicAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Map<String, String>> lookupPartitionedTopicAsync(String topic) {
        CompletableFuture<Map<String, String>> future = new CompletableFuture<>();
        topics.getPartitionedTopicMetadataAsync(topic).thenAccept(partitionedTopicMetadata -> {
            int partitions = partitionedTopicMetadata.partitions;
            if (partitions <= 0) {
               future.completeExceptionally(
                        new PulsarAdminException("Topic " + topic + " is not a partitioned topic"));
               return;
            }

            Map<String, CompletableFuture<String>> lookupResult = new LinkedHashMap<>(partitions);
            for (int i = 0; i < partitions; i++) {
                String partitionTopicName = topic + "-partition-" + i;
                lookupResult.put(partitionTopicName, lookupTopicAsync(partitionTopicName));
            }

            FutureUtil.waitForAll(new ArrayList<>(lookupResult.values())).whenComplete((url, throwable) ->{
               if (throwable != null) {
                   future.completeExceptionally(getApiException(throwable.getCause()));
                   return;
               }
               Map<String, String> result = new LinkedHashMap<>();
               for (Map.Entry<String, CompletableFuture<String>> entry : lookupResult.entrySet()) {
                   try {
                       result.put(entry.getKey(), entry.getValue().get());
                   } catch (InterruptedException | ExecutionException e) {
                       future.completeExceptionally(e);
                       return;
                   }
               }
               future.complete(result);
            });

        }).exceptionally(throwable -> {
            future.completeExceptionally(getApiException(throwable.getCause()));
            return null;
        });

        return future;
    }


    @Override
    public String getBundleRange(String topic) throws PulsarAdminException {
        try {
            return getBundleRangeAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<String> getBundleRangeAsync(String topic) {
        TopicName topicName = TopicName.get(topic);
        String prefix = topicName.isV2() ? "/topic" : "/destination";
        WebTarget path = v2lookup.path(prefix).path(topicName.getLookupName()).path("bundle");
        final CompletableFuture<String> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<String>() {
                    @Override
                    public void completed(String bundleRange) {
                        future.complete(bundleRange);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

>>>>>>> f773c602c... Test pr 10 (#27)
}
