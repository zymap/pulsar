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

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
<<<<<<< HEAD
=======
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
>>>>>>> f773c602c... Test pr 10 (#27)

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import org.apache.pulsar.client.admin.NonPersistentTopics;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
<<<<<<< HEAD
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.naming.NamespaceName;
=======
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.NonPersistentTopicStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;

public class NonPersistentTopicsImpl extends BaseResource implements NonPersistentTopics {

    private final WebTarget adminNonPersistentTopics;
    private final WebTarget adminV2NonPersistentTopics;

<<<<<<< HEAD
    public NonPersistentTopicsImpl(WebTarget web, Authentication auth) {
        super(auth);
=======
    public NonPersistentTopicsImpl(WebTarget web, Authentication auth, long readTimeoutMs) {
        super(auth, readTimeoutMs);
>>>>>>> f773c602c... Test pr 10 (#27)
        adminNonPersistentTopics = web.path("/admin");
        adminV2NonPersistentTopics = web.path("/admin/v2");
    }

    @Override
    public void createPartitionedTopic(String topic, int numPartitions) throws PulsarAdminException {
        try {
<<<<<<< HEAD
            createPartitionedTopicAsync(topic, numPartitions).get();
=======
            createPartitionedTopicAsync(topic, numPartitions).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
>>>>>>> f773c602c... Test pr 10 (#27)
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
<<<<<<< HEAD
            throw new PulsarAdminException(e.getCause());
=======
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
>>>>>>> f773c602c... Test pr 10 (#27)
        }
    }

    @Override
    public CompletableFuture<Void> createPartitionedTopicAsync(String topic, int numPartitions) {
<<<<<<< HEAD
        checkArgument(numPartitions > 1, "Number of partitions should be more than 1");
=======
        checkArgument(numPartitions > 0, "Number of partitions should be more than 0");
>>>>>>> f773c602c... Test pr 10 (#27)
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "partitions");
        return asyncPutRequest(path, Entity.entity(numPartitions, MediaType.APPLICATION_JSON));
    }

    @Override
    public PartitionedTopicMetadata getPartitionedTopicMetadata(String topic) throws PulsarAdminException {
        try {
<<<<<<< HEAD
            return getPartitionedTopicMetadataAsync(topic).get();
=======
            return getPartitionedTopicMetadataAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
>>>>>>> f773c602c... Test pr 10 (#27)
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
<<<<<<< HEAD
            throw new PulsarAdminException(e.getCause());
=======
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
>>>>>>> f773c602c... Test pr 10 (#27)
        }
    }

    @Override
    public CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadataAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        final CompletableFuture<PartitionedTopicMetadata> future = new CompletableFuture<>();
        WebTarget path = topicPath(topicName, "partitions");
        asyncGetRequest(path,
                new InvocationCallback<PartitionedTopicMetadata>() {

                    @Override
                    public void completed(PartitionedTopicMetadata response) {
                        future.complete(response);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public NonPersistentTopicStats getStats(String topic) throws PulsarAdminException {
        try {
<<<<<<< HEAD
            return getStatsAsync(topic).get();
=======
            return getStatsAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
>>>>>>> f773c602c... Test pr 10 (#27)
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
<<<<<<< HEAD
            throw new PulsarAdminException(e.getCause());
=======
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
>>>>>>> f773c602c... Test pr 10 (#27)
        }
    }

    @Override
    public CompletableFuture<NonPersistentTopicStats> getStatsAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        final CompletableFuture<NonPersistentTopicStats> future = new CompletableFuture<>();
        WebTarget path = topicPath(topicName, "stats");
        asyncGetRequest(path,
                new InvocationCallback<NonPersistentTopicStats>() {

                    @Override
                    public void completed(NonPersistentTopicStats response) {
                        future.complete(response);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public PersistentTopicInternalStats getInternalStats(String topic) throws PulsarAdminException {
        try {
<<<<<<< HEAD
            return getInternalStatsAsync(topic).get();
=======
            return getInternalStatsAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
>>>>>>> f773c602c... Test pr 10 (#27)
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
<<<<<<< HEAD
            throw new PulsarAdminException(e.getCause());
=======
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
>>>>>>> f773c602c... Test pr 10 (#27)
        }
    }

    @Override
    public CompletableFuture<PersistentTopicInternalStats> getInternalStatsAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        final CompletableFuture<PersistentTopicInternalStats> future = new CompletableFuture<>();
        WebTarget path = topicPath(topicName, "internalStats");
        asyncGetRequest(path,
                new InvocationCallback<PersistentTopicInternalStats>() {

                    @Override
                    public void completed(PersistentTopicInternalStats response) {
                        future.complete(response);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void unload(String topic) throws PulsarAdminException {
        try {
<<<<<<< HEAD
            unloadAsync(topic).get();
=======
            unloadAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
>>>>>>> f773c602c... Test pr 10 (#27)
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
<<<<<<< HEAD
            throw new PulsarAdminException(e.getCause());
=======
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
>>>>>>> f773c602c... Test pr 10 (#27)
        }
    }

    @Override
    public CompletableFuture<Void> unloadAsync(String topic) {
        TopicName topicName = validateTopic(topic);
        WebTarget path = topicPath(topicName, "unload");
        return asyncPutRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public List<String> getListInBundle(String namespace, String bundleRange) throws PulsarAdminException {
        try {
<<<<<<< HEAD
            return getListInBundleAsync(namespace, bundleRange).get();
=======
            return getListInBundleAsync(namespace, bundleRange).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
>>>>>>> f773c602c... Test pr 10 (#27)
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
<<<<<<< HEAD
            throw new PulsarAdminException(e.getCause());
=======
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
>>>>>>> f773c602c... Test pr 10 (#27)
        }
    }

    @Override
    public CompletableFuture<List<String>> getListInBundleAsync(String namespace, String bundleRange) {
        NamespaceName ns = NamespaceName.get(namespace);
        final CompletableFuture<List<String>> future = new CompletableFuture<>();
        WebTarget path = namespacePath("non-persistent", ns, bundleRange);
        asyncGetRequest(path,
                new InvocationCallback<List<String>>() {
                    @Override
                    public void completed(List<String> response) {
                        future.complete(response);
                    }
                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public List<String> getList(String namespace) throws PulsarAdminException {
        try {
<<<<<<< HEAD
            return getListAsync(namespace).get();
=======
            return getListAsync(namespace).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
>>>>>>> f773c602c... Test pr 10 (#27)
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
<<<<<<< HEAD
            throw new PulsarAdminException(e.getCause());
=======
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
>>>>>>> f773c602c... Test pr 10 (#27)
        }
    }

    @Override
    public CompletableFuture<List<String>> getListAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        final CompletableFuture<List<String>> future = new CompletableFuture<>();
        WebTarget path = namespacePath("non-persistent", ns);
        asyncGetRequest(path,
                new InvocationCallback<List<String>>() {
                    @Override
                    public void completed(List<String> response) {
                        future.complete(response);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    /*
     * returns topic name with encoded Local Name
     */
    private TopicName validateTopic(String topic) {
        // Parsing will throw exception if name is not valid
        return TopicName.get(topic);
    }

    private WebTarget namespacePath(String domain, NamespaceName namespace, String... parts) {
        final WebTarget base = namespace.isV2() ? adminV2NonPersistentTopics : adminNonPersistentTopics;
        WebTarget namespacePath = base.path(domain).path(namespace.toString());
        namespacePath = WebTargets.addParts(namespacePath, parts);
        return namespacePath;
    }

    private WebTarget topicPath(TopicName topic, String... parts) {
        final WebTarget base = topic.isV2() ? adminV2NonPersistentTopics : adminNonPersistentTopics;
        WebTarget topicPath = base.path(topic.getRestPath());
        topicPath = WebTargets.addParts(topicPath, parts);
        return topicPath;
    }
}
