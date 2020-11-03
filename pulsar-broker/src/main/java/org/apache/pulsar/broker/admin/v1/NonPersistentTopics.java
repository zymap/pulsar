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
package org.apache.pulsar.broker.admin.v1;

import com.google.common.collect.Lists;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.Encoded;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
<<<<<<< HEAD
=======
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
>>>>>>> f773c602c... Test pr 10 (#27)
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentTopic;
import org.apache.pulsar.broker.web.RestException;
<<<<<<< HEAD
=======
import org.apache.pulsar.common.api.proto.PulsarApi;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.common.naming.Constants;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.NonPersistentTopicStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
@Path("/non-persistent")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/non-persistent", description = "Non-Persistent topic admin apis", tags = "non-persistent topic", hidden = true)
@SuppressWarnings("deprecation")
public class NonPersistentTopics extends PersistentTopics {
    private static final Logger log = LoggerFactory.getLogger(NonPersistentTopics.class);

    @GET
    @Path("/{property}/{cluster}/{namespace}/{topic}/partitions")
    @ApiOperation(hidden = true, value = "Get partitioned topic metadata.")
<<<<<<< HEAD
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission") })
    public PartitionedTopicMetadata getPartitionedMetadata(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(property, cluster, namespace, encodedTopic);
        return getPartitionedTopicMetadata(topicName, authoritative);
=======
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 403, message = "Don't have admin permission") })
    public PartitionedTopicMetadata getPartitionedMetadata(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @QueryParam("checkAllowAutoCreation") @DefaultValue("false") boolean checkAllowAutoCreation) {
        validateTopicName(property, cluster, namespace, encodedTopic);
        return getPartitionedTopicMetadata(topicName, authoritative, checkAllowAutoCreation);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @GET
    @Path("{property}/{cluster}/{namespace}/{topic}/stats")
    @ApiOperation(hidden = true, value = "Get the stats for the topic.")
<<<<<<< HEAD
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
=======
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
>>>>>>> f773c602c... Test pr 10 (#27)
            @ApiResponse(code = 404, message = "Topic does not exist") })
    public NonPersistentTopicStats getStats(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(property, cluster, namespace, encodedTopic);
        validateAdminOperationOnTopic(authoritative);
        Topic topic = getTopicReference(topicName);
<<<<<<< HEAD
        return ((NonPersistentTopic) topic).getStats();
=======
        return ((NonPersistentTopic) topic).getStats(false);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @GET
    @Path("{property}/{cluster}/{namespace}/{topic}/internalStats")
    @ApiOperation(hidden = true, value = "Get the internal stats for the topic.")
<<<<<<< HEAD
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
=======
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
>>>>>>> f773c602c... Test pr 10 (#27)
            @ApiResponse(code = 404, message = "Topic does not exist") })
    public PersistentTopicInternalStats getInternalStats(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
<<<<<<< HEAD
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(property, cluster, namespace, encodedTopic);
        validateAdminOperationOnTopic(authoritative);
        Topic topic = getTopicReference(topicName);
        return topic.getInternalStats();
=======
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @QueryParam("metadata") @DefaultValue("false") boolean metadata) {
        validateTopicName(property, cluster, namespace, encodedTopic);
        validateAdminOperationOnTopic(authoritative);
        Topic topic = getTopicReference(topicName);
        try {
            boolean includeMetadata = metadata && hasSuperUserAccess();
            return topic.getInternalStats(includeMetadata).get();
        } catch (Exception e) {
            throw new RestException(Status.INTERNAL_SERVER_ERROR, (e instanceof ExecutionException) ? e.getCause().getMessage() : e.getMessage());
        }
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @PUT
    @Path("/{property}/{cluster}/{namespace}/{topic}/partitions")
    @ApiOperation(hidden = true, value = "Create a partitioned topic.", notes = "It needs to be called before creating a producer on a partitioned topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
<<<<<<< HEAD
            @ApiResponse(code = 409, message = "Partitioned topic already exist") })
    public void createPartitionedTopic(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            int numPartitions, @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(property, cluster, namespace, encodedTopic);
        validateAdminAccessForTenant(topicName.getTenant());
        if (numPartitions <= 1) {
            throw new RestException(Status.NOT_ACCEPTABLE, "Number of partitions should be more than 1");
        }
        try {
            String path = path(PARTITIONED_TOPIC_PATH_ZNODE, namespaceName.toString(), domain(),
                    topicName.getEncodedLocalName());
            byte[] data = jsonMapper().writeValueAsBytes(new PartitionedTopicMetadata(numPartitions));
            zkCreateOptimistic(path, data);
            // we wait for the data to be synced in all quorums and the observers
            Thread.sleep(PARTITIONED_TOPIC_WAIT_SYNC_TIME_MS);
            log.info("[{}] Successfully created partitioned topic {}", clientAppId(), topicName);
        } catch (KeeperException.NodeExistsException e) {
            log.warn("[{}] Failed to create already existing partitioned topic {}", clientAppId(), topicName);
            throw new RestException(Status.CONFLICT, "Partitioned topic already exist");
        } catch (Exception e) {
            log.error("[{}] Failed to create partitioned topic {}", clientAppId(), topicName, e);
            throw new RestException(e);
=======
            @ApiResponse(code = 406, message = "The number of partitions should be more than 0 and less than or equal to maxNumPartitionsPerPartitionedTopic"),
            @ApiResponse(code = 409, message = "Partitioned topic already exist") })
    public void createPartitionedTopic(@Suspended final AsyncResponse asyncResponse, @PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            int numPartitions) {
        try {
            validateTopicName(property, cluster, namespace, encodedTopic);
            internalCreatePartitionedTopic(asyncResponse, numPartitions);
        } catch (Exception e) {
            log.error("[{}] Failed to create partitioned topic {}", clientAppId(), topicName, e);
            resumeAsyncResponseExceptionally(asyncResponse, e);
>>>>>>> f773c602c... Test pr 10 (#27)
        }
    }

    @PUT
    @Path("/{property}/{cluster}/{namespace}/{topic}/unload")
    @ApiOperation(hidden = true, value = "Unload a topic")
<<<<<<< HEAD
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist") })
    public void unloadTopic(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(property, cluster, namespace, encodedTopic);
        log.info("[{}] Unloading topic {}", clientAppId(), topicName);

        if (topicName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        unloadTopic(topicName, authoritative);
=======
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist") })
    public void unloadTopic(@Suspended final AsyncResponse asyncResponse, @PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateTopicName(property, cluster, namespace, encodedTopic);
            internalUnloadTopic(asyncResponse, authoritative);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}")
    @ApiOperation(value = "Get the list of non-persistent topics under a namespace.", response = String.class, responseContainer = "List")
<<<<<<< HEAD
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist") })
    public List<String> getList(@PathParam("property") String property, @PathParam("cluster") String cluster,
                                @PathParam("namespace") String namespace) {
        log.info("[{}] list of topics on namespace {}/{}/{}/{}", clientAppId(), property, cluster, namespace);
        validateAdminAccessForTenant(property);
        Policies policies = getNamespacePolicies(property, cluster, namespace);
        NamespaceName nsName = NamespaceName.get(property, cluster, namespace);

        if (!cluster.equals(Constants.GLOBAL_CLUSTER)) {
            validateClusterOwnership(cluster);
            validateClusterForTenant(property, cluster);
        } else {
            // check cluster ownership for a given global namespace: redirect if peer-cluster owns it
            validateGlobalNamespaceOwnership(nsName);
        }
=======
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist") })
    public void getList(@Suspended final AsyncResponse asyncResponse, @PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace) {
        log.info("[{}] list of topics on namespace {}/{}/{}", clientAppId(), property, cluster, namespace);

        Policies policies = null;
        NamespaceName nsName = null;
        try {
            validateAdminAccessForTenant(property);
            policies = getNamespacePolicies(property, cluster, namespace);
            nsName = NamespaceName.get(property, cluster, namespace);

            if (!cluster.equals(Constants.GLOBAL_CLUSTER)) {
                validateClusterOwnership(cluster);
                validateClusterForTenant(property, cluster);
            } else {
                // check cluster ownership for a given global namespace: redirect if peer-cluster owns it
                validateGlobalNamespaceOwnership(nsName);
            }
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
            return;
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
            return;
        }

>>>>>>> f773c602c... Test pr 10 (#27)
        final List<CompletableFuture<List<String>>> futures = Lists.newArrayList();
        final List<String> boundaries = policies.bundles.getBoundaries();
        for (int i = 0; i < boundaries.size() - 1; i++) {
            final String bundle = String.format("%s_%s", boundaries.get(i), boundaries.get(i + 1));
            try {
                futures.add(pulsar().getAdminClient().nonPersistentTopics().getListInBundleAsync(nsName.toString(),
                        bundle));
            } catch (PulsarServerException e) {
<<<<<<< HEAD
                log.error(String.format("[%s] Failed to get list of topics under namespace %s/%s/%s/%s", clientAppId(),
                        property, cluster, namespace, bundle), e);
                throw new RestException(e);
            }
        }
        final List<String> topics = Lists.newArrayList();
        try {
            FutureUtil.waitForAll(futures).get();
            futures.forEach(topicListFuture -> {
                try {
                    if (topicListFuture.isDone() && topicListFuture.get() != null) {
                        topics.addAll(topicListFuture.get());
                    }
                } catch (InterruptedException | ExecutionException e) {
                    log.error(String.format("[%s] Failed to get list of topics under namespace %s/%s/%s", clientAppId(),
                            property, cluster, namespace), e);
                }
            });
        } catch (InterruptedException | ExecutionException e) {
            log.error(String.format("[%s] Failed to get list of topics under namespace %s/%s/%s", clientAppId(),
                    property, cluster, namespace), e);
            throw new RestException(e instanceof ExecutionException ? e.getCause() : e);
        }
        return topics;
=======
                log.error("[{}] Failed to get list of topics under namespace {}/{}/{}/{}", clientAppId(), property,
                        cluster, namespace, bundle, e);
                asyncResponse.resume(new RestException(e));
                return;
            }
        }

        final List<String> topics = Lists.newArrayList();
        FutureUtil.waitForAll(futures).handle((result, exception) -> {
            for (int i = 0; i < futures.size(); i++) {
                try {
                    if (futures.get(i).isDone() && futures.get(i).get() != null) {
                        topics.addAll(futures.get(i).get());
                    }
                } catch (InterruptedException | ExecutionException e) {
                    log.error("[{}] Failed to get list of topics under namespace {}/{}/{}", clientAppId(), property,
                            cluster, namespace, e);
                    asyncResponse.resume(new RestException(e instanceof ExecutionException ? e.getCause() : e));
                    return null;
                }
            }
            asyncResponse.resume(topics);
            return null;
        });
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/{bundle}")
    @ApiOperation(value = "Get the list of non-persistent topics under a namespace bundle.", response = String.class, responseContainer = "List")
<<<<<<< HEAD
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
=======
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
>>>>>>> f773c602c... Test pr 10 (#27)
            @ApiResponse(code = 404, message = "Namespace doesn't exist") })
    public List<String> getListFromBundle(@PathParam("property") String property, @PathParam("cluster") String cluster,
                                          @PathParam("namespace") String namespace, @PathParam("bundle") String bundleRange) {
        log.info("[{}] list of topics on namespace bundle {}/{}/{}/{}", clientAppId(), property, cluster, namespace,
                bundleRange);
        validateAdminAccessForTenant(property);
        Policies policies = getNamespacePolicies(property, cluster, namespace);
        if (!cluster.equals(Constants.GLOBAL_CLUSTER)) {
            validateClusterOwnership(cluster);
            validateClusterForTenant(property, cluster);
        } else {
            // check cluster ownership for a given global namespace: redirect if peer-cluster owns it
            validateGlobalNamespaceOwnership(NamespaceName.get(property, cluster, namespace));
        }
        NamespaceName fqnn = NamespaceName.get(property, cluster, namespace);
        if (!isBundleOwnedByAnyBroker(fqnn, policies.bundles, bundleRange)) {
            log.info("[{}] Namespace bundle is not owned by any broker {}/{}/{}/{}", clientAppId(), property, cluster,
                    namespace, bundleRange);
            return null;
        }
        NamespaceBundle nsBundle = validateNamespaceBundleOwnership(fqnn, policies.bundles, bundleRange, true, true);
        try {
            final List<String> topicList = Lists.newArrayList();
            pulsar().getBrokerService().forEachTopic(topic -> {
                TopicName topicName = TopicName.get(topic.getName());
                if (nsBundle.includes(topicName)) {
                    topicList.add(topic.getName());
                }
            });
            return topicList;
        } catch (Exception e) {
            log.error("[{}] Failed to unload namespace bundle {}/{}", clientAppId(), fqnn.toString(), bundleRange, e);
            throw new RestException(e);
        }
    }

    protected void validateAdminOperationOnTopic(TopicName topicName, boolean authoritative) {
        validateAdminAccessForTenant(topicName.getTenant());
        validateTopicOwnership(topicName, authoritative);
    }

    private Topic getTopicReference(TopicName topicName) {
        return pulsar().getBrokerService().getTopicIfExists(topicName.toString()).join()
                .orElseThrow(() -> new RestException(Status.NOT_FOUND, "Topic not found"));
    }
}
