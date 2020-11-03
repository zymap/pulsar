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
package org.apache.pulsar.broker.admin.v2;

import com.google.common.collect.Lists;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
<<<<<<< HEAD
=======
import io.swagger.annotations.ApiParam;
>>>>>>> f773c602c... Test pr 10 (#27)
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
<<<<<<< HEAD
=======
import java.util.stream.Collectors;
>>>>>>> f773c602c... Test pr 10 (#27)

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
<<<<<<< HEAD
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentTopic;
import org.apache.pulsar.broker.web.RestException;
=======
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentTopic;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.api.proto.PulsarApi;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.common.naming.NamespaceBundle;
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
@Api(value = "/non-persistent", description = "Non-Persistent topic admin apis", tags = "non-persistent topic")
public class NonPersistentTopics extends PersistentTopics {
    private static final Logger log = LoggerFactory.getLogger(NonPersistentTopics.class);

    @GET
    @Path("/{tenant}/{namespace}/{topic}/partitions")
    @ApiOperation(value = "Get partitioned topic metadata.")
<<<<<<< HEAD
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission") })
    public PartitionedTopicMetadata getPartitionedMetadata(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        return getPartitionedTopicMetadata(topicName, authoritative);
=======
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to manage resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "The tenant/namespace/topic does not exist"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate cluster configuration")
    })
    public PartitionedTopicMetadata getPartitionedMetadata(
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Is authentication required to perform this operation")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(value = "Is check configuration required to automatically create topic")
            @QueryParam("checkAllowAutoCreation") @DefaultValue("false") boolean checkAllowAutoCreation) {
        validateTopicName(tenant, namespace, encodedTopic);
        return getPartitionedTopicMetadata(topicName, authoritative, checkAllowAutoCreation);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @GET
    @Path("{tenant}/{namespace}/{topic}/stats")
    @ApiOperation(value = "Get the stats for the topic.")
<<<<<<< HEAD
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist") })
    public NonPersistentTopicStats getStats(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateAdminOperationOnTopic(topicName, authoritative);
        Topic topic = getTopicReference(topicName);
        return ((NonPersistentTopic) topic).getStats();
=======
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to manage resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "The tenant/namespace/topic does not exist"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
    })
    public NonPersistentTopicStats getStats(
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Is authentication required to perform this operation")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(value = "Is return precise backlog or imprecise backlog")
            @QueryParam("getPreciseBacklog") @DefaultValue("false") boolean getPreciseBacklog) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateAdminOperationOnTopic(topicName, authoritative);
        Topic topic = getTopicReference(topicName);
        return ((NonPersistentTopic) topic).getStats(getPreciseBacklog);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @GET
    @Path("{tenant}/{namespace}/{topic}/internalStats")
    @ApiOperation(value = "Get the internal stats for the topic.")
<<<<<<< HEAD
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist") })
    public PersistentTopicInternalStats getInternalStats(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateAdminOperationOnTopic(topicName, authoritative);
        Topic topic = getTopicReference(topicName);
        return topic.getInternalStats();
=======
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to manage resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "The tenant/namespace/topic does not exist"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
    })
    public PersistentTopicInternalStats getInternalStats(
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Is authentication required to perform this operation")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @QueryParam("metadata") @DefaultValue("false") boolean metadata) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateAdminOperationOnTopic(topicName, authoritative);
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
    @Path("/{tenant}/{namespace}/{topic}/partitions")
    @ApiOperation(value = "Create a partitioned topic.", notes = "It needs to be called before creating a producer on a partitioned topic.")
<<<<<<< HEAD
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 409, message = "Partitioned topic already exists"),
            @ApiResponse(code = 412, message = "Failed Reason : Name is invalid or Namespace does not have any clusters configured"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration")})
    public void createPartitionedTopic(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic, int numPartitions,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateGlobalNamespaceOwnership(tenant,namespace);
        validateTopicName(tenant, namespace, encodedTopic);
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
            throw new RestException(Status.CONFLICT, "Partitioned topic already exists");
        } catch (Exception e) {
            log.error("[{}] Failed to create partitioned topic {}", clientAppId(), topicName, e);
            throw new RestException(e);
=======
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to manage resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "The tenant/namespace does not exist"),
            @ApiResponse(code = 406, message = "The number of partitions should be more than 0 and less than or equal to maxNumPartitionsPerPartitionedTopic"),
            @ApiResponse(code = 409, message = "Partitioned topic already exists"),
            @ApiResponse(code = 412, message = "Failed Reason : Name is invalid or Namespace does not have any clusters configured"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration"),
    })
    public void createPartitionedTopic(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "The number of partitions for the topic", required = true, type = "int", defaultValue = "0")
            int numPartitions) {

        try {
            validateGlobalNamespaceOwnership(tenant,namespace);
            validateTopicName(tenant, namespace, encodedTopic);
            validateAdminAccessForTenant(topicName.getTenant());
            internalCreatePartitionedTopic(asyncResponse, numPartitions);
        } catch (Exception e) {
            log.error("[{}] Failed to create partitioned topic {}", clientAppId(), topicName, e);
            resumeAsyncResponseExceptionally(asyncResponse, e);
>>>>>>> f773c602c... Test pr 10 (#27)
        }
    }

    @PUT
    @Path("/{tenant}/{namespace}/{topic}/unload")
    @ApiOperation(value = "Unload a topic")
<<<<<<< HEAD
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist") })
    public void unloadTopic(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        log.info("[{}] Unloading topic {}", clientAppId(), topicName);
        if (topicName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        unloadTopic(topicName, authoritative);
=======
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "This operation requires super-user access"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "The tenant/namespace/topic does not exist"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration"),
    })
    public void unloadTopic(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Is authentication required to perform this operation")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateTopicName(tenant, namespace, encodedTopic);
            internalUnloadTopic(asyncResponse, authoritative);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @GET
    @Path("/{tenant}/{namespace}")
    @ApiOperation(value = "Get the list of non-persistent topics under a namespace.", response = String.class, responseContainer = "List")
<<<<<<< HEAD
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist") })
    public List<String> getList(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        if (log.isDebugEnabled()) {
            log.debug("[{}] list of topics on namespace {}", clientAppId(), namespaceName);
        }
        validateAdminAccessForTenant(tenant);
        Policies policies = getNamespacePolicies(namespaceName);


        // check cluster ownership for a given global namespace: redirect if peer-cluster owns it
        validateGlobalNamespaceOwnership(namespaceName);
=======
    @ApiResponses(value = {
            @ApiResponse(code = 401, message = "Don't have permission to manage resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "The tenant/namespace does not exist"),
            @ApiResponse(code = 412, message = "Namespace name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration"),
    })
    public void getList(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace) {
        Policies policies = null;
        try {
            validateNamespaceName(tenant, namespace);
            if (log.isDebugEnabled()) {
                log.debug("[{}] list of topics on namespace {}", clientAppId(), namespaceName);
            }
            validateAdminAccessForTenant(tenant);
            policies = getNamespacePolicies(namespaceName);

            // check cluster ownership for a given global namespace: redirect if peer-cluster owns it
            validateGlobalNamespaceOwnership(namespaceName);
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
                futures.add(pulsar().getAdminClient().topics().getListInBundleAsync(namespaceName.toString(), bundle));
            } catch (PulsarServerException e) {
<<<<<<< HEAD
                log.error(String.format("[%s] Failed to get list of topics under namespace %s/%s", clientAppId(),
                        namespaceName, bundle), e);
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
                    log.error(String.format("[%s] Failed to get list of topics under namespace %s", clientAppId(),
                            namespaceName), e);
                }
            });
        } catch (InterruptedException | ExecutionException e) {
            log.error(
                    String.format("[%s] Failed to get list of topics under namespace %s", clientAppId(), namespaceName),
                    e);
            throw new RestException(e instanceof ExecutionException ? e.getCause() : e);
        }
        return topics;
=======
                log.error("[{}] Failed to get list of topics under namespace {}/{}", clientAppId(), namespaceName,
                        bundle, e);
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
                    log.error("[{}] Failed to get list of topics under namespace {}", clientAppId(), namespaceName, e);
                    asyncResponse.resume(new RestException(e instanceof ExecutionException ? e.getCause() : e));
                    return null;
                }
            }

            final List<String> nonPersistentTopics =
                topics.stream()
                      .filter(name -> !TopicName.get(name).isPersistent())
                      .collect(Collectors.toList());
            asyncResponse.resume(nonPersistentTopics);
            return null;
        });
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @GET
    @Path("/{tenant}/{namespace}/{bundle}")
    @ApiOperation(value = "Get the list of non-persistent topics under a namespace bundle.", response = String.class, responseContainer = "List")
<<<<<<< HEAD
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist") })
    public List<String> getListFromBundle(@PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
=======
    @ApiResponses(value = {
            @ApiResponse(code = 401, message = "Don't have permission to manage resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Namespace name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration"),
    })
    public List<String> getListFromBundle(
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Bundle range of a topic", required = true)
>>>>>>> f773c602c... Test pr 10 (#27)
            @PathParam("bundle") String bundleRange) {
        validateNamespaceName(tenant, namespace);
        if (log.isDebugEnabled()) {
            log.debug("[{}] list of topics on namespace bundle {}/{}", clientAppId(), namespaceName, bundleRange);
        }

        validateAdminAccessForTenant(tenant);
        Policies policies = getNamespacePolicies(namespaceName);

        // check cluster ownership for a given global namespace: redirect if peer-cluster owns it
        validateGlobalNamespaceOwnership(namespaceName);

        if (!isBundleOwnedByAnyBroker(namespaceName, policies.bundles, bundleRange)) {
            log.info("[{}] Namespace bundle is not owned by any broker {}/{}", clientAppId(), namespaceName,
                    bundleRange);
            return null;
        }

        NamespaceBundle nsBundle = validateNamespaceBundleOwnership(namespaceName, policies.bundles, bundleRange, true, true);
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
            log.error("[{}] Failed to unload namespace bundle {}/{}", clientAppId(), namespaceName, bundleRange, e);
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
