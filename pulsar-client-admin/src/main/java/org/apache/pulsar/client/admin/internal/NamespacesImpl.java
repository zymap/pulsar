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
import java.util.Map;
import java.util.Set;
<<<<<<< HEAD
import java.util.concurrent.TimeUnit;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
=======
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.WebTarget;
>>>>>>> f773c602c... Test pr 10 (#27)
import javax.ws.rs.core.MediaType;

import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.AuthAction;
<<<<<<< HEAD
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SchemaAutoUpdateCompatibilityStrategy;
=======
import org.apache.pulsar.common.policies.data.AutoSubscriptionCreationOverride;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;
import org.apache.pulsar.common.policies.data.BookieAffinityGroupData;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SchemaAutoUpdateCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.common.policies.data.SubscribeRate;
import org.apache.pulsar.common.policies.data.SubscriptionAuthMode;

public class NamespacesImpl extends BaseResource implements Namespaces {

    private final WebTarget adminNamespaces;
    private final WebTarget adminV2Namespaces;

<<<<<<< HEAD
    public NamespacesImpl(WebTarget web, Authentication auth) {
        super(auth);
=======
    public NamespacesImpl(WebTarget web, Authentication auth, long readTimeoutMs) {
        super(auth, readTimeoutMs);
>>>>>>> f773c602c... Test pr 10 (#27)
        adminNamespaces = web.path("/admin/namespaces");
        adminV2Namespaces = web.path("/admin/v2/namespaces");
    }

    @Override
    public List<String> getNamespaces(String tenant) throws PulsarAdminException {
        try {
<<<<<<< HEAD
            WebTarget path = adminV2Namespaces.path(tenant);
            return request(path).get(new GenericType<List<String>>() {
            });
        } catch (Exception e) {
            throw getApiException(e);
=======
            return getNamespacesAsync(tenant).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public List<String> getNamespaces(String tenant, String cluster) throws PulsarAdminException {
        try {
            WebTarget path = adminNamespaces.path(tenant).path(cluster);
            return request(path).get(new GenericType<List<String>>() {
            });
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<List<String>> getNamespacesAsync(String tenant) {
        WebTarget path = adminV2Namespaces.path(tenant);
        final CompletableFuture<List<String>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<List<String>>() {
                    @Override
                    public void completed(List<String> namespaces) {
                        future.complete(namespaces);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public List<String> getNamespaces(String tenant, String cluster) throws PulsarAdminException {
        WebTarget path = adminNamespaces.path(tenant).path(cluster);
        final CompletableFuture<List<String>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<List<String>>() {

                    @Override
                    public void completed(List<String> namespaces) {
                        future.complete(namespaces);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        try {
            return future.get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public List<String> getTopics(String namespace) throws PulsarAdminException {
        try {
<<<<<<< HEAD
            NamespaceName ns = NamespaceName.get(namespace);
            String action = ns.isV2() ? "topics" : "destinations";
            WebTarget path = namespacePath(ns, action);
            return request(path).get(new GenericType<List<String>>() {
            });
        } catch (Exception e) {
            throw getApiException(e);
=======
            return getTopicsAsync(namespace).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public Policies getPolicies(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns);
            return request(path).get(Policies.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<List<String>> getTopicsAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        String action = ns.isV2() ? "topics" : "destinations";
        WebTarget path = namespacePath(ns, action);
        final CompletableFuture<List<String>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<List<String>>() {
                    @Override
                    public void completed(List<String> topics) {
                        future.complete(topics);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public Policies getPolicies(String namespace) throws PulsarAdminException {
        try {
            return getPoliciesAsync(namespace).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void createNamespace(String namespace, Set<String> clusters) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns);

            if (ns.isV2()) {
                // For V2 API we pass full Policy class instance
                Policies policies = new Policies();
                policies.replication_clusters = clusters;
                request(path).put(Entity.entity(policies, MediaType.APPLICATION_JSON), ErrorData.class);
            } else {
                // For V1 API, we pass the BundlesData on creation
                request(path).put(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
                // For V1, we need to do it in 2 steps
                setNamespaceReplicationClusters(namespace, clusters);
            }
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Policies> getPoliciesAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns);
        final CompletableFuture<Policies> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Policies>() {
                    @Override
                    public void completed(Policies policies) {
                        future.complete(policies);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void createNamespace(String namespace, Set<String> clusters) throws PulsarAdminException {
        try {
            createNamespaceAsync(namespace, clusters).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> createNamespaceAsync(String namespace, Set<String> clusters) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns);

        if (ns.isV2()) {
            // For V2 API we pass full Policy class instance
            Policies policies = new Policies();
            policies.replication_clusters = clusters;
            return asyncPutRequest(path, Entity.entity(policies, MediaType.APPLICATION_JSON));
        } else {
            // For V1 API, we pass the BundlesData on creation
            return asyncPutRequest(path, Entity.entity("", MediaType.APPLICATION_JSON)).thenAccept(ignore -> {
                // For V1, we need to do it in 2 steps
                setNamespaceReplicationClustersAsync(namespace, clusters);
            });
>>>>>>> f773c602c... Test pr 10 (#27)
        }
    }

    @Override
    public void createNamespace(String namespace, int numBundles) throws PulsarAdminException {
        createNamespace(namespace, new BundlesData(numBundles));
    }

    @Override
<<<<<<< HEAD
    public void createNamespace(String namespace, Policies policies) throws PulsarAdminException {
        NamespaceName ns = NamespaceName.get(namespace);
        checkArgument(ns.isV2(), "Create namespace with policies is only supported on newer namespaces");

        try {
            WebTarget path = namespacePath(ns);

            // For V2 API we pass full Policy class instance
            request(path).put(Entity.entity(policies, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Void> createNamespaceAsync(String namespace, int numBundles) {
        return createNamespaceAsync(namespace, new BundlesData(numBundles));
    }

    @Override
    public void createNamespace(String namespace, Policies policies) throws PulsarAdminException {
        try {
            createNamespaceAsync(namespace, policies).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> createNamespaceAsync(String namespace, Policies policies) {
        NamespaceName ns = NamespaceName.get(namespace);
        checkArgument(ns.isV2(), "Create namespace with policies is only supported on newer namespaces");
        WebTarget path = namespacePath(ns);
        // For V2 API we pass full Policy class instance
        return asyncPutRequest(path, Entity.entity(policies, MediaType.APPLICATION_JSON));
    }

    @Override
    public void createNamespace(String namespace, BundlesData bundlesData) throws PulsarAdminException {
        try {
            createNamespaceAsync(namespace, bundlesData).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> createNamespaceAsync(String namespace, BundlesData bundlesData) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns);

        if (ns.isV2()) {
            // For V2 API we pass full Policy class instance
            Policies policies = new Policies();
            policies.bundles = bundlesData;
            return asyncPutRequest(path, Entity.entity(policies, MediaType.APPLICATION_JSON));
        } else {
            // For V1 API, we pass the BundlesData on creation
            return asyncPutRequest(path, Entity.entity(bundlesData, MediaType.APPLICATION_JSON));
>>>>>>> f773c602c... Test pr 10 (#27)
        }
    }

    @Override
<<<<<<< HEAD
    public void createNamespace(String namespace, BundlesData bundlesData) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns);

            if (ns.isV2()) {
                // For V2 API we pass full Policy class instance
                Policies policies = new Policies();
                policies.bundles = bundlesData;
                request(path).put(Entity.entity(policies, MediaType.APPLICATION_JSON), ErrorData.class);
            } else {
                // For V1 API, we pass the BundlesData on creation
                request(path).put(Entity.entity(bundlesData, MediaType.APPLICATION_JSON), ErrorData.class);
            }
        } catch (Exception e) {
            throw getApiException(e);
=======
    public void createNamespace(String namespace) throws PulsarAdminException {
        try {
            createNamespaceAsync(namespace).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void createNamespace(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns);
            request(path).put(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Void> createNamespaceAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns);
        return asyncPutRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void deleteNamespace(String namespace) throws PulsarAdminException {
        try {
            deleteNamespaceAsync(namespace).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void deleteNamespace(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns);
            request(path).delete(ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public void deleteNamespace(String namespace, boolean force) throws PulsarAdminException {
        try {
            deleteNamespaceAsync(namespace, force).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void deleteNamespaceBundle(String namespace, String bundleRange) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, bundleRange);
            request(path).delete(ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Void> deleteNamespaceAsync(String namespace) {
        return deleteNamespaceAsync(namespace, false);
    }

    @Override
    public CompletableFuture<Void> deleteNamespaceAsync(String namespace, boolean force) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns);
        path = path.queryParam("force", force);
        return asyncDeleteRequest(path);
    }

    @Override
    public void deleteNamespaceBundle(String namespace, String bundleRange) throws PulsarAdminException {
        try {
            deleteNamespaceBundleAsync(namespace, bundleRange).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public Map<String, Set<AuthAction>> getPermissions(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "permissions");
            return request(path).get(new GenericType<Map<String, Set<AuthAction>>>() {});
        } catch (Exception e) {
            throw getApiException(e);
=======
    public void deleteNamespaceBundle(String namespace, String bundleRange, boolean force) throws PulsarAdminException {
        try {
            deleteNamespaceBundleAsync(namespace, bundleRange, force).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> deleteNamespaceBundleAsync(String namespace, String bundleRange) {
        return deleteNamespaceBundleAsync(namespace, bundleRange, false);
    }

    @Override
    public CompletableFuture<Void> deleteNamespaceBundleAsync(String namespace, String bundleRange, boolean force) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, bundleRange);
        path = path.queryParam("force", force);
        return asyncDeleteRequest(path);
    }

    @Override
    public Map<String, Set<AuthAction>> getPermissions(String namespace) throws PulsarAdminException {
        try {
            return getPermissionsAsync(namespace).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void grantPermissionOnNamespace(String namespace, String role, Set<AuthAction> actions)
            throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "permissions", role);
            request(path).post(Entity.entity(actions, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Map<String, Set<AuthAction>>> getPermissionsAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "permissions");
        final CompletableFuture<Map<String, Set<AuthAction>>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Map<String, Set<AuthAction>>>() {
                    @Override
                    public void completed(Map<String, Set<AuthAction>> permissions) {
                        future.complete(permissions);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void grantPermissionOnNamespace(String namespace, String role, Set<AuthAction> actions)
            throws PulsarAdminException {
        try {
            grantPermissionOnNamespaceAsync(namespace, role, actions)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void revokePermissionsOnNamespace(String namespace, String role) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "permissions", role);
            request(path).delete(ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    
=======
    public CompletableFuture<Void> grantPermissionOnNamespaceAsync(
            String namespace, String role, Set<AuthAction> actions) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "permissions", role);
        return asyncPostRequest(path, Entity.entity(actions, MediaType.APPLICATION_JSON));
    }

    @Override
    public void revokePermissionsOnNamespace(String namespace, String role) throws PulsarAdminException {
        try {
            revokePermissionsOnNamespaceAsync(namespace, role).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> revokePermissionsOnNamespaceAsync(String namespace, String role) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "permissions", role);
        return asyncDeleteRequest(path);
    }


>>>>>>> f773c602c... Test pr 10 (#27)
    @Override
    public void grantPermissionOnSubscription(String namespace, String subscription, Set<String> roles)
            throws PulsarAdminException {
        try {
<<<<<<< HEAD
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "permissions", "subscription", subscription);
            request(path).post(Entity.entity(roles, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
            grantPermissionOnSubscriptionAsync(namespace, subscription, roles)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void revokePermissionOnSubscription(String namespace, String subscription, String role) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "permissions", subscription, role);
            request(path).delete(ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }
    
    @Override
    public List<String> getNamespaceReplicationClusters(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "replication");
            return request(path).get(new GenericType<List<String>>() {});
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Void> grantPermissionOnSubscriptionAsync(
            String namespace, String subscription, Set<String> roles) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "permissions", "subscription", subscription);
        return asyncPostRequest(path, Entity.entity(roles, MediaType.APPLICATION_JSON));
    }

    @Override
    public void revokePermissionOnSubscription(
            String namespace, String subscription, String role) throws PulsarAdminException {
        try {
            revokePermissionOnSubscriptionAsync(namespace, subscription, role)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> revokePermissionOnSubscriptionAsync(
            String namespace, String subscription, String role) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "permissions", subscription, role);
        return asyncDeleteRequest(path);
    }

    @Override
    public List<String> getNamespaceReplicationClusters(String namespace) throws PulsarAdminException {
        try {
            return getNamespaceReplicationClustersAsync(namespace)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void setNamespaceReplicationClusters(String namespace, Set<String> clusterIds) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "replication");
            request(path).post(Entity.entity(clusterIds, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<List<String>> getNamespaceReplicationClustersAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "replication");
        final CompletableFuture<List<String>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<List<String>>() {
                    @Override
                    public void completed(List<String> clusters) {
                        future.complete(clusters);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setNamespaceReplicationClusters(String namespace, Set<String> clusterIds) throws PulsarAdminException {
        try {
            setNamespaceReplicationClustersAsync(namespace, clusterIds).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public int getNamespaceMessageTTL(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "messageTTL");
            return request(path).get(new GenericType<Integer>() {});
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Void> setNamespaceReplicationClustersAsync(String namespace, Set<String> clusterIds) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "replication");
        return asyncPostRequest(path, Entity.entity(clusterIds, MediaType.APPLICATION_JSON));
    }

    @Override
    public int getNamespaceMessageTTL(String namespace) throws PulsarAdminException {
        try {
            return getNamespaceMessageTTLAsync(namespace).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void setNamespaceMessageTTL(String namespace, int ttlInSeconds) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "messageTTL");
            request(path).post(Entity.entity(ttlInSeconds, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Integer> getNamespaceMessageTTLAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "messageTTL");
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Integer>() {
                    @Override
                    public void completed(Integer ttl) {
                        future.complete(ttl);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setNamespaceMessageTTL(String namespace, int ttlInSeconds) throws PulsarAdminException {
        try {
            setNamespaceMessageTTLAsync(namespace, ttlInSeconds)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void setNamespaceAntiAffinityGroup(String namespace, String namespaceAntiAffinityGroup)
            throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "antiAffinity");
            request(path).post(Entity.entity(namespaceAntiAffinityGroup, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public String getNamespaceAntiAffinityGroup(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "antiAffinity");
            return request(path).get(new GenericType<String>() {});
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Void> setNamespaceMessageTTLAsync(String namespace, int ttlInSeconds) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "messageTTL");
        return asyncPostRequest(path, Entity.entity(ttlInSeconds, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeNamespaceMessageTTL(String namespace) throws PulsarAdminException {
        try {
            removeNamespaceMessageTTLAsync(namespace)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public List<String> getAntiAffinityNamespaces(String tenant, String cluster, String namespaceAntiAffinityGroup)
            throws PulsarAdminException {
        try {
            WebTarget path = adminNamespaces.path(cluster).path("antiAffinity").path(namespaceAntiAffinityGroup);
            return request(path.queryParam("property", tenant)).get(new GenericType<List<String>>() {});
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void deleteNamespaceAntiAffinityGroup(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "antiAffinity");
            request(path).delete(ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Void> removeNamespaceMessageTTLAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "messageTTL");
        return asyncDeleteRequest(path);
    }

    @Override
    public int getSubscriptionExpirationTime(String namespace) throws PulsarAdminException {
        try {
            return getSubscriptionExpirationTimeAsync(namespace).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void setDeduplicationStatus(String namespace, boolean enableDeduplication) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "deduplication");
            request(path).post(Entity.entity(enableDeduplication, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public Map<BacklogQuotaType, BacklogQuota> getBacklogQuotaMap(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "backlogQuotaMap");
            return request(path).get(new GenericType<Map<BacklogQuotaType, BacklogQuota>>() {
            });
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Integer> getSubscriptionExpirationTimeAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "subscriptionExpirationTime");
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        asyncGetRequest(path, new InvocationCallback<Integer>() {
            @Override
            public void completed(Integer expirationTime) {
                future.complete(expirationTime);
            }

            @Override
            public void failed(Throwable throwable) {
                future.completeExceptionally(getApiException(throwable.getCause()));
            }
        });
        return future;
    }

    @Override
    public void setSubscriptionExpirationTime(String namespace, int expirationTime)
            throws PulsarAdminException {
        try {
            setSubscriptionExpirationTimeAsync(namespace, expirationTime).get(this.readTimeoutMs,
                    TimeUnit.MILLISECONDS);
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
    public void setBacklogQuota(String namespace, BacklogQuota backlogQuota) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "backlogQuota");
            request(path).post(Entity.entity(backlogQuota, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void removeBacklogQuota(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "backlogQuota");
            request(path.queryParam("backlogQuotaType", BacklogQuotaType.destination_storage.toString()))
                    .delete(ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Void> setSubscriptionExpirationTimeAsync(String namespace, int expirationTime) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "subscriptionExpirationTime");
        return asyncPostRequest(path, Entity.entity(expirationTime, MediaType.APPLICATION_JSON));
    }

    @Override
    public void setNamespaceAntiAffinityGroup(String namespace, String namespaceAntiAffinityGroup)
            throws PulsarAdminException {
        try {
            setNamespaceAntiAffinityGroupAsync(namespace, namespaceAntiAffinityGroup)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void setPersistence(String namespace, PersistencePolicies persistence) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "persistence");
            request(path).post(Entity.entity(persistence, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public PersistencePolicies getPersistence(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "persistence");
            return request(path).get(PersistencePolicies.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Void> setNamespaceAntiAffinityGroupAsync(
            String namespace, String namespaceAntiAffinityGroup) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "antiAffinity");
        return asyncPostRequest(path, Entity.entity(namespaceAntiAffinityGroup, MediaType.APPLICATION_JSON));
    }

    @Override
    public String getNamespaceAntiAffinityGroup(String namespace) throws PulsarAdminException {
        try {
            return getNamespaceAntiAffinityGroupAsync(namespace).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void setRetention(String namespace, RetentionPolicies retention) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "retention");
            request(path).post(Entity.entity(retention, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }

    }

    @Override
    public RetentionPolicies getRetention(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "retention");
            return request(path).get(RetentionPolicies.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<String> getNamespaceAntiAffinityGroupAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "antiAffinity");
        final CompletableFuture<String> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<String>() {
                    @Override
                    public void completed(String s) {
                        future.complete(s);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public List<String> getAntiAffinityNamespaces(String tenant, String cluster, String namespaceAntiAffinityGroup)
            throws PulsarAdminException {
        try {
            return getAntiAffinityNamespacesAsync(tenant, cluster, namespaceAntiAffinityGroup)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void unload(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "unload");
            request(path).put(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public String getReplicationConfigVersion(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "configversion");
            return request(path).get(String.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<List<String>> getAntiAffinityNamespacesAsync(
            String tenant, String cluster, String namespaceAntiAffinityGroup) {
        WebTarget path = adminNamespaces.path(cluster)
                .path("antiAffinity").path(namespaceAntiAffinityGroup).queryParam("property", tenant);
        final CompletableFuture<List<String>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<List<String>>() {
                    @Override
                    public void completed(List<String> antiNamespaces) {
                        future.complete(antiNamespaces);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void deleteNamespaceAntiAffinityGroup(String namespace) throws PulsarAdminException {
        try {
            deleteNamespaceAntiAffinityGroupAsync(namespace).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void unloadNamespaceBundle(String namespace, String bundle) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, bundle, "unload");
            request(path).put(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Void> deleteNamespaceAntiAffinityGroupAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "antiAffinity");
        return asyncDeleteRequest(path);
    }

    @Override
    public void setDeduplicationStatus(String namespace, boolean enableDeduplication) throws PulsarAdminException {
        try {
            setDeduplicationStatusAsync(namespace, enableDeduplication)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> setDeduplicationStatusAsync(String namespace, boolean enableDeduplication) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "deduplication");
        return asyncPostRequest(path, Entity.entity(enableDeduplication, MediaType.APPLICATION_JSON));
    }

    @Override
    public void setAutoTopicCreation(String namespace,
                                     AutoTopicCreationOverride autoTopicCreationOverride) throws PulsarAdminException {
        try {
            setAutoTopicCreationAsync(namespace, autoTopicCreationOverride)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> setAutoTopicCreationAsync(
            String namespace, AutoTopicCreationOverride autoTopicCreationOverride) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "autoTopicCreation");
        return asyncPostRequest(path, Entity.entity(autoTopicCreationOverride, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeAutoTopicCreation(String namespace) throws PulsarAdminException {
        try {
            removeAutoTopicCreationAsync(namespace).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> removeAutoTopicCreationAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "autoTopicCreation");
        return asyncDeleteRequest(path);
    }

    @Override
    public void setAutoSubscriptionCreation(String namespace,
            AutoSubscriptionCreationOverride autoSubscriptionCreationOverride) throws PulsarAdminException {
        try {
            setAutoSubscriptionCreationAsync(namespace, autoSubscriptionCreationOverride)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> setAutoSubscriptionCreationAsync(String namespace,
            AutoSubscriptionCreationOverride autoSubscriptionCreationOverride) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "autoSubscriptionCreation");
        return asyncPostRequest(path, Entity.entity(autoSubscriptionCreationOverride, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeAutoSubscriptionCreation(String namespace) throws PulsarAdminException {
        try {
            removeAutoSubscriptionCreationAsync(namespace).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> removeAutoSubscriptionCreationAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "autoSubscriptionCreation");
        return asyncDeleteRequest(path);
    }

    @Override
    public Map<BacklogQuotaType, BacklogQuota> getBacklogQuotaMap(String namespace) throws PulsarAdminException {
        try {
            return getBacklogQuotaMapAsync(namespace).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void splitNamespaceBundle(String namespace, String bundle, boolean unloadSplitBundles)
            throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, bundle, "split");
            request(path.queryParam("unload", Boolean.toString(unloadSplitBundles)))
                    .put(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Map<BacklogQuotaType, BacklogQuota>> getBacklogQuotaMapAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "backlogQuotaMap");
        final CompletableFuture<Map<BacklogQuotaType, BacklogQuota>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Map<BacklogQuotaType, BacklogQuota>>() {
                    @Override
                    public void completed(Map<BacklogQuotaType, BacklogQuota> quotaMap) {
                        future.complete(quotaMap);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setBacklogQuota(String namespace, BacklogQuota backlogQuota) throws PulsarAdminException {
        try {
            setBacklogQuotaAsync(namespace, backlogQuota).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> setBacklogQuotaAsync(String namespace, BacklogQuota backlogQuota) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "backlogQuota");
        return asyncPostRequest(path, Entity.entity(backlogQuota, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeBacklogQuota(String namespace) throws PulsarAdminException {
        try {
            removeBacklogQuotaAsync(namespace).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void removeInactiveTopicPolicies(String namespace) throws PulsarAdminException {
        try {
            removeInactiveTopicPoliciesAsync(namespace).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> removeInactiveTopicPoliciesAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "inactiveTopicPolicies");
        return asyncDeleteRequest(path);
    }

    @Override
    public CompletableFuture<Void> removeBacklogQuotaAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "backlogQuota")
                .queryParam("backlogQuotaType", BacklogQuotaType.destination_storage.toString());
        return asyncDeleteRequest(path);
    }

    @Override
    public void setPersistence(String namespace, PersistencePolicies persistence) throws PulsarAdminException {
        try {
            setPersistenceAsync(namespace, persistence).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> setPersistenceAsync(String namespace, PersistencePolicies persistence) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "persistence");
        return asyncPostRequest(path, Entity.entity(persistence, MediaType.APPLICATION_JSON));
    }

    @Override
    public void setBookieAffinityGroup(
            String namespace, BookieAffinityGroupData bookieAffinityGroup) throws PulsarAdminException {
        try {
            setBookieAffinityGroupAsync(namespace, bookieAffinityGroup).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> setBookieAffinityGroupAsync(
            String namespace, BookieAffinityGroupData bookieAffinityGroup) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "persistence", "bookieAffinity");
        return asyncPostRequest(path, Entity.entity(bookieAffinityGroup, MediaType.APPLICATION_JSON));
    }

    @Override
    public void deleteBookieAffinityGroup(String namespace) throws PulsarAdminException {
        try {
            deleteBookieAffinityGroupAsync(namespace).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> deleteBookieAffinityGroupAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "persistence", "bookieAffinity");
        return asyncDeleteRequest(path);
    }

    @Override
    public BookieAffinityGroupData getBookieAffinityGroup(String namespace) throws PulsarAdminException {
        try {
            return getBookieAffinityGroupAsync(namespace).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<BookieAffinityGroupData> getBookieAffinityGroupAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "persistence", "bookieAffinity");
        final CompletableFuture<BookieAffinityGroupData> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<BookieAffinityGroupData>() {
                    @Override
                    public void completed(BookieAffinityGroupData bookieAffinityGroupData) {
                        future.complete(bookieAffinityGroupData);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public PersistencePolicies getPersistence(String namespace) throws PulsarAdminException {
        try {
            return getPersistenceAsync(namespace).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<PersistencePolicies> getPersistenceAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "persistence");
        final CompletableFuture<PersistencePolicies> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<PersistencePolicies>() {
                    @Override
                    public void completed(PersistencePolicies persistencePolicies) {
                        future.complete(persistencePolicies);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setRetention(String namespace, RetentionPolicies retention) throws PulsarAdminException {
        try {
            setRetentionAsync(namespace, retention).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> setRetentionAsync(String namespace, RetentionPolicies retention) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "retention");
        return asyncPostRequest(path, Entity.entity(retention, MediaType.APPLICATION_JSON));
    }

    @Override
    public RetentionPolicies getRetention(String namespace) throws PulsarAdminException {
        try {
            return getRetentionAsync(namespace).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<RetentionPolicies> getRetentionAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "retention");
        final CompletableFuture<RetentionPolicies> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<RetentionPolicies>() {
                    @Override
                    public void completed(RetentionPolicies retentionPolicies) {
                        future.complete(retentionPolicies);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void unload(String namespace) throws PulsarAdminException {
        try {
            unloadAsync(namespace).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> unloadAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "unload");
        return asyncPutRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public String getReplicationConfigVersion(String namespace) throws PulsarAdminException {
        try {
            return getReplicationConfigVersionAsync(namespace).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<String> getReplicationConfigVersionAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "configversion");
        final CompletableFuture<String> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<String>() {
                    @Override
                    public void completed(String s) {
                        future.complete(s);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void unloadNamespaceBundle(String namespace, String bundle) throws PulsarAdminException {
        try {
            unloadNamespaceBundleAsync(namespace, bundle).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> unloadNamespaceBundleAsync(String namespace, String bundle) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, bundle, "unload");
        return asyncPutRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void splitNamespaceBundle(
            String namespace, String bundle, boolean unloadSplitBundles, String splitAlgorithmName)
            throws PulsarAdminException {
        try {
            splitNamespaceBundleAsync(namespace, bundle, unloadSplitBundles, splitAlgorithmName)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> splitNamespaceBundleAsync(
            String namespace, String bundle, boolean unloadSplitBundles, String splitAlgorithmName) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, bundle, "split")
                .queryParam("unload", Boolean.toString(unloadSplitBundles))
                .queryParam("splitAlgorithmName", splitAlgorithmName);
        return asyncPutRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void setPublishRate(String namespace, PublishRate publishMsgRate) throws PulsarAdminException {
        try {
            setPublishRateAsync(namespace, publishMsgRate).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void removePublishRate(String namespace) throws PulsarAdminException {
        try {
            removePublishRateAsync(namespace).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> setPublishRateAsync(String namespace, PublishRate publishMsgRate) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "publishRate");
        return asyncPostRequest(path, Entity.entity(publishMsgRate, MediaType.APPLICATION_JSON));
    }

    @Override
    public CompletableFuture<Void> removePublishRateAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "publishRate");
        return asyncDeleteRequest(path);
    }

    @Override
    public PublishRate getPublishRate(String namespace) throws PulsarAdminException {
        try {
            return getPublishRateAsync(namespace).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<PublishRate> getPublishRateAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "publishRate");
        final CompletableFuture<PublishRate> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<PublishRate>() {
                    @Override
                    public void completed(PublishRate publishRate) {
                        future.complete(publishRate);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setDispatchRate(String namespace, DispatchRate dispatchRate) throws PulsarAdminException {
        try {
            setDispatchRateAsync(namespace, dispatchRate).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> setDispatchRateAsync(String namespace, DispatchRate dispatchRate) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "dispatchRate");
        return asyncPostRequest(path, Entity.entity(dispatchRate, MediaType.APPLICATION_JSON));
    }

    @Override
    public DispatchRate getDispatchRate(String namespace) throws PulsarAdminException {
        try {
            return getDispatchRateAsync(namespace).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<DispatchRate> getDispatchRateAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "dispatchRate");
        final CompletableFuture<DispatchRate> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<DispatchRate>() {
                    @Override
                    public void completed(DispatchRate dispatchRate) {
                        future.complete(dispatchRate);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setSubscribeRate(String namespace, SubscribeRate subscribeRate) throws PulsarAdminException {
        try {
            setSubscribeRateAsync(namespace, subscribeRate).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> setSubscribeRateAsync(String namespace, SubscribeRate subscribeRate) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "subscribeRate");
        return asyncPostRequest(path, Entity.entity(subscribeRate, MediaType.APPLICATION_JSON));
    }

    @Override
    public SubscribeRate getSubscribeRate(String namespace) throws PulsarAdminException {
        try {
            return getSubscribeRateAsync(namespace).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<SubscribeRate> getSubscribeRateAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "subscribeRate");
        final CompletableFuture<SubscribeRate> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<SubscribeRate>() {
                    @Override
                    public void completed(SubscribeRate subscribeRate) {
                        future.complete(subscribeRate);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setSubscriptionDispatchRate(String namespace, DispatchRate dispatchRate) throws PulsarAdminException {
        try {
            setSubscriptionDispatchRateAsync(namespace, dispatchRate).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> setSubscriptionDispatchRateAsync(String namespace, DispatchRate dispatchRate) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "subscriptionDispatchRate");
        return asyncPostRequest(path, Entity.entity(dispatchRate, MediaType.APPLICATION_JSON));
    }

    @Override
    public DispatchRate getSubscriptionDispatchRate(String namespace) throws PulsarAdminException {
        try {
            return getSubscriptionDispatchRateAsync(namespace).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<DispatchRate> getSubscriptionDispatchRateAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "subscriptionDispatchRate");
        final CompletableFuture<DispatchRate> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<DispatchRate>() {
                    @Override
                    public void completed(DispatchRate dispatchRate) {
                        future.complete(dispatchRate);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setReplicatorDispatchRate(String namespace, DispatchRate dispatchRate) throws PulsarAdminException {
        try {
            setReplicatorDispatchRateAsync(namespace, dispatchRate).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> setReplicatorDispatchRateAsync(String namespace, DispatchRate dispatchRate) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "replicatorDispatchRate");
        return asyncPostRequest(path, Entity.entity(dispatchRate, MediaType.APPLICATION_JSON));
    }

    @Override
    public DispatchRate getReplicatorDispatchRate(String namespace) throws PulsarAdminException {
        try {
            return getReplicatorDispatchRateAsync(namespace).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<DispatchRate> getReplicatorDispatchRateAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "replicatorDispatchRate");
        final CompletableFuture<DispatchRate> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<DispatchRate>() {
                    @Override
                    public void completed(DispatchRate dispatchRate) {
                        future.complete(dispatchRate);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void clearNamespaceBacklog(String namespace) throws PulsarAdminException {
        try {
            clearNamespaceBacklogAsync(namespace).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> clearNamespaceBacklogAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "clearBacklog");
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void clearNamespaceBacklogForSubscription(String namespace, String subscription)
            throws PulsarAdminException {
        try {
            clearNamespaceBacklogForSubscriptionAsync(namespace, subscription).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> clearNamespaceBacklogForSubscriptionAsync(String namespace, String subscription) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "clearBacklog", subscription);
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void clearNamespaceBundleBacklog(String namespace, String bundle) throws PulsarAdminException {
        try {
            clearNamespaceBundleBacklogAsync(namespace, bundle).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> clearNamespaceBundleBacklogAsync(String namespace, String bundle) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, bundle, "clearBacklog");
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void clearNamespaceBundleBacklogForSubscription(String namespace, String bundle, String subscription)
            throws PulsarAdminException {
        try {
            clearNamespaceBundleBacklogForSubscriptionAsync(namespace, bundle, subscription)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> clearNamespaceBundleBacklogForSubscriptionAsync(String namespace, String bundle,
            String subscription) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, bundle, "clearBacklog", subscription);
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void unsubscribeNamespace(String namespace, String subscription) throws PulsarAdminException {
        try {
            unsubscribeNamespaceAsync(namespace, subscription).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> unsubscribeNamespaceAsync(String namespace, String subscription) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "unsubscribe", subscription);
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void unsubscribeNamespaceBundle(String namespace, String bundle, String subscription)
            throws PulsarAdminException {
        try {
            unsubscribeNamespaceBundleAsync(namespace, bundle, subscription)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> unsubscribeNamespaceBundleAsync(String namespace, String bundle,
            String subscription) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, bundle, "unsubscribe", subscription);
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void setSubscriptionAuthMode(String namespace, SubscriptionAuthMode subscriptionAuthMode)
            throws PulsarAdminException {
        try {
            setSubscriptionAuthModeAsync(namespace, subscriptionAuthMode)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> setSubscriptionAuthModeAsync(
            String namespace, SubscriptionAuthMode subscriptionAuthMode) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "subscriptionAuthMode");
        return asyncPostRequest(path, Entity.entity(subscriptionAuthMode, MediaType.APPLICATION_JSON));
    }

    @Override
    public void setEncryptionRequiredStatus(String namespace, boolean encryptionRequired) throws PulsarAdminException {
        try {
            setEncryptionRequiredStatusAsync(namespace, encryptionRequired)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> setEncryptionRequiredStatusAsync(String namespace, boolean encryptionRequired) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "encryptionRequired");
        return asyncPostRequest(path, Entity.entity(encryptionRequired, MediaType.APPLICATION_JSON));
    }

    @Override
    public DelayedDeliveryPolicies getDelayedDelivery(String namespace) throws PulsarAdminException {
        try {
            return getDelayedDeliveryAsync(namespace).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<DelayedDeliveryPolicies> getDelayedDeliveryAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "delayedDelivery");
        final CompletableFuture<DelayedDeliveryPolicies> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<DelayedDeliveryPolicies>() {
                    @Override
                    public void completed(DelayedDeliveryPolicies delayedDeliveryPolicies) {
                        future.complete(delayedDeliveryPolicies);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setDelayedDeliveryMessages(
            String namespace, DelayedDeliveryPolicies delayedDeliveryPolicies) throws PulsarAdminException {
        try {
            setDelayedDeliveryMessagesAsync(namespace, delayedDeliveryPolicies)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> setDelayedDeliveryMessagesAsync(
            String namespace, DelayedDeliveryPolicies delayedDeliveryPolicies) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "delayedDelivery");
        return asyncPostRequest(path, Entity.entity(delayedDeliveryPolicies, MediaType.APPLICATION_JSON));
    }

    @Override
    public InactiveTopicPolicies getInactiveTopicPolicies(String namespace) throws PulsarAdminException {
        try {
            return getInactiveTopicPoliciesAsync(namespace).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<InactiveTopicPolicies> getInactiveTopicPoliciesAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "inactiveTopicPolicies");
        final CompletableFuture<InactiveTopicPolicies> future = new CompletableFuture<>();
        asyncGetRequest(path, new InvocationCallback<InactiveTopicPolicies>() {
                    @Override
                    public void completed(InactiveTopicPolicies inactiveTopicPolicies) {
                        future.complete(inactiveTopicPolicies);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setInactiveTopicPolicies(
            String namespace, InactiveTopicPolicies inactiveTopicPolicies) throws PulsarAdminException {
        try {
            setInactiveTopicPoliciesAsync(namespace, inactiveTopicPolicies)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> setInactiveTopicPoliciesAsync(
            String namespace, InactiveTopicPolicies inactiveTopicPolicies) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "inactiveTopicPolicies");
        return asyncPostRequest(path, Entity.entity(inactiveTopicPolicies, MediaType.APPLICATION_JSON));
    }

    @Override
    public int getMaxProducersPerTopic(String namespace) throws PulsarAdminException {
        try {
            return getMaxProducersPerTopicAsync(namespace).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void setDispatchRate(String namespace, DispatchRate dispatchRate) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "dispatchRate");
            request(path).post(Entity.entity(dispatchRate, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public DispatchRate getDispatchRate(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "dispatchRate");
            return request(path).get(DispatchRate.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Integer> getMaxProducersPerTopicAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "maxProducersPerTopic");
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Integer>() {
                    @Override
                    public void completed(Integer max) {
                        future.complete(max);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setMaxProducersPerTopic(String namespace, int maxProducersPerTopic) throws PulsarAdminException {
        try {
            setMaxProducersPerTopicAsync(namespace, maxProducersPerTopic).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void setSubscribeRate(String namespace, SubscribeRate subscribeRate) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "subscribeRate");
            request(path).post(Entity.entity(subscribeRate, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public SubscribeRate getSubscribeRate(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "subscribeRate");
            return request(path).get(SubscribeRate.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Void> setMaxProducersPerTopicAsync(String namespace, int maxProducersPerTopic) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "maxProducersPerTopic");
        return asyncPostRequest(path, Entity.entity(maxProducersPerTopic, MediaType.APPLICATION_JSON));
    }

    @Override
    public int getMaxConsumersPerTopic(String namespace) throws PulsarAdminException {
        try {
            return getMaxConsumersPerTopicAsync(namespace).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void setSubscriptionDispatchRate(String namespace, DispatchRate dispatchRate) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "subscriptionDispatchRate");
            request(path).post(Entity.entity(dispatchRate, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public DispatchRate getSubscriptionDispatchRate(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "subscriptionDispatchRate");
            return request(path).get(DispatchRate.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Integer> getMaxConsumersPerTopicAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "maxConsumersPerTopic");
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Integer>() {
                    @Override
                    public void completed(Integer max) {
                        future.complete(max);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setMaxConsumersPerTopic(String namespace, int maxConsumersPerTopic) throws PulsarAdminException {
        try {
            setMaxConsumersPerTopicAsync(namespace, maxConsumersPerTopic)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void clearNamespaceBacklog(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "clearBacklog");
            request(path).post(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Void> setMaxConsumersPerTopicAsync(String namespace, int maxConsumersPerTopic) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "maxConsumersPerTopic");
        return asyncPostRequest(path, Entity.entity(maxConsumersPerTopic, MediaType.APPLICATION_JSON));
    }

    @Override
    public int getMaxConsumersPerSubscription(String namespace) throws PulsarAdminException {
        try {
            return getMaxConsumersPerSubscriptionAsync(namespace).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void clearNamespaceBacklogForSubscription(String namespace, String subscription)
            throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "clearBacklog", subscription);
            request(path).post(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Integer> getMaxConsumersPerSubscriptionAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "maxConsumersPerSubscription");
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Integer>() {
                    @Override
                    public void completed(Integer max) {
                        future.complete(max);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setMaxConsumersPerSubscription(String namespace, int maxConsumersPerSubscription)
            throws PulsarAdminException {
        try {
            setMaxConsumersPerSubscriptionAsync(namespace, maxConsumersPerSubscription)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void clearNamespaceBundleBacklog(String namespace, String bundle) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, bundle, "clearBacklog");
            request(path).post(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Void> setMaxConsumersPerSubscriptionAsync(
            String namespace, int maxConsumersPerSubscription) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "maxConsumersPerSubscription");
        return asyncPostRequest(path, Entity.entity(maxConsumersPerSubscription, MediaType.APPLICATION_JSON));
    }

    @Override
    public int getMaxUnackedMessagesPerConsumer(String namespace) throws PulsarAdminException {
        try {
            return getMaxUnackedMessagesPerConsumerAsync(namespace).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void clearNamespaceBundleBacklogForSubscription(String namespace, String bundle, String subscription)
            throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, bundle, "clearBacklog", subscription);
            request(path).post(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Integer> getMaxUnackedMessagesPerConsumerAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "maxUnackedMessagesPerConsumer");
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Integer>() {
                    @Override
                    public void completed(Integer max) {
                        future.complete(max);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setMaxUnackedMessagesPerConsumer(String namespace, int maxUnackedMessagesPerConsumer)
            throws PulsarAdminException {
        try {
            setMaxUnackedMessagesPerConsumerAsync(namespace, maxUnackedMessagesPerConsumer).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void unsubscribeNamespace(String namespace, String subscription) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "unsubscribe", subscription);
            request(path).post(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Void> setMaxUnackedMessagesPerConsumerAsync(
            String namespace, int maxUnackedMessagesPerConsumer) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "maxUnackedMessagesPerConsumer");
        return asyncPostRequest(path, Entity.entity(maxUnackedMessagesPerConsumer, MediaType.APPLICATION_JSON));
    }

    @Override
    public int getMaxUnackedMessagesPerSubscription(String namespace) throws PulsarAdminException {
        try {
            return getMaxUnackedMessagesPerSubscriptionAsync(namespace).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void unsubscribeNamespaceBundle(String namespace, String bundle, String subscription)
            throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, bundle, "unsubscribe", subscription);
            request(path).post(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Integer> getMaxUnackedMessagesPerSubscriptionAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "maxUnackedMessagesPerSubscription");
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Integer>() {
                    @Override
                    public void completed(Integer max) {
                        future.complete(max);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setMaxUnackedMessagesPerSubscription(String namespace, int maxUnackedMessagesPerSubscription)
            throws PulsarAdminException {
        try {
            setMaxUnackedMessagesPerSubscriptionAsync(namespace, maxUnackedMessagesPerSubscription)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void setSubscriptionAuthMode(String namespace, SubscriptionAuthMode subscriptionAuthMode) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "subscriptionAuthMode");
            request(path).post(Entity.entity(subscriptionAuthMode, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Void> setMaxUnackedMessagesPerSubscriptionAsync(
            String namespace, int maxUnackedMessagesPerSubscription) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "maxUnackedMessagesPerSubscription");
        return asyncPostRequest(path, Entity.entity(maxUnackedMessagesPerSubscription, MediaType.APPLICATION_JSON));
    }

    @Override
    public long getCompactionThreshold(String namespace) throws PulsarAdminException {
        try {
            return getCompactionThresholdAsync(namespace).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void setEncryptionRequiredStatus(String namespace, boolean encryptionRequired) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "encryptionRequired");
            request(path).post(Entity.entity(encryptionRequired, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Long> getCompactionThresholdAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "compactionThreshold");
        final CompletableFuture<Long> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Long>() {
                    @Override
                    public void completed(Long threshold) {
                        future.complete(threshold);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setCompactionThreshold(String namespace, long compactionThreshold) throws PulsarAdminException {
        try {
            setCompactionThresholdAsync(namespace, compactionThreshold)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public int getMaxProducersPerTopic(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "maxProducersPerTopic");
            return request(path).get(Integer.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Void> setCompactionThresholdAsync(String namespace, long compactionThreshold) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "compactionThreshold");
        return asyncPutRequest(path, Entity.entity(compactionThreshold, MediaType.APPLICATION_JSON));
    }

    @Override
    public long getOffloadThreshold(String namespace) throws PulsarAdminException {
        try {
            return getOffloadThresholdAsync(namespace).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void setMaxProducersPerTopic(String namespace, int maxProducersPerTopic) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "maxProducersPerTopic");
            request(path).post(Entity.entity(maxProducersPerTopic, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Long> getOffloadThresholdAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "offloadThreshold");
        final CompletableFuture<Long> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Long>() {
                    @Override
                    public void completed(Long threshold) {
                        future.complete(threshold);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setOffloadThreshold(String namespace, long offloadThreshold) throws PulsarAdminException {
        try {
            setOffloadThresholdAsync(namespace, offloadThreshold).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public int getMaxConsumersPerTopic(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "maxConsumersPerTopic");
            return request(path).get(Integer.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Void> setOffloadThresholdAsync(String namespace, long offloadThreshold) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "offloadThreshold");
        return asyncPutRequest(path, Entity.entity(offloadThreshold, MediaType.APPLICATION_JSON));
    }

    @Override
    public Long getOffloadDeleteLagMs(String namespace) throws PulsarAdminException {
        try {
            return getOffloadDeleteLagMsAsync(namespace).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void setMaxConsumersPerTopic(String namespace, int maxConsumersPerTopic) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "maxConsumersPerTopic");
            request(path).post(Entity.entity(maxConsumersPerTopic, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Long> getOffloadDeleteLagMsAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "offloadDeletionLagMs");
        final CompletableFuture<Long> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Long>() {
                    @Override
                    public void completed(Long lag) {
                        future.complete(lag);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setOffloadDeleteLag(String namespace, long lag, TimeUnit unit) throws PulsarAdminException {
        try {
            setOffloadDeleteLagAsync(namespace, lag, unit).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public int getMaxConsumersPerSubscription(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "maxConsumersPerSubscription");
            return request(path).get(Integer.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Void> setOffloadDeleteLagAsync(String namespace, long lag, TimeUnit unit) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "offloadDeletionLagMs");
        return asyncPutRequest(path, Entity.entity(
                TimeUnit.MILLISECONDS.convert(lag, unit), MediaType.APPLICATION_JSON));
    }

    @Override
    public void clearOffloadDeleteLag(String namespace) throws PulsarAdminException {
        try {
            clearOffloadDeleteLagAsync(namespace).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void setMaxConsumersPerSubscription(String namespace, int maxConsumersPerSubscription) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "maxConsumersPerSubscription");
            request(path).post(Entity.entity(maxConsumersPerSubscription, MediaType.APPLICATION_JSON), ErrorData.class);
=======
    public CompletableFuture<Void> clearOffloadDeleteLagAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "offloadDeletionLagMs");
        return asyncDeleteRequest(path);
    }

    @Override
    public SchemaAutoUpdateCompatibilityStrategy getSchemaAutoUpdateCompatibilityStrategy(String namespace)
            throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "schemaAutoUpdateCompatibilityStrategy");
            return request(path).get(SchemaAutoUpdateCompatibilityStrategy.class);
>>>>>>> f773c602c... Test pr 10 (#27)
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
<<<<<<< HEAD
    public long getCompactionThreshold(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "compactionThreshold");
            return request(path).get(Long.class);
=======
    public void setSchemaAutoUpdateCompatibilityStrategy(String namespace,
                                                         SchemaAutoUpdateCompatibilityStrategy strategy)
            throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "schemaAutoUpdateCompatibilityStrategy");
            request(path).put(Entity.entity(strategy, MediaType.APPLICATION_JSON),
                              ErrorData.class);
>>>>>>> f773c602c... Test pr 10 (#27)
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
<<<<<<< HEAD
    public void setCompactionThreshold(String namespace, long compactionThreshold) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "compactionThreshold");
            request(path).put(Entity.entity(compactionThreshold, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public boolean getSchemaValidationEnforced(String namespace)
            throws PulsarAdminException {
        try {
            return getSchemaValidationEnforcedAsync(namespace).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public long getOffloadThreshold(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "offloadThreshold");
            return request(path).get(Long.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Boolean> getSchemaValidationEnforcedAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "schemaValidationEnforced");
        final CompletableFuture<Boolean> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Boolean>() {
                    @Override
                    public void completed(Boolean enforced) {
                        future.complete(enforced);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setSchemaValidationEnforced(String namespace, boolean schemaValidationEnforced)
            throws PulsarAdminException {
        try {
            setSchemaValidationEnforcedAsync(namespace, schemaValidationEnforced)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void setOffloadThreshold(String namespace, long offloadThreshold) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "offloadThreshold");
            request(path).put(Entity.entity(offloadThreshold, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Void> setSchemaValidationEnforcedAsync(
            String namespace, boolean schemaValidationEnforced) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "schemaValidationEnforced");
        return asyncPostRequest(path, Entity.entity(schemaValidationEnforced, MediaType.APPLICATION_JSON));
    }

    @Override
    public SchemaCompatibilityStrategy getSchemaCompatibilityStrategy(String namespace) throws PulsarAdminException {
        try {
            return getSchemaCompatibilityStrategyAsync(namespace).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public Long getOffloadDeleteLagMs(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "offloadDeletionLagMs");
            return request(path).get(Long.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<SchemaCompatibilityStrategy> getSchemaCompatibilityStrategyAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "schemaCompatibilityStrategy");
        final CompletableFuture<SchemaCompatibilityStrategy> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<SchemaCompatibilityStrategy>() {
                    @Override
                    public void completed(SchemaCompatibilityStrategy schemaCompatibilityStrategy) {
                        future.complete(schemaCompatibilityStrategy);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setSchemaCompatibilityStrategy(String namespace, SchemaCompatibilityStrategy strategy)
            throws PulsarAdminException {
        try {
            setSchemaCompatibilityStrategyAsync(namespace, strategy).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void setOffloadDeleteLag(String namespace, long lag, TimeUnit unit) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "offloadDeletionLagMs");
            request(path).put(Entity.entity(TimeUnit.MILLISECONDS.convert(lag, unit), MediaType.APPLICATION_JSON),
                              ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Void> setSchemaCompatibilityStrategyAsync(
            String namespace, SchemaCompatibilityStrategy strategy) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "schemaCompatibilityStrategy");
        return asyncPutRequest(path, Entity.entity(strategy, MediaType.APPLICATION_JSON));
    }

    @Override
    public boolean getIsAllowAutoUpdateSchema(String namespace) throws PulsarAdminException {
        try {
            return getIsAllowAutoUpdateSchemaAsync(namespace).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void clearOffloadDeleteLag(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "offloadDeletionLagMs");
            request(path).delete(ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Boolean> getIsAllowAutoUpdateSchemaAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "isAllowAutoUpdateSchema");
        final CompletableFuture<Boolean> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Boolean>() {
                    @Override
                    public void completed(Boolean allowAutoUpdate) {
                        future.complete(allowAutoUpdate);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setIsAllowAutoUpdateSchema(String namespace, boolean isAllowAutoUpdateSchema)
            throws PulsarAdminException {
        try {
            setIsAllowAutoUpdateSchemaAsync(namespace, isAllowAutoUpdateSchema).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public SchemaAutoUpdateCompatibilityStrategy getSchemaAutoUpdateCompatibilityStrategy(String namespace)
            throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "schemaAutoUpdateCompatibilityStrategy");
            return request(path).get(SchemaAutoUpdateCompatibilityStrategy.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Void> setIsAllowAutoUpdateSchemaAsync(String namespace, boolean isAllowAutoUpdateSchema) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "isAllowAutoUpdateSchema");
        return asyncPostRequest(path, Entity.entity(isAllowAutoUpdateSchema, MediaType.APPLICATION_JSON));
    }

    @Override
    public void setOffloadPolicies(String namespace, OffloadPolicies offloadPolicies) throws PulsarAdminException {
        try {
            setOffloadPoliciesAsync(namespace, offloadPolicies)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void setSchemaAutoUpdateCompatibilityStrategy(String namespace,
                                                         SchemaAutoUpdateCompatibilityStrategy strategy)
            throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "schemaAutoUpdateCompatibilityStrategy");
            request(path).put(Entity.entity(strategy, MediaType.APPLICATION_JSON),
                              ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

=======
    public CompletableFuture<Void> setOffloadPoliciesAsync(String namespace, OffloadPolicies offloadPolicies) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "offloadPolicies");
        return asyncPostRequest(path, Entity.entity(offloadPolicies, MediaType.APPLICATION_JSON));
    }

    @Override
    public OffloadPolicies getOffloadPolicies(String namespace) throws PulsarAdminException {
        try {
            return getOffloadPoliciesAsync(namespace).
                    get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<OffloadPolicies> getOffloadPoliciesAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "offloadPolicies");
        final CompletableFuture<OffloadPolicies> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<OffloadPolicies>() {
                    @Override
                    public void completed(OffloadPolicies offloadPolicies) {
                        future.complete(offloadPolicies);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

>>>>>>> f773c602c... Test pr 10 (#27)
    private WebTarget namespacePath(NamespaceName namespace, String... parts) {
        final WebTarget base = namespace.isV2() ? adminV2Namespaces : adminNamespaces;
        WebTarget namespacePath = base.path(namespace.toString());
        namespacePath = WebTargets.addParts(namespacePath, parts);
        return namespacePath;
    }
}
