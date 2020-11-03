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

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
<<<<<<< HEAD

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

import org.apache.pulsar.client.admin.Clusters;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.policies.data.BrokerNamespaceIsolationData;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.FailureDomain;
<<<<<<< HEAD
import org.apache.pulsar.common.policies.data.ErrorData;
=======
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.common.policies.data.NamespaceIsolationData;

public class ClustersImpl extends BaseResource implements Clusters {

    private final WebTarget adminClusters;

<<<<<<< HEAD
    public ClustersImpl(WebTarget web, Authentication auth) {
        super(auth);
=======
    public ClustersImpl(WebTarget web, Authentication auth, long readTimeoutMs) {
        super(auth, readTimeoutMs);
>>>>>>> f773c602c... Test pr 10 (#27)
        adminClusters = web.path("/admin/v2/clusters");
    }

    @Override
    public List<String> getClusters() throws PulsarAdminException {
        try {
<<<<<<< HEAD
            return request(adminClusters).get(new GenericType<List<String>>() {
            });
        } catch (Exception e) {
            throw getApiException(e);
=======
            return getClustersAsync().get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public ClusterData getCluster(String cluster) throws PulsarAdminException {
        try {
            return request(adminClusters.path(cluster)).get(ClusterData.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<List<String>> getClustersAsync() {
        final CompletableFuture<List<String>> future = new CompletableFuture<>();
        asyncGetRequest(adminClusters,
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
    public ClusterData getCluster(String cluster) throws PulsarAdminException {
        try {
            return getClusterAsync(cluster).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void createCluster(String cluster, ClusterData clusterData) throws PulsarAdminException {
        try {
            request(adminClusters.path(cluster))
                    .put(Entity.entity(clusterData, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<ClusterData> getClusterAsync(String cluster) {
        WebTarget path = adminClusters.path(cluster);
        final CompletableFuture<ClusterData> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<ClusterData>() {
                    @Override
                    public void completed(ClusterData clusterData) {
                        future.complete(clusterData);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void createCluster(String cluster, ClusterData clusterData) throws PulsarAdminException {
        try {
            createClusterAsync(cluster, clusterData).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void updateCluster(String cluster, ClusterData clusterData) throws PulsarAdminException {
        try {
            request(adminClusters.path(cluster)).post(Entity.entity(clusterData, MediaType.APPLICATION_JSON),
                    ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Void> createClusterAsync(String cluster, ClusterData clusterData) {
        WebTarget path = adminClusters.path(cluster);
        return asyncPutRequest(path, Entity.entity(clusterData, MediaType.APPLICATION_JSON));
    }

    @Override
    public void updateCluster(String cluster, ClusterData clusterData) throws PulsarAdminException {
        try {
            updateClusterAsync(cluster, clusterData).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void updatePeerClusterNames(String cluster, LinkedHashSet<String> peerClusterNames) throws PulsarAdminException {
        try {
            request(adminClusters.path(cluster).path("peers")).post(Entity.entity(peerClusterNames, MediaType.APPLICATION_JSON),
                    ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }

    }

	@Override
    @SuppressWarnings("unchecked")
	public Set<String> getPeerClusterNames(String cluster) throws PulsarAdminException {
		try {
			return request(adminClusters.path(cluster).path("peers")).get(LinkedHashSet.class);
		} catch (Exception e) {
			throw getApiException(e);
		}
	}
=======
    public CompletableFuture<Void> updateClusterAsync(String cluster, ClusterData clusterData) {
        WebTarget path = adminClusters.path(cluster);
        return asyncPostRequest(path, Entity.entity(clusterData, MediaType.APPLICATION_JSON_TYPE));
    }

    @Override
    public void updatePeerClusterNames(
            String cluster, LinkedHashSet<String> peerClusterNames) throws PulsarAdminException {
        try {
            updatePeerClusterNamesAsync(cluster, peerClusterNames).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> updatePeerClusterNamesAsync(String cluster, LinkedHashSet<String> peerClusterNames) {
        WebTarget path = adminClusters.path(cluster).path("peers");
        return asyncPostRequest(path, Entity.entity(peerClusterNames, MediaType.APPLICATION_JSON));
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<String> getPeerClusterNames(String cluster) throws PulsarAdminException {
        try {
            return getPeerClusterNamesAsync(cluster).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Set<String>> getPeerClusterNamesAsync(String cluster) {
        WebTarget path = adminClusters.path(cluster).path("peers");
        final CompletableFuture<Set<String>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Set<String>>() {
                    @Override
                    public void completed(Set<String> clusterNames) {
                        future.complete(clusterNames);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }
>>>>>>> f773c602c... Test pr 10 (#27)

    @Override
    public void deleteCluster(String cluster) throws PulsarAdminException {
        try {
<<<<<<< HEAD
            request(adminClusters.path(cluster)).delete(ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
            deleteClusterAsync(cluster).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public Map<String, NamespaceIsolationData> getNamespaceIsolationPolicies(String cluster) throws PulsarAdminException {
        try {
            return request(adminClusters.path(cluster).path("namespaceIsolationPolicies")).get(
                    new GenericType<Map<String, NamespaceIsolationData>>() {
                    });
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

=======
    public CompletableFuture<Void> deleteClusterAsync(String cluster) {
        WebTarget path = adminClusters.path(cluster);
        return asyncDeleteRequest(path);
    }

    @Override
    public Map<String, NamespaceIsolationData> getNamespaceIsolationPolicies(String cluster)
            throws PulsarAdminException {
        try {
            return getNamespaceIsolationPoliciesAsync(cluster).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Map<String, NamespaceIsolationData>> getNamespaceIsolationPoliciesAsync(String cluster) {
        WebTarget path = adminClusters.path(cluster).path("namespaceIsolationPolicies");
        final CompletableFuture<Map<String, NamespaceIsolationData>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Map<String, NamespaceIsolationData>>() {
                    @Override
                    public void completed(Map<String, NamespaceIsolationData> stringNamespaceIsolationDataMap) {
                        future.complete(stringNamespaceIsolationDataMap);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }
>>>>>>> f773c602c... Test pr 10 (#27)

    @Override
    public List<BrokerNamespaceIsolationData> getBrokersWithNamespaceIsolationPolicy(String cluster)
            throws PulsarAdminException {
        try {
<<<<<<< HEAD
            return request(adminClusters.path(cluster).path("namespaceIsolationPolicies").path("brokers"))
                    .get(new GenericType<List<BrokerNamespaceIsolationData>>() {
                    });
        } catch (Exception e) {
            throw getApiException(e);
=======
            return getBrokersWithNamespaceIsolationPolicyAsync(cluster).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public BrokerNamespaceIsolationData getBrokerWithNamespaceIsolationPolicy(String cluster, String broker)
            throws PulsarAdminException {
        try {
            return request(adminClusters.path(cluster).path("namespaceIsolationPolicies").path("brokers").path(broker))
                    .get(BrokerNamespaceIsolationData.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<List<BrokerNamespaceIsolationData>> getBrokersWithNamespaceIsolationPolicyAsync(
            String cluster) {
        WebTarget path = adminClusters.path(cluster).path("namespaceIsolationPolicies").path("brokers");
        final CompletableFuture<List<BrokerNamespaceIsolationData>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<List<BrokerNamespaceIsolationData>>() {
                    @Override
                    public void completed(List<BrokerNamespaceIsolationData> brokerNamespaceIsolationData) {
                        future.complete(brokerNamespaceIsolationData);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public BrokerNamespaceIsolationData getBrokerWithNamespaceIsolationPolicy(String cluster, String broker)
            throws PulsarAdminException {
        try {
            return getBrokerWithNamespaceIsolationPolicyAsync(cluster, broker)
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
=======
    public CompletableFuture<BrokerNamespaceIsolationData> getBrokerWithNamespaceIsolationPolicyAsync(
            String cluster, String broker) {
        WebTarget path = adminClusters.path(cluster).path("namespaceIsolationPolicies").path("brokers").path(broker);
        final CompletableFuture<BrokerNamespaceIsolationData> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<BrokerNamespaceIsolationData>() {
                    @Override
                    public void completed(BrokerNamespaceIsolationData brokerNamespaceIsolationData) {
                        future.complete(brokerNamespaceIsolationData);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
>>>>>>> f773c602c... Test pr 10 (#27)
    public void createNamespaceIsolationPolicy(String cluster, String policyName,
            NamespaceIsolationData namespaceIsolationData) throws PulsarAdminException {
        setNamespaceIsolationPolicy(cluster, policyName, namespaceIsolationData);
    }

    @Override
<<<<<<< HEAD
=======
    public CompletableFuture<Void> createNamespaceIsolationPolicyAsync(
            String cluster, String policyName, NamespaceIsolationData namespaceIsolationData) {
        return setNamespaceIsolationPolicyAsync(cluster, policyName, namespaceIsolationData);
    }

    @Override
>>>>>>> f773c602c... Test pr 10 (#27)
    public void updateNamespaceIsolationPolicy(String cluster, String policyName,
            NamespaceIsolationData namespaceIsolationData) throws PulsarAdminException {
        setNamespaceIsolationPolicy(cluster, policyName, namespaceIsolationData);
    }

    @Override
<<<<<<< HEAD
    public void deleteNamespaceIsolationPolicy(String cluster, String policyName) throws PulsarAdminException {
        request(adminClusters.path(cluster)
                .path("namespaceIsolationPolicies").path(policyName)).delete(ErrorData.class);
=======
    public CompletableFuture<Void> updateNamespaceIsolationPolicyAsync(
            String cluster, String policyName, NamespaceIsolationData namespaceIsolationData) {
        return setNamespaceIsolationPolicyAsync(cluster, policyName, namespaceIsolationData);
    }

    @Override
    public void deleteNamespaceIsolationPolicy(String cluster, String policyName) throws PulsarAdminException {
        try {
            deleteNamespaceIsolationPolicyAsync(cluster, policyName).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> deleteNamespaceIsolationPolicyAsync(String cluster, String policyName) {
        WebTarget path = adminClusters.path(cluster).path("namespaceIsolationPolicies").path(policyName);
        return asyncDeleteRequest(path);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    private void setNamespaceIsolationPolicy(String cluster, String policyName,
            NamespaceIsolationData namespaceIsolationData) throws PulsarAdminException {
        try {
<<<<<<< HEAD
            request(adminClusters.path(cluster).path("namespaceIsolationPolicies").path(policyName)).post(
                    Entity.entity(namespaceIsolationData, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

=======
            setNamespaceIsolationPolicyAsync(cluster, policyName, namespaceIsolationData)
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

    private CompletableFuture<Void> setNamespaceIsolationPolicyAsync(String cluster, String policyName,
                                             NamespaceIsolationData namespaceIsolationData) {
        WebTarget path = adminClusters.path(cluster).path("namespaceIsolationPolicies").path(policyName);
        return asyncPostRequest(path, Entity.entity(namespaceIsolationData, MediaType.APPLICATION_JSON));
    }

>>>>>>> f773c602c... Test pr 10 (#27)
    @Override
    public NamespaceIsolationData getNamespaceIsolationPolicy(String cluster, String policyName)
            throws PulsarAdminException {
        try {
<<<<<<< HEAD
            return request(adminClusters.path(cluster).path("namespaceIsolationPolicies").path(policyName)).get(
                    NamespaceIsolationData.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
            return getNamespaceIsolationPolicyAsync(cluster, policyName)
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
    public void createFailureDomain(String cluster, String domainName, FailureDomain domain) throws PulsarAdminException {
=======
    public CompletableFuture<NamespaceIsolationData> getNamespaceIsolationPolicyAsync(
            String cluster, String policyName) {
        WebTarget path = adminClusters.path(cluster).path("namespaceIsolationPolicies").path(policyName);
        final CompletableFuture<NamespaceIsolationData> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<NamespaceIsolationData>() {
                    @Override
                    public void completed(NamespaceIsolationData namespaceIsolationData) {
                        future.complete(namespaceIsolationData);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void createFailureDomain(String cluster, String domainName, FailureDomain domain)
            throws PulsarAdminException {
>>>>>>> f773c602c... Test pr 10 (#27)
        setDomain(cluster, domainName, domain);
    }

    @Override
<<<<<<< HEAD
    public void updateFailureDomain(String cluster, String domainName, FailureDomain domain) throws PulsarAdminException {
=======
    public CompletableFuture<Void> createFailureDomainAsync(String cluster, String domainName, FailureDomain domain) {
        return setDomainAsync(cluster, domainName, domain);
    }

    @Override
    public void updateFailureDomain(String cluster, String domainName, FailureDomain domain)
            throws PulsarAdminException {
>>>>>>> f773c602c... Test pr 10 (#27)
        setDomain(cluster, domainName, domain);
    }

    @Override
<<<<<<< HEAD
    public void deleteFailureDomain(String cluster, String domainName) throws PulsarAdminException {
        request(adminClusters.path(cluster).path("failureDomains").path(domainName)).delete(ErrorData.class);
=======
    public CompletableFuture<Void> updateFailureDomainAsync(String cluster, String domainName, FailureDomain domain) {
        return setDomainAsync(cluster, domainName, domain);
    }

    @Override
    public void deleteFailureDomain(String cluster, String domainName) throws PulsarAdminException {
        try {
            deleteFailureDomainAsync(cluster, domainName).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> deleteFailureDomainAsync(String cluster, String domainName) {
        WebTarget path = adminClusters.path(cluster).path("failureDomains").path(domainName);
        return asyncDeleteRequest(path);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Override
    public Map<String, FailureDomain> getFailureDomains(String cluster) throws PulsarAdminException {
        try {
<<<<<<< HEAD
            return request(adminClusters.path(cluster).path("failureDomains"))
                    .get(new GenericType<Map<String, FailureDomain>>() {
                    });
        } catch (Exception e) {
            throw getApiException(e);
=======
            return getFailureDomainsAsync(cluster).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public FailureDomain getFailureDomain(String cluster, String domainName) throws PulsarAdminException {
        try {
            return request(adminClusters.path(cluster).path("failureDomains")
                    .path(domainName)).get(FailureDomain.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    private void setDomain(String cluster, String domainName,
            FailureDomain domain) throws PulsarAdminException {
        try {
            request(adminClusters.path(cluster).path("failureDomains").path(domainName)).post(
                    Entity.entity(domain, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }
=======
    public CompletableFuture<Map<String, FailureDomain>> getFailureDomainsAsync(String cluster) {
        WebTarget path = adminClusters.path(cluster).path("failureDomains");
        final CompletableFuture<Map<String, FailureDomain>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Map<String, FailureDomain>>() {
                    @Override
                    public void completed(Map<String, FailureDomain> failureDomains) {
                        future.complete(failureDomains);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public FailureDomain getFailureDomain(String cluster, String domainName) throws PulsarAdminException {
        try {
            return getFailureDomainAsync(cluster, domainName).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<FailureDomain> getFailureDomainAsync(String cluster, String domainName) {
        WebTarget path = adminClusters.path(cluster).path("failureDomains").path(domainName);
        final CompletableFuture<FailureDomain> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<FailureDomain>() {
                    @Override
                    public void completed(FailureDomain failureDomain) {
                        future.complete(failureDomain);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    private void setDomain(String cluster, String domainName,
            FailureDomain domain) throws PulsarAdminException {
        try {
            setDomainAsync(cluster, domainName, domain).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    private CompletableFuture<Void> setDomainAsync(String cluster, String domainName,
                           FailureDomain domain) {
        WebTarget path = adminClusters.path(cluster).path("failureDomains").path(domainName);
        return asyncPostRequest(path, Entity.entity(domain, MediaType.APPLICATION_JSON));
    }
>>>>>>> f773c602c... Test pr 10 (#27)
}
