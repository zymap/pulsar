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

import java.util.List;
import java.util.Map;
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
import javax.ws.rs.core.MediaType;
>>>>>>> f773c602c... Test pr 10 (#27)

import org.apache.pulsar.client.admin.Brokers;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.conf.InternalConfigurationData;
<<<<<<< HEAD
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.policies.data.NamespaceOwnershipStatus;
=======
import org.apache.pulsar.common.policies.data.NamespaceOwnershipStatus;
import org.apache.pulsar.common.util.Codec;
>>>>>>> f773c602c... Test pr 10 (#27)

public class BrokersImpl extends BaseResource implements Brokers {
    private final WebTarget adminBrokers;

<<<<<<< HEAD
    public BrokersImpl(WebTarget web, Authentication auth) {
        super(auth);
        adminBrokers = web.path("/admin/v2/brokers");
=======
    public BrokersImpl(WebTarget web, Authentication auth, long readTimeoutMs) {
        super(auth, readTimeoutMs);
        adminBrokers = web.path("admin/v2/brokers");
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Override
    public List<String> getActiveBrokers(String cluster) throws PulsarAdminException {
        try {
<<<<<<< HEAD
            return request(adminBrokers.path(cluster)).get(new GenericType<List<String>>() {
            });
        } catch (Exception e) {
            throw getApiException(e);
=======
            return getActiveBrokersAsync(cluster).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public Map<String, NamespaceOwnershipStatus> getOwnedNamespaces(String cluster, String brokerUrl)
            throws PulsarAdminException {
        try {
            return request(adminBrokers.path(cluster).path(brokerUrl).path("ownedNamespaces")).get(
                    new GenericType<Map<String, NamespaceOwnershipStatus>>() {
                    });
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<List<String>> getActiveBrokersAsync(String cluster) {
        WebTarget path = adminBrokers.path(cluster);
        final CompletableFuture<List<String>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<List<String>>() {
                    @Override
                    public void completed(List<String> brokers) {
                        future.complete(brokers);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public Map<String, NamespaceOwnershipStatus> getOwnedNamespaces(String cluster, String brokerUrl)
            throws PulsarAdminException {
        try {
            return getOwnedNamespacesAsync(cluster, brokerUrl).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void updateDynamicConfiguration(String configName, String configValue) throws PulsarAdminException {
        try {
            request(adminBrokers.path("/configuration/").path(configName).path(configValue)).post(Entity.json(""),
                    ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Map<String, NamespaceOwnershipStatus>> getOwnedNamespacesAsync(
            String cluster, String brokerUrl) {
        WebTarget path = adminBrokers.path(cluster).path(brokerUrl).path("ownedNamespaces");
        final CompletableFuture<Map<String, NamespaceOwnershipStatus>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Map<String, NamespaceOwnershipStatus>>() {
                    @Override
                    public void completed(Map<String, NamespaceOwnershipStatus> ownedNamespaces) {
                        future.complete(ownedNamespaces);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void updateDynamicConfiguration(String configName, String configValue) throws PulsarAdminException {
        try {
            updateDynamicConfigurationAsync(configName, configValue).
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
    public CompletableFuture<Void> updateDynamicConfigurationAsync(String configName, String configValue) {
        String value = Codec.encode(configValue);
        WebTarget path = adminBrokers.path("configuration").path(configName).path(value);
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void deleteDynamicConfiguration(String configName) throws PulsarAdminException {
        try {
            deleteDynamicConfigurationAsync(configName).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public Map<String, String> getAllDynamicConfigurations() throws PulsarAdminException {
        try {
            return request(adminBrokers.path("/configuration/").path("values")).get(new GenericType<Map<String, String>>() {
            });
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Void> deleteDynamicConfigurationAsync(String configName) {
        WebTarget path = adminBrokers.path("configuration").path(configName);
        return asyncDeleteRequest(path);
    }

    @Override
    public Map<String, String> getAllDynamicConfigurations() throws PulsarAdminException {
        try {
            return getAllDynamicConfigurationsAsync().get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public List<String> getDynamicConfigurationNames() throws PulsarAdminException {
        try {
            return request(adminBrokers.path("/configuration")).get(new GenericType<List<String>>() {
            });
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Map<String, String>> getAllDynamicConfigurationsAsync() {
        WebTarget path = adminBrokers.path("configuration").path("values");
        final CompletableFuture<Map<String, String>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Map<String, String>>() {
                    @Override
                    public void completed(Map<String, String> allConfs) {
                        future.complete(allConfs);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public List<String> getDynamicConfigurationNames() throws PulsarAdminException {
        try {
            return getDynamicConfigurationNamesAsync().get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<List<String>> getDynamicConfigurationNamesAsync() {
        WebTarget path = adminBrokers.path("configuration");
        final CompletableFuture<List<String>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<List<String>>() {
                    @Override
                    public void completed(List<String> confNames) {
                        future.complete(confNames);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public Map<String, String> getRuntimeConfigurations() throws PulsarAdminException {
        try {
            return getRuntimeConfigurationsAsync().get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public InternalConfigurationData getInternalConfigurationData() throws PulsarAdminException {
        try {
            return request(adminBrokers.path("/internal-configuration")).get(InternalConfigurationData.class);
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Map<String, String>> getRuntimeConfigurationsAsync() {
        WebTarget path = adminBrokers.path("configuration").path("runtime");
        final CompletableFuture<Map<String, String>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Map<String, String>>() {
                    @Override
                    public void completed(Map<String, String> runtimeConfs) {
                        future.complete(runtimeConfs);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public InternalConfigurationData getInternalConfigurationData() throws PulsarAdminException {
        try {
            return getInternalConfigurationDataAsync().get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public void healthcheck() throws PulsarAdminException {
        try {
            String result = request(adminBrokers.path("/health")).get(String.class);
            if (!result.trim().toLowerCase().equals("ok")) {
                throw new PulsarAdminException("Healthcheck returned unexpected result: " + result);
            }
        } catch (Exception e) {
            throw getApiException(e);
        }
    }
=======
    public CompletableFuture<InternalConfigurationData> getInternalConfigurationDataAsync() {
        WebTarget path = adminBrokers.path("internal-configuration");
        final CompletableFuture<InternalConfigurationData> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<InternalConfigurationData>() {
                    @Override
                    public void completed(InternalConfigurationData internalConfigurationData) {
                        future.complete(internalConfigurationData);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void backlogQuotaCheck() throws PulsarAdminException {
        try {
            backlogQuotaCheckAsync().get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> backlogQuotaCheckAsync() {
        WebTarget path = adminBrokers.path("backlogQuotaCheck");
        final CompletableFuture<Void> future = new CompletableFuture<>();
        asyncGetRequest(path, new InvocationCallback<Void>() {
            @Override
            public void completed(Void unused) {
                future.complete(null);
            }

            @Override
            public void failed(Throwable throwable) {
                future.completeExceptionally(throwable);
            }
        });
        return future;
    }

    @Override
    public void healthcheck() throws PulsarAdminException {
        try {
            healthcheckAsync().get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Void> healthcheckAsync() {
        WebTarget path = adminBrokers.path("health");
        final CompletableFuture<Void> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<String>() {
                    @Override
                    public void completed(String result) {
                        if (!"ok".equalsIgnoreCase(result.trim())) {
                            future.completeExceptionally(
                                    new PulsarAdminException("Healthcheck returned unexpected result: " + result));
                        } else {
                            future.complete(null);
                        }
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
