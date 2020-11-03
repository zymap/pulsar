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
import lombok.extern.slf4j.Slf4j;
=======
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;

import lombok.extern.slf4j.Slf4j;

>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Worker;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.functions.WorkerInfo;
import org.apache.pulsar.common.policies.data.WorkerFunctionInstanceStats;
<<<<<<< HEAD

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.util.Collection;
import java.util.List;
import java.util.Map;
=======
import org.apache.pulsar.common.stats.Metrics;
>>>>>>> f773c602c... Test pr 10 (#27)

@Slf4j
public class WorkerImpl extends BaseResource implements Worker {

    private final WebTarget workerStats;
    private final WebTarget worker;

<<<<<<< HEAD
    public WorkerImpl(WebTarget web, Authentication auth) {
        super(auth);
=======
    public WorkerImpl(WebTarget web, Authentication auth, long readTimeoutMs) {
        super(auth, readTimeoutMs);
>>>>>>> f773c602c... Test pr 10 (#27)
        this.worker = web.path("/admin/v2/worker");
        this.workerStats = web.path("/admin/v2/worker-stats");
    }

    @Override
    public List<WorkerFunctionInstanceStats> getFunctionsStats() throws PulsarAdminException {
        try {
<<<<<<< HEAD
            Response response = request(workerStats.path("functionsmetrics")).get();
            if (!response.getStatusInfo().equals(Response.Status.OK)) {
                throw new ClientErrorException(response);
            }
            List<WorkerFunctionInstanceStats> metricsList
                    = response.readEntity(new GenericType<List<WorkerFunctionInstanceStats>>() {});
            return metricsList;
        } catch (Exception e) {
            throw getApiException(e);
=======
            return getFunctionsStatsAsync().get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public Collection<org.apache.pulsar.common.stats.Metrics> getMetrics() throws PulsarAdminException {
        try {
            Response response = request(workerStats.path("metrics")).get();
            if (!response.getStatusInfo().equals(Response.Status.OK)) {
                throw new ClientErrorException(response);
            }
            return response.readEntity(new GenericType<List<org.apache.pulsar.common.stats.Metrics>>() {});
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<List<WorkerFunctionInstanceStats>> getFunctionsStatsAsync() {
        WebTarget path = workerStats.path("functionsmetrics");
        final CompletableFuture<List<WorkerFunctionInstanceStats>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Response>() {
                    @Override
                    public void completed(Response response) {
                        if (!response.getStatusInfo().equals(Response.Status.OK)) {
                            future.completeExceptionally(new ClientErrorException(response));
                        } else {
                            List<WorkerFunctionInstanceStats> metricsList =
                                    response.readEntity(new GenericType<List<WorkerFunctionInstanceStats>>() {});
                            future.complete(metricsList);
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
    public Collection<Metrics> getMetrics() throws PulsarAdminException {
        try {
            return getMetricsAsync().get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public List<WorkerInfo> getCluster() throws PulsarAdminException {
        try {
            Response response = request(worker.path("cluster")).get();
            if (!response.getStatusInfo().equals(Response.Status.OK)) {
                throw new ClientErrorException(response);
            }
            return response.readEntity(new GenericType<List<WorkerInfo>>() {});
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<Collection<Metrics>> getMetricsAsync() {
        WebTarget path = workerStats.path("metrics");
        final CompletableFuture<Collection<Metrics>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Response>() {
                    @Override
                    public void completed(Response response) {
                        if (!response.getStatusInfo().equals(Response.Status.OK)) {
                            future.completeExceptionally(new ClientErrorException(response));
                        } else {
                            future.complete(response.readEntity(
                                    new GenericType<List<Metrics>>() {}));
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
    public List<WorkerInfo> getCluster() throws PulsarAdminException {
        try {
            return getClusterAsync().get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public WorkerInfo getClusterLeader() throws PulsarAdminException {
        try {
            Response response = request(worker.path("cluster").path("leader")).get();
            if (!response.getStatusInfo().equals(Response.Status.OK)) {
                throw new ClientErrorException(response);
            }
            return response.readEntity(new GenericType<WorkerInfo>(){});
        } catch (Exception e) {
            throw getApiException(e);
=======
    public CompletableFuture<List<WorkerInfo>> getClusterAsync() {
        WebTarget path = worker.path("cluster");
        final CompletableFuture<List<WorkerInfo>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Response>() {

                    @Override
                    public void completed(Response response) {
                        if (!response.getStatusInfo().equals(Response.Status.OK)) {
                            future.completeExceptionally(new ClientErrorException(response));
                        } else {
                            future.complete(response.readEntity(new GenericType<List<WorkerInfo>>() {}));
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
    public WorkerInfo getClusterLeader() throws PulsarAdminException {
        try {
            return getClusterLeaderAsync().get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public Map<String, Collection<String>> getAssignments() throws PulsarAdminException {
        try {
            Response response = request(worker.path("assignments")).get();
            if (!response.getStatusInfo().equals(Response.Status.OK)) {
                throw new ClientErrorException(response);
            }
            Map<String, Collection<String>> assignments
                    = response.readEntity(new GenericType<Map<String, Collection<String>>>() {});
            return assignments;
        } catch (Exception e) {
            throw getApiException(e);
        }
    }
}
=======
    public CompletableFuture<WorkerInfo> getClusterLeaderAsync() {
        WebTarget path = worker.path("cluster").path("leader");
        final CompletableFuture<WorkerInfo> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Response>() {
                    @Override
                    public void completed(Response response) {
                        if (!response.getStatusInfo().equals(Response.Status.OK)) {
                            future.completeExceptionally(new ClientErrorException(response));
                        } else {
                            future.complete(response.readEntity(new GenericType<WorkerInfo>(){}));
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
    public Map<String, Collection<String>> getAssignments() throws PulsarAdminException {
        try {
            return getAssignmentsAsync().get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<Map<String, Collection<String>>> getAssignmentsAsync() {
        WebTarget path = worker.path("assignments");
        final CompletableFuture<Map<String, Collection<String>>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Response>() {
                    @Override
                    public void completed(Response response) {
                        if (!response.getStatusInfo().equals(Response.Status.OK)) {
                            future.completeExceptionally(new ClientErrorException(response));
                        } else {
                            future.complete(response.readEntity(
                                    new GenericType<Map<String, Collection<String>>>() {}));
                        }
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }
}
>>>>>>> f773c602c... Test pr 10 (#27)
