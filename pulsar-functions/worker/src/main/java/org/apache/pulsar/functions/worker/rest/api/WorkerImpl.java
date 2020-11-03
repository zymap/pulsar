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
package org.apache.pulsar.functions.worker.rest.api;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.functions.WorkerInfo;
<<<<<<< HEAD
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.policies.data.FunctionStats;
import org.apache.pulsar.common.policies.data.WorkerFunctionInstanceStats;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.worker.FunctionRuntimeInfo;
import org.apache.pulsar.functions.worker.FunctionRuntimeManager;
import org.apache.pulsar.functions.worker.MembershipManager;
import org.apache.pulsar.functions.worker.Utils;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.rest.RestException;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.IOException;
=======
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.policies.data.FunctionStats;
import org.apache.pulsar.common.policies.data.WorkerFunctionInstanceStats;
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.worker.FunctionRuntimeInfo;
import org.apache.pulsar.functions.worker.FunctionRuntimeManager;
import org.apache.pulsar.functions.worker.MembershipManager;
import org.apache.pulsar.functions.worker.SchedulerManager;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.WorkerUtils;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.net.URI;
>>>>>>> f773c602c... Test pr 10 (#27)
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
<<<<<<< HEAD
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;
=======
import java.util.concurrent.Future;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.pulsar.functions.worker.rest.RestUtils.throwUnavailableException;
>>>>>>> f773c602c... Test pr 10 (#27)

@Slf4j
public class WorkerImpl {

    private final Supplier<WorkerService> workerServiceSupplier;
<<<<<<< HEAD
=======
    private Future<?> currentRebalanceFuture;

>>>>>>> f773c602c... Test pr 10 (#27)

    public WorkerImpl(Supplier<WorkerService> workerServiceSupplier) {
        this.workerServiceSupplier = workerServiceSupplier;
    }

    private WorkerService worker() {
        try {
            return checkNotNull(workerServiceSupplier.get());
        } catch (Throwable t) {
            log.info("Failed to get worker service", t);
            throw t;
        }
    }

    private boolean isWorkerServiceAvailable() {
        WorkerService workerService = workerServiceSupplier.get();
        if (workerService == null) {
            return false;
        }
        if (!workerService.isInitialized()) {
            return false;
        }
        return true;
    }

<<<<<<< HEAD
    public List<WorkerInfo> getCluster() {
        if (!isWorkerServiceAvailable()) {
            throw new RestException(Status.SERVICE_UNAVAILABLE, "Function worker service is not done initializing. Please try again in a little while.");
        }
=======
    public List<WorkerInfo> getCluster(String clientRole) {
        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        if (worker().getWorkerConfig().isAuthorizationEnabled() && !isSuperUser(clientRole)) {
            throw new RestException(Status.UNAUTHORIZED, "client is not authorize to perform operation");
        }

>>>>>>> f773c602c... Test pr 10 (#27)
        List<WorkerInfo> workers = worker().getMembershipManager().getCurrentMembership();
        return workers;
    }

<<<<<<< HEAD
    public WorkerInfo getClusterLeader() {
        if (!isWorkerServiceAvailable()) {
            throw new RestException(Status.SERVICE_UNAVAILABLE, "Function worker service is not done initializing. Please try again in a little while.");
=======
    public WorkerInfo getClusterLeader(String clientRole) {
        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        if (worker().getWorkerConfig().isAuthorizationEnabled() && !isSuperUser(clientRole)) {
            log.error("Client [{}] is not authorized to get cluster leader", clientRole);
            throw new RestException(Status.UNAUTHORIZED, "client is not authorize to perform operation");
>>>>>>> f773c602c... Test pr 10 (#27)
        }

        MembershipManager membershipManager = worker().getMembershipManager();
        WorkerInfo leader = membershipManager.getLeader();

        if (leader == null) {
            throw new RestException(Status.INTERNAL_SERVER_ERROR, "Leader cannot be determined");
        }

        return leader;
    }

<<<<<<< HEAD
    public Map<String, Collection<String>> getAssignments() {

        if (!isWorkerServiceAvailable()) {
            throw new RestException(Status.SERVICE_UNAVAILABLE, "Function worker service is not done initializing. Please try again in a little while.");
=======
    public Map<String, Collection<String>> getAssignments(String clientRole) {
        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        if (worker().getWorkerConfig().isAuthorizationEnabled() && !isSuperUser(clientRole)) {
            log.error("Client [{}] is not authorized to get cluster assignments", clientRole);
            throw new RestException(Status.UNAUTHORIZED, "client is not authorize to perform operation");
>>>>>>> f773c602c... Test pr 10 (#27)
        }

        FunctionRuntimeManager functionRuntimeManager = worker().getFunctionRuntimeManager();
        Map<String, Map<String, Function.Assignment>> assignments = functionRuntimeManager.getCurrentAssignments();
        Map<String, Collection<String>> ret = new HashMap<>();
        for (Map.Entry<String, Map<String, Function.Assignment>> entry : assignments.entrySet()) {
            ret.put(entry.getKey(), entry.getValue().keySet());
        }
        return ret;
    }

<<<<<<< HEAD
    public boolean isSuperUser(final String clientRole) {
=======
    private boolean isSuperUser(final String clientRole) {
>>>>>>> f773c602c... Test pr 10 (#27)
        return clientRole != null && worker().getWorkerConfig().getSuperUserRoles().contains(clientRole);
    }

    public List<org.apache.pulsar.common.stats.Metrics> getWorkerMetrics(final String clientRole) {
<<<<<<< HEAD
        if (worker().getWorkerConfig().isAuthorizationEnabled() && !isSuperUser(clientRole)) {
            log.error("Client [{}] is not admin and authorized to get function-stats", clientRole);
            throw new WebApplicationException(Response.status(Status.UNAUTHORIZED).type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorData(clientRole + " is not authorize to get metrics")).build());
        }
        return getWorkerMetrics();
    }

    private List<org.apache.pulsar.common.stats.Metrics> getWorkerMetrics() {
        if (!isWorkerServiceAvailable()) {
            throw new WebApplicationException(
                    Response.status(Status.SERVICE_UNAVAILABLE).type(MediaType.APPLICATION_JSON)
                            .entity(new ErrorData("Function worker service is not available")).build());
=======
        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        if (worker().getWorkerConfig().isAuthorizationEnabled() && !isSuperUser(clientRole)) {
            log.error("Client [{}] is not authorized to get worker stats", clientRole);
            throw new RestException(Status.UNAUTHORIZED, "client is not authorize to perform operation");
>>>>>>> f773c602c... Test pr 10 (#27)
        }
        return worker().getMetricsGenerator().generate();
    }

    public List<WorkerFunctionInstanceStats> getFunctionsMetrics(String clientRole) throws IOException {
<<<<<<< HEAD

        if (worker().getWorkerConfig().isAuthorizationEnabled() && !isSuperUser(clientRole)) {
            log.error("Client [{}] is not admin and authorized to get function-stats", clientRole);
            throw new RestException(Status.UNAUTHORIZED, "client is not authorize to perform operation");
        }
        return getFunctionsMetrics();
    }

    private List<WorkerFunctionInstanceStats> getFunctionsMetrics() throws IOException {
        if (!isWorkerServiceAvailable()) {
            throw new RestException(Status.SERVICE_UNAVAILABLE, "Function worker service is not done initializing. Please try again in a little while.");
        }
=======
        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        if (worker().getWorkerConfig().isAuthorizationEnabled() && !isSuperUser(clientRole)) {
            log.error("Client [{}] is not authorized to get function stats", clientRole);
            throw new RestException(Status.UNAUTHORIZED, "client is not authorize to perform operation");
        }
>>>>>>> f773c602c... Test pr 10 (#27)

        WorkerService workerService = worker();
        Map<String, FunctionRuntimeInfo> functionRuntimes = workerService.getFunctionRuntimeManager()
                .getFunctionRuntimeInfos();

        List<WorkerFunctionInstanceStats> metricsList = new ArrayList<>(functionRuntimes.size());

        for (Map.Entry<String, FunctionRuntimeInfo> entry : functionRuntimes.entrySet()) {
            String fullyQualifiedInstanceName = entry.getKey();
            FunctionRuntimeInfo functionRuntimeInfo = entry.getValue();

            if (workerService.getFunctionRuntimeManager().getRuntimeFactory().externallyManaged()) {
                Function.FunctionDetails functionDetails = functionRuntimeInfo.getFunctionInstance().getFunctionMetaData().getFunctionDetails();
                int parallelism = functionDetails.getParallelism();
                for (int i = 0; i < parallelism; ++i) {
                    FunctionStats.FunctionInstanceStats functionInstanceStats =
<<<<<<< HEAD
                            Utils.getFunctionInstanceStats(fullyQualifiedInstanceName, functionRuntimeInfo, i);
                    WorkerFunctionInstanceStats workerFunctionInstanceStats = new WorkerFunctionInstanceStats();
                    workerFunctionInstanceStats.setName(org.apache.pulsar.functions.utils.Utils.getFullyQualifiedInstanceId(
=======
                            WorkerUtils.getFunctionInstanceStats(fullyQualifiedInstanceName, functionRuntimeInfo, i);
                    WorkerFunctionInstanceStats workerFunctionInstanceStats = new WorkerFunctionInstanceStats();
                    workerFunctionInstanceStats.setName(FunctionCommon.getFullyQualifiedInstanceId(
>>>>>>> f773c602c... Test pr 10 (#27)
                            functionDetails.getTenant(), functionDetails.getNamespace(), functionDetails.getName(), i
                    ));
                    workerFunctionInstanceStats.setMetrics(functionInstanceStats.getMetrics());
                    metricsList.add(workerFunctionInstanceStats);
                }
            } else {
                FunctionStats.FunctionInstanceStats functionInstanceStats =
<<<<<<< HEAD
                        Utils.getFunctionInstanceStats(fullyQualifiedInstanceName, functionRuntimeInfo,
=======
                        WorkerUtils.getFunctionInstanceStats(fullyQualifiedInstanceName, functionRuntimeInfo,
>>>>>>> f773c602c... Test pr 10 (#27)
                                functionRuntimeInfo.getFunctionInstance().getInstanceId());
                WorkerFunctionInstanceStats workerFunctionInstanceStats = new WorkerFunctionInstanceStats();
                workerFunctionInstanceStats.setName(fullyQualifiedInstanceName);
                workerFunctionInstanceStats.setMetrics(functionInstanceStats.getMetrics());
                metricsList.add(workerFunctionInstanceStats);
            }
        }
        return metricsList;
    }
<<<<<<< HEAD
=======

    public List<ConnectorDefinition> getListOfConnectors(String clientRole) {
        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        if (worker().getWorkerConfig().isAuthorizationEnabled() && !isSuperUser(clientRole)) {
            throw new RestException(Status.UNAUTHORIZED, "client is not authorize to perform operation");
        }

        return this.worker().getConnectorsManager().getConnectors();
    }

    public void rebalance(final URI uri, final String clientRole) {
        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        if (worker().getWorkerConfig().isAuthorizationEnabled() && !isSuperUser(clientRole)) {
            log.error("Client [{}] is not authorized rebalance cluster", clientRole);
            throw new RestException(Status.UNAUTHORIZED, "client is not authorize to perform operation");
        }

        if (worker().getLeaderService().isLeader()) {
            try {
                worker().getSchedulerManager().rebalanceIfNotInprogress();
            } catch (SchedulerManager.RebalanceInProgressException e) {
                throw new RestException(Status.BAD_REQUEST, "Rebalance already in progress");
            }
        } else {
            WorkerInfo workerInfo = worker().getMembershipManager().getLeader();
            URI redirect = UriBuilder.fromUri(uri).host(workerInfo.getWorkerHostname()).port(workerInfo.getPort()).build();
            throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
        }
    }

    public Boolean isLeaderReady(final String clientRole) {
        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }
        if (worker().getLeaderService().isLeader()) {
            return true;
        } else {
            throwUnavailableException();
            return false; // make compiler happy
        }
    }
>>>>>>> f773c602c... Test pr 10 (#27)
}
