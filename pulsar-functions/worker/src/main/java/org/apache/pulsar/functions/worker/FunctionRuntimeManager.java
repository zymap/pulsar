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
package org.apache.pulsar.functions.worker;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
<<<<<<< HEAD
=======
import org.apache.pulsar.client.api.Message;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.common.functions.WorkerInfo;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.policies.data.FunctionStats;
<<<<<<< HEAD
import org.apache.pulsar.functions.instance.AuthenticationConfig;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.Assignment;
import org.apache.pulsar.functions.runtime.KubernetesRuntimeFactory;
import org.apache.pulsar.functions.runtime.ProcessRuntimeFactory;
import org.apache.pulsar.functions.runtime.RuntimeFactory;
import org.apache.pulsar.functions.runtime.RuntimeSpawner;
import org.apache.pulsar.functions.runtime.ThreadRuntimeFactory;
import org.apache.pulsar.functions.secretsprovider.ClearTextSecretsProvider;
import org.apache.pulsar.functions.secretsproviderconfigurator.DefaultSecretsProviderConfigurator;
import org.apache.pulsar.functions.secretsproviderconfigurator.SecretsProviderConfigurator;
import org.apache.pulsar.functions.utils.Reflections;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.Collection;
=======
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.util.Reflections;
import org.apache.pulsar.functions.auth.FunctionAuthProvider;
import org.apache.pulsar.functions.instance.AuthenticationConfig;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.Assignment;
import org.apache.pulsar.functions.runtime.RuntimeCustomizer;
import org.apache.pulsar.functions.runtime.RuntimeFactory;
import org.apache.pulsar.functions.runtime.RuntimeSpawner;
import org.apache.pulsar.functions.runtime.kubernetes.KubernetesRuntimeFactory;
import org.apache.pulsar.functions.runtime.process.ProcessRuntimeFactory;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactory;
import org.apache.pulsar.functions.secretsproviderconfigurator.DefaultSecretsProviderConfigurator;
import org.apache.pulsar.functions.secretsproviderconfigurator.SecretsProviderConfigurator;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.utils.FunctionInstanceId;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
>>>>>>> f773c602c... Test pr 10 (#27)
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
<<<<<<< HEAD
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

=======
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;

>>>>>>> f773c602c... Test pr 10 (#27)
/**
 * This class managers all aspects of functions assignments and running of function assignments for this worker
 */
@Slf4j
public class FunctionRuntimeManager implements AutoCloseable{

    // all assignments
    // WorkerId -> Function Fully Qualified InstanceId -> List<Assignments>
    @VisibleForTesting
    Map<String, Map<String, Assignment>> workerIdToAssignments = new ConcurrentHashMap<>();

    // All the runtime info related to functions executed by this worker
    // Fully Qualified InstanceId - > FunctionRuntimeInfo
<<<<<<< HEAD
    // NOTE: please use setFunctionRuntimeInfo and deleteFunctionRuntimeInfo methods to modify this data structure
    // Since during initialization phase nothing should be modified
    @VisibleForTesting
    Map<String, FunctionRuntimeInfo> functionRuntimeInfoMap = new ConcurrentHashMap<>();

    @VisibleForTesting
    @Getter
    final WorkerConfig workerConfig;

    @VisibleForTesting
    LinkedBlockingQueue<FunctionAction> actionQueue;

    private FunctionAssignmentTailer functionAssignmentTailer;

    @Setter
=======
    @VisibleForTesting
    class FunctionRuntimeInfos {

        private Map<String, FunctionRuntimeInfo> functionRuntimeInfoMap = new ConcurrentHashMap<>();

        public FunctionRuntimeInfo get(String fullyQualifiedInstanceId) {
            return functionRuntimeInfoMap.get(fullyQualifiedInstanceId);
        }

        public void put(String fullyQualifiedInstanceId, FunctionRuntimeInfo functionRuntimeInfo) {
            if (!isInitializePhase) {
                functionRuntimeInfoMap.put(fullyQualifiedInstanceId, functionRuntimeInfo);
            }
        }

        public void remove (String fullyQualifiedInstanceId) {
            if (!isInitializePhase) {
                functionRuntimeInfoMap.remove(fullyQualifiedInstanceId);
            }
        }

        public Map<String, FunctionRuntimeInfo> getAll() {
            return functionRuntimeInfoMap;
        }

        public int size() {
            return functionRuntimeInfoMap.size();
        }
    }

    @VisibleForTesting
    final FunctionRuntimeInfos functionRuntimeInfos = new FunctionRuntimeInfos();

    @VisibleForTesting
    @Getter
    final WorkerConfig workerConfig;

    @Setter
    @Getter
>>>>>>> f773c602c... Test pr 10 (#27)
    private FunctionActioner functionActioner;

    @Getter
    private RuntimeFactory runtimeFactory;

    private MembershipManager membershipManager;
<<<<<<< HEAD
    
    private final PulsarAdmin functionAdmin;
    
    @Getter
    private WorkerService workerService;

    @Setter
    @Getter
    boolean isInitializePhase = false;

    private final FunctionMetaDataManager functionMetaDataManager;


    public FunctionRuntimeManager(WorkerConfig workerConfig, WorkerService workerService, Namespace dlogNamespace,
                                  MembershipManager membershipManager, ConnectorsManager connectorsManager,
                                  FunctionMetaDataManager functionMetaDataManager) throws Exception {
=======

    private final PulsarAdmin functionAdmin;

    @Getter
    private WorkerService workerService;

    boolean isInitializePhase = false;

    @Getter
    private final CompletableFuture<Void> isInitialized = new CompletableFuture<>();

    private final FunctionMetaDataManager functionMetaDataManager;

    private final WorkerStatsManager workerStatsManager;

    private final ErrorNotifier errorNotifier;

    public FunctionRuntimeManager(WorkerConfig workerConfig, WorkerService workerService, Namespace dlogNamespace,
                                  MembershipManager membershipManager, ConnectorsManager connectorsManager, FunctionsManager functionsManager,
                                  FunctionMetaDataManager functionMetaDataManager, WorkerStatsManager workerStatsManager, ErrorNotifier errorNotifier) throws Exception {
>>>>>>> f773c602c... Test pr 10 (#27)
        this.workerConfig = workerConfig;
        this.workerService = workerService;
        this.functionAdmin = workerService.getFunctionAdmin();

        SecretsProviderConfigurator secretsProviderConfigurator;
        if (!StringUtils.isEmpty(workerConfig.getSecretsProviderConfiguratorClassName())) {
            secretsProviderConfigurator = (SecretsProviderConfigurator) Reflections.createInstance(workerConfig.getSecretsProviderConfiguratorClassName(), ClassLoader.getSystemClassLoader());
        } else {
            secretsProviderConfigurator = new DefaultSecretsProviderConfigurator();
        }
<<<<<<< HEAD
        secretsProviderConfigurator.init(workerConfig.getSecretsProviderConfiguratorConfig());

        AuthenticationConfig authConfig = AuthenticationConfig.builder()
                .clientAuthenticationPlugin(workerConfig.getClientAuthenticationPlugin())
                .clientAuthenticationParameters(workerConfig.getClientAuthenticationParameters())
                .tlsTrustCertsFilePath(workerConfig.getTlsTrustCertsFilePath())
                .useTls(workerConfig.isUseTls()).tlsAllowInsecureConnection(workerConfig.isTlsAllowInsecureConnection())
                .tlsHostnameVerificationEnable(workerConfig.isTlsHostnameVerificationEnable()).build();

        if (workerConfig.getThreadContainerFactory() != null) {
            this.runtimeFactory = new ThreadRuntimeFactory(
                    workerConfig.getThreadContainerFactory().getThreadGroupName(),
                    workerConfig.getPulsarServiceUrl(),
                    workerConfig.getStateStorageServiceUrl(),
                    authConfig,
                    new ClearTextSecretsProvider(),
                     null);
        } else if (workerConfig.getProcessContainerFactory() != null) {
            this.runtimeFactory = new ProcessRuntimeFactory(
                    workerConfig.getPulsarServiceUrl(),
                    workerConfig.getStateStorageServiceUrl(),
                    authConfig,
                    workerConfig.getProcessContainerFactory().getJavaInstanceJarLocation(),
                    workerConfig.getProcessContainerFactory().getPythonInstanceLocation(),
                    workerConfig.getProcessContainerFactory().getLogDirectory(),
                    workerConfig.getProcessContainerFactory().getExtraFunctionDependenciesDir(),
                    secretsProviderConfigurator);
        } else if (workerConfig.getKubernetesContainerFactory() != null){
            this.runtimeFactory = new KubernetesRuntimeFactory(
                    workerConfig.getKubernetesContainerFactory().getK8Uri(),
                    workerConfig.getKubernetesContainerFactory().getJobNamespace(),
                    workerConfig.getKubernetesContainerFactory().getPulsarDockerImageName(),
                    workerConfig.getKubernetesContainerFactory().getImagePullPolicy(),
                    workerConfig.getKubernetesContainerFactory().getPulsarRootDir(),
                    workerConfig.getKubernetesContainerFactory().getSubmittingInsidePod(),
                    workerConfig.getKubernetesContainerFactory().getInstallUserCodeDependencies(),
                    workerConfig.getKubernetesContainerFactory().getPythonDependencyRepository(),
                    workerConfig.getKubernetesContainerFactory().getPythonExtraDependencyRepository(),
                    workerConfig.getKubernetesContainerFactory().getExtraFunctionDependenciesDir(),
                    workerConfig.getKubernetesContainerFactory().getCustomLabels(),
                    StringUtils.isEmpty(workerConfig.getKubernetesContainerFactory().getPulsarServiceUrl()) ? workerConfig.getPulsarServiceUrl() : workerConfig.getKubernetesContainerFactory().getPulsarServiceUrl(),
                    StringUtils.isEmpty(workerConfig.getKubernetesContainerFactory().getPulsarAdminUrl()) ? workerConfig.getPulsarWebServiceUrl() : workerConfig.getKubernetesContainerFactory().getPulsarAdminUrl(),
                    workerConfig.getStateStorageServiceUrl(),
                    authConfig,
                    workerConfig.getKubernetesContainerFactory().getExpectedMetricsCollectionInterval() == null ? -1 : workerConfig.getKubernetesContainerFactory().getExpectedMetricsCollectionInterval(),
                    workerConfig.getKubernetesContainerFactory().getChangeConfigMap(),
                    workerConfig.getKubernetesContainerFactory().getChangeConfigMapNamespace(),
                    workerConfig.getFunctionInstanceMinResources(),
                    secretsProviderConfigurator);
        } else {
            throw new RuntimeException("Either Thread, Process or Kubernetes Container Factory need to be set");
        }

        this.actionQueue = new LinkedBlockingQueue<>();

        this.functionActioner = new FunctionActioner(this.workerConfig, runtimeFactory,
                dlogNamespace, actionQueue, connectorsManager, workerService.getBrokerAdmin());

        this.membershipManager = membershipManager;
        this.functionMetaDataManager = functionMetaDataManager;
=======
        log.info("Initializing secrets provider configurator {} with configs: {}",
          secretsProviderConfigurator.getClass().getName(), workerConfig.getSecretsProviderConfiguratorConfig());
        secretsProviderConfigurator.init(workerConfig.getSecretsProviderConfiguratorConfig());

        Optional<FunctionAuthProvider> functionAuthProvider = Optional.empty();
        AuthenticationConfig authConfig = null;
        if (workerConfig.isAuthenticationEnabled()) {
            authConfig = AuthenticationConfig.builder()
                    .clientAuthenticationPlugin(workerConfig.getBrokerClientAuthenticationPlugin())
                    .clientAuthenticationParameters(workerConfig.getBrokerClientAuthenticationParameters())
                    .tlsTrustCertsFilePath(workerConfig.getTlsTrustCertsFilePath())
                    .useTls(workerConfig.isUseTls())
                    .tlsAllowInsecureConnection(workerConfig.isTlsAllowInsecureConnection())
                    .tlsHostnameVerificationEnable(workerConfig.isTlsEnableHostnameVerification())
                    .build();

            //initialize function authentication provider
            if (!StringUtils.isEmpty(workerConfig.getFunctionAuthProviderClassName())) {
                functionAuthProvider = Optional.of(FunctionAuthProvider.getAuthProvider(workerConfig.getFunctionAuthProviderClassName()));
            }
        }

        // initialize the runtime customizer
        Optional<RuntimeCustomizer> runtimeCustomizer = Optional.empty();
        if (!StringUtils.isEmpty(workerConfig.getRuntimeCustomizerClassName())) {
            runtimeCustomizer = Optional.of(RuntimeCustomizer.getRuntimeCustomizer(workerConfig.getRuntimeCustomizerClassName()));
            runtimeCustomizer.get().initialize(Optional.ofNullable(workerConfig.getRuntimeCustomizerConfig()).orElse(Collections.emptyMap()));
        }

        // initialize function runtime factory
        if (!StringUtils.isEmpty(workerConfig.getFunctionRuntimeFactoryClassName())) {
            this.runtimeFactory = RuntimeFactory.getFuntionRuntimeFactory(workerConfig.getFunctionRuntimeFactoryClassName());
        } else {
            if (workerConfig.getThreadContainerFactory() != null) {
                this.runtimeFactory = new ThreadRuntimeFactory();
                workerConfig.setFunctionRuntimeFactoryConfigs(
                        ObjectMapperFactory.getThreadLocal().convertValue(workerConfig.getThreadContainerFactory(), Map.class));
            } else if (workerConfig.getProcessContainerFactory() != null) {
                this.runtimeFactory = new ProcessRuntimeFactory();
                workerConfig.setFunctionRuntimeFactoryConfigs(
                        ObjectMapperFactory.getThreadLocal().convertValue(workerConfig.getProcessContainerFactory(), Map.class));
            } else if (workerConfig.getKubernetesContainerFactory() != null) {
                this.runtimeFactory = new KubernetesRuntimeFactory();
                workerConfig.setFunctionRuntimeFactoryConfigs(
                        ObjectMapperFactory.getThreadLocal().convertValue(workerConfig.getKubernetesContainerFactory(), Map.class));
            } else {
                throw new RuntimeException("A Function Runtime Factory needs to be set");
            }
        }
        // initialize runtime
        this.runtimeFactory.initialize(workerConfig, authConfig, secretsProviderConfigurator, functionAuthProvider, runtimeCustomizer);

        this.functionActioner = new FunctionActioner(this.workerConfig, runtimeFactory,
                dlogNamespace, connectorsManager, functionsManager, workerService.getBrokerAdmin());

        this.membershipManager = membershipManager;
        this.functionMetaDataManager = functionMetaDataManager;
        this.workerStatsManager = workerStatsManager;
        this.errorNotifier = errorNotifier;
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    /**
     * Initializes the FunctionRuntimeManager.  Does the following:
     * 1. Consume all existing assignments to establish existing/latest set of assignments
     * 2. After current assignments are read, assignments belonging to this worker will be processed
<<<<<<< HEAD
     */
    public void initialize() {
        log.info("/** Initializing Runtime Manager **/");
        try {
            Reader<byte[]> reader = this.getWorkerService().getClient().newReader()
                    .topic(this.getWorkerConfig().getFunctionAssignmentTopic()).readCompacted(true)
                    .startMessageId(MessageId.earliest).create();

            this.functionAssignmentTailer = new FunctionAssignmentTailer(this, reader);
            // read all existing messages
            this.setInitializePhase(true);
            while (reader.hasMessageAvailable()) {
                this.functionAssignmentTailer.processAssignment(reader.readNext());
            }
            this.setInitializePhase(false);
=======
     *
     * @return the message id of the message processed during init phase
     */
    public MessageId initialize() {
        try {
            Reader<byte[]> reader = WorkerUtils.createReader(
                    workerService.getClient().newReader(),
                    workerConfig.getWorkerId() + "-function-assignment-initialize",
                    workerConfig.getFunctionAssignmentTopic(),
                    MessageId.earliest);

            // start init phase
            this.isInitializePhase = true;
            // keep track of the last message read
            MessageId lastMessageRead = MessageId.earliest;
            // read all existing messages
            while (reader.hasMessageAvailable()) {
                Message<byte[]> message = reader.readNext();
                lastMessageRead = message.getMessageId();
                processAssignmentMessage(message);
            }
            // init phase is done
            this.isInitializePhase = false;
            // close reader
            reader.close();
>>>>>>> f773c602c... Test pr 10 (#27)
            // realize existing assignments
            Map<String, Assignment> assignmentMap = workerIdToAssignments.get(this.workerConfig.getWorkerId());
            if (assignmentMap != null) {
                for (Assignment assignment : assignmentMap.values()) {
                    if (needsStart(assignment)) {
                        startFunctionInstance(assignment);
                    }
                }
            }
<<<<<<< HEAD
            // start assignment tailer
            this.functionAssignmentTailer.start();

        } catch (Exception e) {
            log.error("Failed to initialize function runtime manager: ", e.getMessage(), e);
=======
            // complete future to indicate initialization is complete
            isInitialized.complete(null);
            return lastMessageRead;
        } catch (Exception e) {
            log.error("Failed to initialize function runtime manager: {}", e.getMessage(), e);
>>>>>>> f773c602c... Test pr 10 (#27)
            throw new RuntimeException(e);
        }
    }

    /**
     * Starts the function runtime manager
     */
<<<<<<< HEAD
    public void start() {
        log.info("/** Starting Function Runtime Manager **/");
        log.info("Initialize metrics sink...");
        log.info("Starting function actioner...");
        this.functionActioner.start();
        log.info("Starting function assignment tailer...");
        this.functionAssignmentTailer.start();
    }

    /**
     * Public methods
     */

    /**
     * Get current assignments
     * @return a map of current assignments in the follwing format
=======

    /**
     * Get current assignments
     * @return a map of current assignments in the following format
>>>>>>> f773c602c... Test pr 10 (#27)
     * {workerId : {FullyQualifiedInstanceId : Assignment}}
     */
    public synchronized Map<String, Map<String, Assignment>> getCurrentAssignments() {
        Map<String, Map<String, Assignment>> copy = new HashMap<>();
        for (Map.Entry<String, Map<String, Assignment>> entry : this.workerIdToAssignments.entrySet()) {
            Map<String, Assignment> tmp = new HashMap<>();
            tmp.putAll(entry.getValue());
            copy.put(entry.getKey(), tmp);
        }
        return copy;
    }

    /**
     * Find a assignment of a function
     * @param tenant the tenant the function belongs to
     * @param namespace the namespace the function belongs to
     * @param functionName the function name
     * @return the assignment of the function
     */
    public synchronized Assignment findFunctionAssignment(String tenant, String namespace,
                                                          String functionName, int instanceId) {
        return this.findAssignment(tenant, namespace, functionName, instanceId);
    }

    /**
     * Find all instance assignments of function
     * @param tenant
     * @param namespace
     * @param functionName
     * @return
     */
    public synchronized Collection<Assignment> findFunctionAssignments(String tenant,
                                                                       String namespace, String functionName) {
        return findFunctionAssignments(tenant, namespace, functionName, this.workerIdToAssignments);
    }

    public static Collection<Assignment> findFunctionAssignments(String tenant,
                                                                 String namespace, String functionName,
                                                                 Map<String, Map<String, Assignment>> workerIdToAssignments) {

        Collection<Assignment> assignments = new LinkedList<>();

        for (Map<String, Assignment> entryMap : workerIdToAssignments.values()) {
            assignments.addAll(entryMap.values().stream()
                    .filter(
                            assignment ->
                                    (tenant.equals(assignment.getInstance()
                                            .getFunctionMetaData().getFunctionDetails()
                                            .getTenant())
                                            && namespace.equals((assignment.getInstance()
                                            .getFunctionMetaData().getFunctionDetails()
                                            .getNamespace()))
                                            && functionName.equals(assignment.getInstance()
                                            .getFunctionMetaData().getFunctionDetails()
                                            .getName())))
                    .collect(Collectors.toList()));
        }

        return assignments;
    }

<<<<<<< HEAD

=======
>>>>>>> f773c602c... Test pr 10 (#27)
    /**
     * Removes a collection of assignments
     * @param assignments assignments to remove
     */
    public synchronized void removeAssignments(Collection<Assignment> assignments) {
        for (Assignment assignment : assignments) {
            this.deleteAssignment(assignment);
        }
    }

<<<<<<< HEAD
    public void restartFunctionInstance(String tenant, String namespace, String functionName, int instanceId,
=======
    public synchronized void restartFunctionInstance(String tenant, String namespace, String functionName, int instanceId,
>>>>>>> f773c602c... Test pr 10 (#27)
            URI uri) throws Exception {
        if (runtimeFactory.externallyManaged()) {
            throw new WebApplicationException(Response.serverError().status(Status.NOT_IMPLEMENTED)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorData("Externally managed schedulers can't do per instance stop")).build());
        }
        Assignment assignment = this.findAssignment(tenant, namespace, functionName, instanceId);
        final String fullFunctionName = String.format("%s/%s/%s/%s", tenant, namespace, functionName, instanceId);
        if (assignment == null) {
            throw new WebApplicationException(Response.serverError().status(Status.BAD_REQUEST)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorData(fullFunctionName + " doesn't exist")).build());
        }

        final String assignedWorkerId = assignment.getWorkerId();
        final String workerId = this.workerConfig.getWorkerId();

        if (assignedWorkerId.equals(workerId)) {
<<<<<<< HEAD
            stopFunction(org.apache.pulsar.functions.utils.Utils.getFullyQualifiedInstanceId(assignment.getInstance()), true);
=======
            stopFunction(FunctionCommon.getFullyQualifiedInstanceId(assignment.getInstance()), true);
>>>>>>> f773c602c... Test pr 10 (#27)
            return;
        } else {
            // query other worker
            List<WorkerInfo> workerInfoList = this.membershipManager.getCurrentMembership();
            WorkerInfo workerInfo = null;
            for (WorkerInfo entry : workerInfoList) {
                if (assignment.getWorkerId().equals(entry.getWorkerId())) {
                    workerInfo = entry;
                }
            }
            if (workerInfo == null) {
                throw new WebApplicationException(Response.serverError().status(Status.BAD_REQUEST)
                        .type(MediaType.APPLICATION_JSON)
                        .entity(new ErrorData(fullFunctionName + " has not been assigned yet")).build());
            }

            if (uri == null) {
                throw new WebApplicationException(Response.serverError().status(Status.INTERNAL_SERVER_ERROR).build());
            } else {
                URI redirect = UriBuilder.fromUri(uri).host(workerInfo.getWorkerHostname()).port(workerInfo.getPort()).build();
                throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
            }
        }
    }

<<<<<<< HEAD
    public void restartFunctionInstances(String tenant, String namespace, String functionName)
=======
    public synchronized void restartFunctionInstances(String tenant, String namespace, String functionName)
>>>>>>> f773c602c... Test pr 10 (#27)
            throws Exception {
        final String fullFunctionName = String.format("%s/%s/%s", tenant, namespace, functionName);
        Collection<Assignment> assignments = this.findFunctionAssignments(tenant, namespace, functionName);

        if (assignments.isEmpty()) {
            throw new WebApplicationException(Response.serverError().status(Status.BAD_REQUEST)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorData(fullFunctionName + " has not been assigned yet")).build());
        }
        if (runtimeFactory.externallyManaged()) {
            Assignment assignment = assignments.iterator().next();
            final String assignedWorkerId = assignment.getWorkerId();
            final String workerId = this.workerConfig.getWorkerId();
<<<<<<< HEAD
            String fullyQualifiedInstanceId = org.apache.pulsar.functions.utils.Utils.getFullyQualifiedInstanceId(assignment.getInstance());
=======
            String fullyQualifiedInstanceId = FunctionCommon.getFullyQualifiedInstanceId(assignment.getInstance());
>>>>>>> f773c602c... Test pr 10 (#27)
            if (assignedWorkerId.equals(workerId)) {
                stopFunction(fullyQualifiedInstanceId, true);
            } else {
                List<WorkerInfo> workerInfoList = this.membershipManager.getCurrentMembership();
                WorkerInfo workerInfo = null;
                for (WorkerInfo entry : workerInfoList) {
                    if (assignment.getWorkerId().equals(entry.getWorkerId())) {
                        workerInfo = entry;
                    }
                }
                if (workerInfo == null) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] has not been assigned yet", fullyQualifiedInstanceId);
                    }
                    throw new WebApplicationException(Response.serverError().status(Status.BAD_REQUEST)
                            .type(MediaType.APPLICATION_JSON)
                            .entity(new ErrorData(fullFunctionName + " has not been assigned yet")).build());
                }
                this.functionAdmin.functions().restartFunction(tenant, namespace, functionName);
            }
        } else {
            for (Assignment assignment : assignments) {
                final String assignedWorkerId = assignment.getWorkerId();
                final String workerId = this.workerConfig.getWorkerId();
<<<<<<< HEAD
                String fullyQualifiedInstanceId = org.apache.pulsar.functions.utils.Utils.getFullyQualifiedInstanceId(assignment.getInstance());
=======
                String fullyQualifiedInstanceId = FunctionCommon.getFullyQualifiedInstanceId(assignment.getInstance());
>>>>>>> f773c602c... Test pr 10 (#27)
                if (assignedWorkerId.equals(workerId)) {
                    stopFunction(fullyQualifiedInstanceId, true);
                } else {
                    List<WorkerInfo> workerInfoList = this.membershipManager.getCurrentMembership();
                    WorkerInfo workerInfo = null;
                    for (WorkerInfo entry : workerInfoList) {
                        if (assignment.getWorkerId().equals(entry.getWorkerId())) {
                            workerInfo = entry;
                        }
                    }
                    if (workerInfo == null) {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] has not been assigned yet", fullyQualifiedInstanceId);
                        }
                        continue;
                    }
                    this.functionAdmin.functions().restartFunction(tenant, namespace, functionName,
                                assignment.getInstance().getInstanceId());
                }
            }
        }
        return;
    }

    /**
     * It stops all functions instances owned by current worker
     * @throws Exception
     */
    public void stopAllOwnedFunctions() {
        if (runtimeFactory.externallyManaged()) {
            log.warn("Will not stop any functions since they are externally managed");
            return;
        }
        final String workerId = this.workerConfig.getWorkerId();
        Map<String, Assignment> assignments = workerIdToAssignments.get(workerId);
        if (assignments != null) {
<<<<<<< HEAD
            assignments.values().forEach(assignment -> {
                String fullyQualifiedInstanceId = org.apache.pulsar.functions.utils.Utils.getFullyQualifiedInstanceId(assignment.getInstance());
=======
            // Take a copy of the map since the stopFunction will modify the same map
            // and invalidate the iterator
            Map<String, Assignment> copiedAssignments = new TreeMap<>(assignments);
            copiedAssignments.values().forEach(assignment -> {
                String fullyQualifiedInstanceId = FunctionCommon.getFullyQualifiedInstanceId(assignment.getInstance());
>>>>>>> f773c602c... Test pr 10 (#27)
                try {
                    stopFunction(fullyQualifiedInstanceId, false);
                } catch (Exception e) {
                    log.warn("Failed to stop function {} - {}", fullyQualifiedInstanceId, e.getMessage());
                }
            });
        }
    }

    private void stopFunction(String fullyQualifiedInstanceId, boolean restart) throws Exception {
        log.info("[{}] {}..", restart ? "restarting" : "stopping", fullyQualifiedInstanceId);
        FunctionRuntimeInfo functionRuntimeInfo = this.getFunctionRuntimeInfo(fullyQualifiedInstanceId);
        if (functionRuntimeInfo != null) {
<<<<<<< HEAD
            this.functionActioner.stopFunction(functionRuntimeInfo);
            try {
                if(restart) {
                    this.functionActioner.startFunction(functionRuntimeInfo);
=======
            this.conditionallyStopFunction(functionRuntimeInfo);
            try {
                if(restart) {
                    this.conditionallyStartFunction(functionRuntimeInfo);
>>>>>>> f773c602c... Test pr 10 (#27)
                }
            } catch (Exception ex) {
                log.info("{} Error re-starting function", fullyQualifiedInstanceId, ex);
                functionRuntimeInfo.setStartupException(ex);
                throw ex;
            }
        }
    }

    /**
<<<<<<< HEAD
     * Get stats of a function instance.  If this worker is not running the function instance,
=======
     * Get stats of a function instance.  If this worker is not running the function instance.
>>>>>>> f773c602c... Test pr 10 (#27)
     * @param tenant the tenant the function belongs to
     * @param namespace the namespace the function belongs to
     * @param functionName the function name
     * @param instanceId the function instance id
     * @return jsonObject containing stats for instance
     */
    public FunctionStats.FunctionInstanceStats.FunctionInstanceStatsData getFunctionInstanceStats(String tenant, String namespace,
                                                                        String functionName, int instanceId, URI uri) {
        Assignment assignment;
        if (runtimeFactory.externallyManaged()) {
            assignment = this.findAssignment(tenant, namespace, functionName, -1);
        } else {
            assignment = this.findAssignment(tenant, namespace, functionName, instanceId);
        }

        if (assignment == null) {
            return new FunctionStats.FunctionInstanceStats.FunctionInstanceStatsData();
        }

        final String assignedWorkerId = assignment.getWorkerId();
        final String workerId = this.workerConfig.getWorkerId();

        // If I am running worker
        if (assignedWorkerId.equals(workerId)) {
            FunctionRuntimeInfo functionRuntimeInfo = this.getFunctionRuntimeInfo(
<<<<<<< HEAD
                    org.apache.pulsar.functions.utils.Utils.getFullyQualifiedInstanceId(assignment.getInstance()));
            RuntimeSpawner runtimeSpawner = functionRuntimeInfo.getRuntimeSpawner();
            if (runtimeSpawner != null) {
                return Utils.getFunctionInstanceStats(org.apache.pulsar.functions.utils.Utils.getFullyQualifiedInstanceId(assignment.getInstance()), functionRuntimeInfo, instanceId).getMetrics();
=======
                    FunctionCommon.getFullyQualifiedInstanceId(assignment.getInstance()));
            RuntimeSpawner runtimeSpawner = functionRuntimeInfo.getRuntimeSpawner();
            if (runtimeSpawner != null) {
                return WorkerUtils.getFunctionInstanceStats(FunctionCommon.getFullyQualifiedInstanceId(assignment.getInstance()), functionRuntimeInfo, instanceId).getMetrics();
>>>>>>> f773c602c... Test pr 10 (#27)
            }
            return new FunctionStats.FunctionInstanceStats.FunctionInstanceStatsData();
        } else {
            // query other worker

            List<WorkerInfo> workerInfoList = this.membershipManager.getCurrentMembership();
            WorkerInfo workerInfo = null;
            for (WorkerInfo entry: workerInfoList) {
                if (assignment.getWorkerId().equals(entry.getWorkerId())) {
                    workerInfo = entry;
                }
            }
            if (workerInfo == null) {
                return new FunctionStats.FunctionInstanceStats.FunctionInstanceStatsData();
            }

            if (uri == null) {
                throw new WebApplicationException(Response.serverError().status(Status.INTERNAL_SERVER_ERROR).build());
            } else {
                URI redirect = UriBuilder.fromUri(uri).host(workerInfo.getWorkerHostname()).port(workerInfo.getPort()).build();
                throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
            }
        }
    }

    /**
     * Get stats of all function instances.
     * @param tenant the tenant the function belongs to
     * @param namespace the namespace the function belongs to
     * @param functionName the function name
     * @return a list of function statuses
     * @throws PulsarAdminException
     */
    public FunctionStats getFunctionStats(String tenant, String namespace, String functionName, URI uri) throws PulsarAdminException {
        Collection<Assignment> assignments = this.findFunctionAssignments(tenant, namespace, functionName);

        FunctionStats functionStats = new FunctionStats();
        if (assignments.isEmpty()) {
            return functionStats;
        }

        if (runtimeFactory.externallyManaged()) {
            Assignment assignment = assignments.iterator().next();
            boolean isOwner = this.workerConfig.getWorkerId().equals(assignment.getWorkerId());
            if (isOwner) {
                int parallelism = assignment.getInstance().getFunctionMetaData().getFunctionDetails().getParallelism();
                for (int i = 0; i < parallelism; ++i) {

                    FunctionStats.FunctionInstanceStats.FunctionInstanceStatsData functionInstanceStatsData = getFunctionInstanceStats(tenant, namespace,
                            functionName, i, null);

                    FunctionStats.FunctionInstanceStats functionInstanceStats = new FunctionStats.FunctionInstanceStats();
                    functionInstanceStats.setInstanceId(i);
                    functionInstanceStats.setMetrics(functionInstanceStatsData);
                    functionStats.addInstance(functionInstanceStats);
                }
            } else {
                // find the hostname/port of the worker who is the owner

                List<WorkerInfo> workerInfoList = this.membershipManager.getCurrentMembership();
                WorkerInfo workerInfo = null;
                for (WorkerInfo entry: workerInfoList) {
                    if (assignment.getWorkerId().equals(entry.getWorkerId())) {
                        workerInfo = entry;
                    }
                }
                if (workerInfo == null) {
                    return functionStats;
                }

                if (uri == null) {
                    throw new WebApplicationException(Response.serverError().status(Status.INTERNAL_SERVER_ERROR).build());
                } else {
                    URI redirect = UriBuilder.fromUri(uri).host(workerInfo.getWorkerHostname()).port(workerInfo.getPort()).build();
                    throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
                }
            }
        } else {
            for (Assignment assignment : assignments) {
                boolean isOwner = this.workerConfig.getWorkerId().equals(assignment.getWorkerId());

                FunctionStats.FunctionInstanceStats.FunctionInstanceStatsData functionInstanceStatsData;
                if (isOwner) {
                    functionInstanceStatsData = getFunctionInstanceStats(tenant, namespace, functionName,
                            assignment.getInstance().getInstanceId(), null);
                } else {
                    functionInstanceStatsData = this.functionAdmin.functions().getFunctionStats(
                            assignment.getInstance().getFunctionMetaData().getFunctionDetails().getTenant(),
                            assignment.getInstance().getFunctionMetaData().getFunctionDetails().getNamespace(),
                            assignment.getInstance().getFunctionMetaData().getFunctionDetails().getName(),
                            assignment.getInstance().getInstanceId());
                }

                FunctionStats.FunctionInstanceStats functionInstanceStats = new FunctionStats.FunctionInstanceStats();
                functionInstanceStats.setInstanceId(assignment.getInstance().getInstanceId());
                functionInstanceStats.setMetrics(functionInstanceStatsData);
                functionStats.addInstance(functionInstanceStats);
            }
        }
        return functionStats.calculateOverall();
    }

<<<<<<< HEAD
=======

    public synchronized void processAssignmentMessage(Message<byte[]> msg) {

        if(msg.getData()==null || (msg.getData().length==0)) {
            log.info("Received assignment delete: {}", msg.getKey());
            deleteAssignment(msg.getKey());
        } else {
            Assignment assignment;
            try {
                assignment = Assignment.parseFrom(msg.getData());
            } catch (IOException e) {
                log.error("[{}] Received bad assignment update at message {}", msg.getMessageId(), e);
                throw new RuntimeException(e);
            }
            log.info("Received assignment update: {}", assignment);
            processAssignment(assignment);
        }
    }

>>>>>>> f773c602c... Test pr 10 (#27)
    /**
     * Process an assignment update from the assignment topic
     * @param newAssignment the assignment
     */
    public synchronized void processAssignment(Assignment newAssignment) {

<<<<<<< HEAD
        Map<String, Assignment> existingAssignmentMap = new HashMap<>();
        for (Map<String, Assignment> entry : this.workerIdToAssignments.values()) {
            existingAssignmentMap.putAll(entry);
        }

        if (existingAssignmentMap.containsKey(org.apache.pulsar.functions.utils.Utils.getFullyQualifiedInstanceId(newAssignment.getInstance()))) {
=======
        boolean exists = false;
        for (Map<String, Assignment> entry : this.workerIdToAssignments.values()) {
            if (entry.containsKey(FunctionCommon.getFullyQualifiedInstanceId(newAssignment.getInstance()))) {
                exists = true;
            }
        }

        if (exists) {
>>>>>>> f773c602c... Test pr 10 (#27)
            updateAssignment(newAssignment);
        } else {
            addAssignment(newAssignment);
        }
    }

    private void updateAssignment(Assignment assignment) {
<<<<<<< HEAD
        String fullyQualifiedInstanceId = org.apache.pulsar.functions.utils.Utils.getFullyQualifiedInstanceId(assignment.getInstance());
=======
        String fullyQualifiedInstanceId = FunctionCommon.getFullyQualifiedInstanceId(assignment.getInstance());
>>>>>>> f773c602c... Test pr 10 (#27)
        Assignment existingAssignment = this.findAssignment(assignment);
        // potential updates need to happen

        if (!existingAssignment.equals(assignment)) {
            FunctionRuntimeInfo functionRuntimeInfo = _getFunctionRuntimeInfo(fullyQualifiedInstanceId);

            // for externally managed functions we don't really care about which worker the function instance is assigned to
            // and we don't really need to stop and start the function instance because the worker its assigned to changed
            // we just need to update the locally cached assignment info.  We only need to stop and start when there are
            // changes to the function meta data of the instance

            if (runtimeFactory.externallyManaged()) {
<<<<<<< HEAD

=======
>>>>>>> f773c602c... Test pr 10 (#27)
                // change in metadata thus need to potentially restart
                if (!assignment.getInstance().equals(existingAssignment.getInstance())) {
                    //stop function
                    if (functionRuntimeInfo != null) {
<<<<<<< HEAD
                        this.insertStopAction(functionRuntimeInfo);
=======
                        this.conditionallyStopFunction(functionRuntimeInfo);
>>>>>>> f773c602c... Test pr 10 (#27)
                    }
                    // still assigned to me, need to restart
                    if (assignment.getWorkerId().equals(this.workerConfig.getWorkerId())) {
                        if (needsStart(assignment)) {
                            //start again
                            FunctionRuntimeInfo newFunctionRuntimeInfo = new FunctionRuntimeInfo();
                            newFunctionRuntimeInfo.setFunctionInstance(assignment.getInstance());

<<<<<<< HEAD
                            this.insertStartAction(newFunctionRuntimeInfo);
                            this.setFunctionRuntimeInfo(fullyQualifiedInstanceId, newFunctionRuntimeInfo);
                        }
                    } else {
                        deleteFunctionRuntimeInfo(fullyQualifiedInstanceId);
=======
                            this.conditionallyStartFunction(newFunctionRuntimeInfo);
                            this.functionRuntimeInfos.put(fullyQualifiedInstanceId, newFunctionRuntimeInfo);
                        }
                    } else {
                        this.functionRuntimeInfos.remove(fullyQualifiedInstanceId);
>>>>>>> f773c602c... Test pr 10 (#27)
                    }
                } else {
                    // if assignment got transferred to me just set function runtime
                    if (assignment.getWorkerId().equals(this.workerConfig.getWorkerId())) {
                        FunctionRuntimeInfo newFunctionRuntimeInfo = new FunctionRuntimeInfo();
                        newFunctionRuntimeInfo.setFunctionInstance(assignment.getInstance());
                        RuntimeSpawner runtimeSpawner = functionActioner.getRuntimeSpawner(
                                assignment.getInstance(),
                                assignment.getInstance().getFunctionMetaData().getPackageLocation().getPackagePath());
<<<<<<< HEAD
                        newFunctionRuntimeInfo.setRuntimeSpawner(runtimeSpawner);

                        this.setFunctionRuntimeInfo(fullyQualifiedInstanceId, newFunctionRuntimeInfo);
                    } else {
                        deleteFunctionRuntimeInfo(fullyQualifiedInstanceId);
=======
                        // re-initialize if necessary
                        runtimeSpawner.getRuntime().reinitialize();
                        newFunctionRuntimeInfo.setRuntimeSpawner(runtimeSpawner);

                        this.functionRuntimeInfos.put(fullyQualifiedInstanceId, newFunctionRuntimeInfo);
                    } else {
                        this.functionRuntimeInfos.remove(fullyQualifiedInstanceId);
>>>>>>> f773c602c... Test pr 10 (#27)
                    }
                }
            } else {
                //stop function
                if (functionRuntimeInfo != null) {
<<<<<<< HEAD
                    this.insertStopAction(functionRuntimeInfo);
=======
                    this.conditionallyStopFunction(functionRuntimeInfo);
>>>>>>> f773c602c... Test pr 10 (#27)
                }
                // still assigned to me, need to restart
                if (assignment.getWorkerId().equals(this.workerConfig.getWorkerId())) {
                    if (needsStart(assignment)) {
                        //start again
                        FunctionRuntimeInfo newFunctionRuntimeInfo = new FunctionRuntimeInfo();
                        newFunctionRuntimeInfo.setFunctionInstance(assignment.getInstance());
<<<<<<< HEAD
                        this.insertStartAction(newFunctionRuntimeInfo);
                        this.setFunctionRuntimeInfo(fullyQualifiedInstanceId, newFunctionRuntimeInfo);
                    }
                } else {
                    deleteFunctionRuntimeInfo(fullyQualifiedInstanceId);
=======
                        this.conditionallyStartFunction(newFunctionRuntimeInfo);
                        this.functionRuntimeInfos.put(fullyQualifiedInstanceId, newFunctionRuntimeInfo);
                    }
                } else {
                    this.functionRuntimeInfos.remove(fullyQualifiedInstanceId);
>>>>>>> f773c602c... Test pr 10 (#27)
                }
            }

            // find existing assignment
            Assignment existing_assignment = this.findAssignment(assignment);
            if (existing_assignment != null) {
                // delete old assignment that could have old data
                this.deleteAssignment(existing_assignment);
            }
            // set to newest assignment
            this.setAssignment(assignment);
        }
    }
<<<<<<< HEAD
   
=======

>>>>>>> f773c602c... Test pr 10 (#27)
    public synchronized void deleteAssignment(String fullyQualifiedInstanceId) {
        FunctionRuntimeInfo functionRuntimeInfo = _getFunctionRuntimeInfo(fullyQualifiedInstanceId);
        if (functionRuntimeInfo != null) {
            Function.FunctionDetails functionDetails = functionRuntimeInfo.getFunctionInstance().getFunctionMetaData().getFunctionDetails();

            // check if this is part of a function delete operation or update operation
            // TODO could be a race condition here if functionMetaDataTailer somehow does not receive the functionMeta prior to the functionAssignmentsTailer gets the assignment for the function.
            if (this.functionMetaDataManager.containsFunction(functionDetails.getTenant(), functionDetails.getNamespace(), functionDetails.getName())) {
                // function still exists thus probably an update or stop operation
<<<<<<< HEAD
                this.insertStopAction(functionRuntimeInfo);
            } else {
                // function doesn't exist anymore thus we should terminate
                this.insertTerminateAction(functionRuntimeInfo);
            }
            this.deleteFunctionRuntimeInfo(fullyQualifiedInstanceId);
        }
        
=======
                this.conditionallyStopFunction(functionRuntimeInfo);
            } else {
                // function doesn't exist anymore thus we should terminate

                FunctionInstanceId functionInstanceId
                        = new FunctionInstanceId(fullyQualifiedInstanceId);
                String name = functionInstanceId.getName();
                String namespace = functionInstanceId.getNamespace();
                String tenant = functionInstanceId.getTenant();

                // only run the termination logic if
                // this is the last/only instance from a function left on the worker
                Collection<Assignment> assignments = findFunctionAssignments(tenant, namespace, name, this
                        .workerIdToAssignments);
                if (assignments.size() > 1) {
                    this.conditionallyStopFunction(functionRuntimeInfo);
                } else {
                    this.conditionallyTerminateFunction(functionRuntimeInfo);
                }
            }
            this.functionRuntimeInfos.remove(fullyQualifiedInstanceId);
        }

>>>>>>> f773c602c... Test pr 10 (#27)
        String workerId = null;
        for(Entry<String, Map<String, Assignment>> workerAssignments : workerIdToAssignments.entrySet()) {
            if(workerAssignments.getValue().remove(fullyQualifiedInstanceId)!=null) {
                workerId = workerAssignments.getKey();
                break;
            }
        }
        Map<String, Assignment> worker;
        if (workerId != null && ((worker = workerIdToAssignments.get(workerId)) != null && worker.isEmpty())) {
            this.workerIdToAssignments.remove(workerId);
        }
    }

    @VisibleForTesting
    void deleteAssignment(Assignment assignment) {
<<<<<<< HEAD
        String fullyQualifiedInstanceId = org.apache.pulsar.functions.utils.Utils.getFullyQualifiedInstanceId(assignment.getInstance());
        Map<String, Assignment> assignmentMap = this.workerIdToAssignments.get(assignment.getWorkerId());
        if (assignmentMap != null) {
            if (assignmentMap.containsKey(fullyQualifiedInstanceId)) {
                assignmentMap.remove(fullyQualifiedInstanceId);
            }
=======
        String fullyQualifiedInstanceId = FunctionCommon.getFullyQualifiedInstanceId(assignment.getInstance());
        Map<String, Assignment> assignmentMap = this.workerIdToAssignments.get(assignment.getWorkerId());
        if (assignmentMap != null) {
            assignmentMap.remove(fullyQualifiedInstanceId);
>>>>>>> f773c602c... Test pr 10 (#27)
            if (assignmentMap.isEmpty()) {
                this.workerIdToAssignments.remove(assignment.getWorkerId());
            }
        }
    }

    private void addAssignment(Assignment assignment) {
        //add new function
        this.setAssignment(assignment);

        //Assigned to me
        if (assignment.getWorkerId().equals(workerConfig.getWorkerId()) && needsStart(assignment)) {
            startFunctionInstance(assignment);
        }
    }

    private void startFunctionInstance(Assignment assignment) {
<<<<<<< HEAD
        String fullyQualifiedInstanceId = org.apache.pulsar.functions.utils.Utils.getFullyQualifiedInstanceId(assignment.getInstance());
=======
        String fullyQualifiedInstanceId = FunctionCommon.getFullyQualifiedInstanceId(assignment.getInstance());
>>>>>>> f773c602c... Test pr 10 (#27)
        FunctionRuntimeInfo functionRuntimeInfo = _getFunctionRuntimeInfo(fullyQualifiedInstanceId);

        if (functionRuntimeInfo == null) {
            functionRuntimeInfo = new FunctionRuntimeInfo()
                    .setFunctionInstance(assignment.getInstance());
<<<<<<< HEAD
            this.setFunctionRuntimeInfo(fullyQualifiedInstanceId, functionRuntimeInfo);

=======
            this.functionRuntimeInfos.put(fullyQualifiedInstanceId, functionRuntimeInfo);
>>>>>>> f773c602c... Test pr 10 (#27)
        } else {
            //Somehow this function is already started
            log.warn("Function {} already running. Going to restart function.",
                    functionRuntimeInfo);
<<<<<<< HEAD
            this.insertStopAction(functionRuntimeInfo);
        }

        this.insertStartAction(functionRuntimeInfo);
    }

    public Map<String, FunctionRuntimeInfo> getFunctionRuntimeInfos() {
        return this.functionRuntimeInfoMap;
=======
            this.conditionallyStopFunction(functionRuntimeInfo);
        }
        this.conditionallyStartFunction(functionRuntimeInfo);
    }

    public Map<String, FunctionRuntimeInfo> getFunctionRuntimeInfos() {
        return this.functionRuntimeInfos.getAll();
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    /**
     * Private methods for internal use.  Should not be used outside of this class
     */
<<<<<<< HEAD

    @VisibleForTesting
    void insertStopAction(FunctionRuntimeInfo functionRuntimeInfo) {
        if (!this.isInitializePhase) {
            FunctionAction functionAction = new FunctionAction();
            functionAction.setAction(FunctionAction.Action.STOP);
            functionAction.setFunctionRuntimeInfo(functionRuntimeInfo);
            try {
                actionQueue.put(functionAction);
            } catch (InterruptedException ex) {
                throw new RuntimeException("Interrupted while putting action");
            }
        }
    }

    @VisibleForTesting
    void insertStartAction(FunctionRuntimeInfo functionRuntimeInfo) {
        if (!this.isInitializePhase) {
            FunctionAction functionAction = new FunctionAction();
            functionAction.setAction(FunctionAction.Action.START);
            functionAction.setFunctionRuntimeInfo(functionRuntimeInfo);
            try {
                actionQueue.put(functionAction);
            } catch (InterruptedException ex) {
                throw new RuntimeException("Interrupted while putting action");
            }
        }
    }

    void insertTerminateAction(FunctionRuntimeInfo functionRuntimeInfo) {
        if (!this.isInitializePhase) {
            FunctionAction functionAction = new FunctionAction();
            functionAction.setAction(FunctionAction.Action.TERMINATE);
            functionAction.setFunctionRuntimeInfo(functionRuntimeInfo);
            try {
                actionQueue.put(functionAction);
            } catch (InterruptedException ex) {
                throw new RuntimeException("Interrupted while putting action");
            }
        }
    }

    private Assignment findAssignment(String tenant, String namespace, String functionName, int instanceId) {
        String fullyQualifiedInstanceId
                = org.apache.pulsar.functions.utils.Utils.getFullyQualifiedInstanceId(tenant, namespace, functionName, instanceId);
=======
    private Assignment findAssignment(String tenant, String namespace, String functionName, int instanceId) {
        String fullyQualifiedInstanceId
                = FunctionCommon.getFullyQualifiedInstanceId(tenant, namespace, functionName, instanceId);
>>>>>>> f773c602c... Test pr 10 (#27)
        for (Map.Entry<String, Map<String, Assignment>> entry : this.workerIdToAssignments.entrySet()) {
            Map<String, Assignment> assignmentMap = entry.getValue();
            Assignment existingAssignment = assignmentMap.get(fullyQualifiedInstanceId);
            if (existingAssignment != null) {
                return existingAssignment;
            }
        }
        return null;
    }

    private Assignment findAssignment(Assignment assignment) {
        return findAssignment(
                assignment.getInstance().getFunctionMetaData().getFunctionDetails().getTenant(),
                assignment.getInstance().getFunctionMetaData().getFunctionDetails().getNamespace(),
                assignment.getInstance().getFunctionMetaData().getFunctionDetails().getName(),
                assignment.getInstance().getInstanceId()
        );
    }

    @VisibleForTesting
    void setAssignment(Assignment assignment) {
        if (!this.workerIdToAssignments.containsKey(assignment.getWorkerId())) {
            this.workerIdToAssignments.put(assignment.getWorkerId(), new HashMap<>());
        }
        this.workerIdToAssignments.get(assignment.getWorkerId()).put(
<<<<<<< HEAD
                org.apache.pulsar.functions.utils.Utils.getFullyQualifiedInstanceId(assignment.getInstance()),
                assignment);
    }

    private void deleteFunctionRuntimeInfo(String fullyQualifiedInstanceId) {
        if (!this.isInitializePhase) {
            this.functionRuntimeInfoMap.remove(fullyQualifiedInstanceId);
        }
    }

    private void setFunctionRuntimeInfo(String fullyQualifiedInstanceId, FunctionRuntimeInfo functionRuntimeInfo) {
        // Don't modify Function Runtime Infos when initializing
        if (!this.isInitializePhase) {
            this.functionRuntimeInfoMap.put(fullyQualifiedInstanceId, functionRuntimeInfo);
        }
    }

    @Override
    public void close() throws Exception {
        this.functionActioner.close();
        this.functionAssignmentTailer.close();
=======
                FunctionCommon.getFullyQualifiedInstanceId(assignment.getInstance()),
                assignment);
    }

    @Override
    public void close() throws Exception {
>>>>>>> f773c602c... Test pr 10 (#27)

        stopAllOwnedFunctions();

        if (runtimeFactory != null) {
            runtimeFactory.close();
        }
    }

    public synchronized FunctionRuntimeInfo getFunctionRuntimeInfo(String fullyQualifiedInstanceId) {
        return _getFunctionRuntimeInfo(fullyQualifiedInstanceId);
    }

    private FunctionRuntimeInfo _getFunctionRuntimeInfo(String fullyQualifiedInstanceId) {
<<<<<<< HEAD
        FunctionRuntimeInfo functionRuntimeInfo = this.functionRuntimeInfoMap.get(fullyQualifiedInstanceId);
=======
        FunctionRuntimeInfo functionRuntimeInfo = this.functionRuntimeInfos.get(fullyQualifiedInstanceId);
>>>>>>> f773c602c... Test pr 10 (#27)

        // sanity check to make sure assignments and runtimeinfo is in sync
        if (functionRuntimeInfo == null) {
            if (this.workerIdToAssignments.containsValue(workerConfig.getWorkerId())
                    && this.workerIdToAssignments.get(workerConfig.getWorkerId()).containsValue(fullyQualifiedInstanceId)) {
                log.error("Assignments and RuntimeInfos are inconsistent. FunctionRuntimeInfo missing for " + fullyQualifiedInstanceId);
            }
        }
        return functionRuntimeInfo;
    }

    private boolean needsStart(Assignment assignment) {
        boolean toStart = false;
        Function.FunctionMetaData functionMetaData = assignment.getInstance().getFunctionMetaData();
        if (functionMetaData.getInstanceStatesMap() == null || functionMetaData.getInstanceStatesMap().isEmpty()) {
            toStart = true;
        } else {
            if (assignment.getInstance().getInstanceId() < 0) {
                // for externally managed functions, insert the start only if there is atleast one
                // instance that needs to be started
                for (Function.FunctionState state : functionMetaData.getInstanceStatesMap().values()) {
                    if (state == Function.FunctionState.RUNNING) {
                        toStart = true;
                    }
                }
            } else {
                if (functionMetaData.getInstanceStatesOrDefault(assignment.getInstance().getInstanceId(), Function.FunctionState.RUNNING) == Function.FunctionState.RUNNING) {
                    toStart = true;
                }
            }
        }
        return toStart;
    }
<<<<<<< HEAD
=======

    private void conditionallyStartFunction(FunctionRuntimeInfo functionRuntimeInfo) {
        if (!this.isInitializePhase) {
            workerStatsManager.startInstanceProcessTimeStart();
            this.functionActioner.startFunction(functionRuntimeInfo);
            workerStatsManager.startInstanceProcessTimeEnd();
        }
    }

    private void conditionallyStopFunction(FunctionRuntimeInfo functionRuntimeInfo) {
        if (!this.isInitializePhase) {
            workerStatsManager.stopInstanceProcessTimeStart();
            this.functionActioner.stopFunction(functionRuntimeInfo);
            workerStatsManager.stopInstanceProcessTimeEnd();
        }
    }

    private void conditionallyTerminateFunction(FunctionRuntimeInfo functionRuntimeInfo) {
        if (!this.isInitializePhase) {
            workerStatsManager.startInstanceProcessTimeStart();
            this.functionActioner.terminateFunction(functionRuntimeInfo);
            workerStatsManager.startInstanceProcessTimeEnd();
        }
    }

    /** Methods for metrics **/

    public int getMyInstances() {
        Map<String, Assignment> myAssignments = workerIdToAssignments.get(workerConfig.getWorkerId());
        return myAssignments == null ? 0 : myAssignments.size();
    }
>>>>>>> f773c602c... Test pr 10 (#27)
}
