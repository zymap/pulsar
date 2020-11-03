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

import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.retryStrategically;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
<<<<<<< HEAD
import static org.testng.Assert.assertFalse;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.lang.reflect.Method;
import java.net.InetAddress;
=======

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.Gson;

import java.lang.reflect.Method;
>>>>>>> f773c602c... Test pr 10 (#27)
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

<<<<<<< HEAD
import com.google.gson.Gson;
import org.apache.bookkeeper.test.PortManager;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
=======
import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.broker.authentication.AuthenticationService;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.broker.loadbalance.impl.SimpleLoadManagerImpl;
import org.apache.pulsar.client.admin.BrokerStats;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
<<<<<<< HEAD
=======
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.Utils;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
<<<<<<< HEAD
import org.apache.pulsar.functions.proto.Function.Assignment;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
=======
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.proto.Function.Assignment;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactory;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactoryConfig;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test Pulsar sink on function
 *
 */
<<<<<<< HEAD
=======
@Slf4j
>>>>>>> f773c602c... Test pr 10 (#27)
public class PulsarWorkerAssignmentTest {
    LocalBookkeeperEnsemble bkEnsemble;

    ServiceConfiguration config;
    WorkerConfig workerConfig;
    PulsarService pulsar;
    PulsarAdmin admin;
    PulsarClient pulsarClient;
    BrokerStats brokerStatsClient;
    WorkerService functionsWorkerService;
    final String tenant = "external-repl-prop";
<<<<<<< HEAD
    String pulsarFunctionsNamespace = tenant + "/use/pulsar-function-admin";
    String primaryHost;
    String workerId;

    private final int ZOOKEEPER_PORT = PortManager.nextFreePort();
    private final int brokerWebServicePort = PortManager.nextFreePort();
    private final int brokerServicePort = PortManager.nextFreePort();
    private final int workerServicePort = PortManager.nextFreePort();

    private static final Logger log = LoggerFactory.getLogger(PulsarWorkerAssignmentTest.class);

    @BeforeMethod
=======
    final String pulsarFunctionsNamespace = tenant + "/pulsar-function-admin";
    String primaryHost;
    String workerId;

    @BeforeMethod(timeOut = 60000)
>>>>>>> f773c602c... Test pr 10 (#27)
    void setup(Method method) throws Exception {

        log.info("--- Setting up method {} ---", method.getName());

        // Start local bookkeeper ensemble
<<<<<<< HEAD
        bkEnsemble = new LocalBookkeeperEnsemble(3, ZOOKEEPER_PORT, () -> PortManager.nextFreePort());
        bkEnsemble.start();

        String brokerServiceUrl = "http://127.0.0.1:" + brokerServicePort;
        String brokerWeServiceUrl = "http://127.0.0.1:" + brokerWebServicePort;

        config = spy(new ServiceConfiguration());
        config.setClusterName("use");
        Set<String> superUsers = Sets.newHashSet("superUser");
        config.setSuperUserRoles(superUsers);
        config.setWebServicePort(brokerWebServicePort);
        config.setZookeeperServers("127.0.0.1" + ":" + ZOOKEEPER_PORT);
        config.setBrokerServicePort(brokerServicePort);
        config.setLoadManagerClassName(SimpleLoadManagerImpl.class.getName());

        functionsWorkerService = createPulsarFunctionWorker(config);
        Optional<WorkerService> functionWorkerService = Optional.of(functionsWorkerService);
        pulsar = new PulsarService(config, functionWorkerService);
        pulsar.start();

        admin = spy(PulsarAdmin.builder().serviceHttpUrl(brokerWeServiceUrl).build());

        brokerStatsClient = admin.brokerStats();
        primaryHost = String.format("http://%s:%d", InetAddress.getLocalHost().getHostName(), brokerWebServicePort);

        // update cluster metadata
        ClusterData clusterData = new ClusterData(brokerServiceUrl);
        admin.clusters().updateCluster(config.getClusterName(), clusterData);

        ClientBuilder clientBuilder = PulsarClient.builder().serviceUrl(this.workerConfig.getPulsarServiceUrl());
        pulsarClient = clientBuilder.build();

        TenantInfo propAdmin = new TenantInfo();
=======
        bkEnsemble = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble.start();

        config = spy(new ServiceConfiguration());
        config.setClusterName("use");
        final Set<String> superUsers = Sets.newHashSet("superUser");
        config.setSuperUserRoles(superUsers);
        config.setWebServicePort(Optional.of(0));
        config.setZookeeperServers("127.0.0.1" + ":" + bkEnsemble.getZookeeperPort());
        config.setBrokerServicePort(Optional.of(0));
        config.setLoadManagerClassName(SimpleLoadManagerImpl.class.getName());
        config.setAdvertisedAddress("localhost");

        functionsWorkerService = createPulsarFunctionWorker(config);
        final Optional<WorkerService> functionWorkerService = Optional.of(functionsWorkerService);
        pulsar = new PulsarService(config, functionWorkerService, (exitCode) -> {});
        pulsar.start();

        admin = spy(PulsarAdmin.builder().serviceHttpUrl(pulsar.getWebServiceAddress()).build());

        brokerStatsClient = admin.brokerStats();
        primaryHost = pulsar.getWebServiceAddress();

        // update cluster metadata
        final ClusterData clusterData = new ClusterData(pulsar.getBrokerServiceUrl());
        admin.clusters().updateCluster(config.getClusterName(), clusterData);

        final ClientBuilder clientBuilder = PulsarClient.builder().serviceUrl(this.workerConfig.getPulsarServiceUrl());
        pulsarClient = clientBuilder.build();

        final TenantInfo propAdmin = new TenantInfo();
>>>>>>> f773c602c... Test pr 10 (#27)
        propAdmin.getAdminRoles().add("superUser");
        propAdmin.setAllowedClusters(Sets.newHashSet(Lists.newArrayList("use")));
        admin.tenants().updateTenant(tenant, propAdmin);

        Thread.sleep(100);
    }

    @AfterMethod
    void shutdown() {
        log.info("--- Shutting down ---");
        try {
            pulsarClient.close();
            admin.close();
            functionsWorkerService.stop();
            pulsar.close();
            bkEnsemble.stop();
        } catch (Exception e) {
            log.warn("Encountered errors at shutting down PulsarWorkerAssignmentTest", e);
        }
    }

    private WorkerService createPulsarFunctionWorker(ServiceConfiguration config) {
        workerConfig = new WorkerConfig();
        workerConfig.setPulsarFunctionsNamespace(pulsarFunctionsNamespace);
        workerConfig.setSchedulerClassName(
                org.apache.pulsar.functions.worker.scheduler.RoundRobinScheduler.class.getName());
<<<<<<< HEAD
        workerConfig.setThreadContainerFactory(new WorkerConfig.ThreadContainerFactory().setThreadGroupName("use"));
=======
        workerConfig.setFunctionRuntimeFactoryClassName(ThreadRuntimeFactory.class.getName());
        workerConfig.setFunctionRuntimeFactoryConfigs(
                ObjectMapperFactory.getThreadLocal().convertValue(new ThreadRuntimeFactoryConfig().setThreadGroupName("use"), Map.class));
>>>>>>> f773c602c... Test pr 10 (#27)
        // worker talks to local broker
        workerConfig.setPulsarServiceUrl("pulsar://127.0.0.1:" + config.getBrokerServicePort().get());
        workerConfig.setPulsarWebServiceUrl("http://127.0.0.1:" + config.getWebServicePort().get());
        workerConfig.setFailureCheckFreqMs(100);
        workerConfig.setNumFunctionPackageReplicas(1);
        workerConfig.setClusterCoordinationTopicName("coordinate");
        workerConfig.setFunctionAssignmentTopicName("assignment");
        workerConfig.setFunctionMetadataTopicName("metadata");
        workerConfig.setInstanceLivenessCheckFreqMs(100);
<<<<<<< HEAD
        workerConfig.setWorkerPort(workerServicePort);
        workerConfig.setPulsarFunctionsCluster(config.getClusterName());
        String hostname = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(config.getAdvertisedAddress());
        this.workerId = "c-" + config.getClusterName() + "-fw-" + hostname + "-" + workerConfig.getWorkerPort();
=======
        workerConfig.setWorkerPort(0);
        workerConfig.setPulsarFunctionsCluster(config.getClusterName());
        final String hostname = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(config.getAdvertisedAddress());
        workerId = "c-" + config.getClusterName() + "-fw-" + hostname + "-" + workerConfig.getWorkerPort();
>>>>>>> f773c602c... Test pr 10 (#27)
        workerConfig.setWorkerHostname(hostname);
        workerConfig.setWorkerId(workerId);
        workerConfig.setTopicCompactionFrequencySec(1);

        return new WorkerService(workerConfig);
    }

<<<<<<< HEAD
    @Test
=======
    @Test(timeOut = 60000, enabled = false)
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testFunctionAssignments() throws Exception {

        final String namespacePortion = "assignment-test";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sinkTopic = "persistent://" + replNamespace + "/my-topic1";
        final String functionName = "assign";
        final String subscriptionName = "test-sub";
        admin.namespaces().createNamespace(replNamespace);
<<<<<<< HEAD
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);

        String jarFilePathUrl = Utils.FILE + ":" + getClass().getClassLoader().getResource("pulsar-functions-api-examples.jar").getFile();
=======
        final Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);

        final String jarFilePathUrl = Utils.FILE + ":" + getClass().getClassLoader().getResource("pulsar-functions-api-examples.jar").getFile();
>>>>>>> f773c602c... Test pr 10 (#27)
        FunctionConfig functionConfig = createFunctionConfig(tenant, namespacePortion,
                functionName, "my.*", sinkTopic, subscriptionName);
        functionConfig.setParallelism(2);

        // (1) Create function with 2 instance
        admin.functions().createFunctionWithUrl(functionConfig, jarFilePathUrl);
        retryStrategically((test) -> {
            try {
                return admin.topics().getStats(sinkTopic).subscriptions.size() == 1
                        && admin.topics().getStats(sinkTopic).subscriptions.values().iterator().next().consumers
                                .size() == 2;
            } catch (PulsarAdminException e) {
                return false;
            }
<<<<<<< HEAD
        }, 5, 150);
=======
        }, 50, 150);
>>>>>>> f773c602c... Test pr 10 (#27)
        // validate 2 instances have been started
        assertEquals(admin.topics().getStats(sinkTopic).subscriptions.size(), 1);
        assertEquals(admin.topics().getStats(sinkTopic).subscriptions.values().iterator().next().consumers.size(), 2);

        // (2) Update function with 1 instance
        functionConfig.setParallelism(1);
        // try to update function to test: update-function functionality
        admin.functions().updateFunctionWithUrl(functionConfig, jarFilePathUrl);
        retryStrategically((test) -> {
            try {
                return admin.topics().getStats(sinkTopic).subscriptions.size() == 1
                        && admin.topics().getStats(sinkTopic).subscriptions.values().iterator().next().consumers
                                .size() == 1;
            } catch (PulsarAdminException e) {
                return false;
            }
<<<<<<< HEAD
        }, 5, 150);
=======
        }, 50, 150);
>>>>>>> f773c602c... Test pr 10 (#27)
        // validate pulsar sink consumer has started on the topic
        log.info("admin.topics().getStats(sinkTopic): {}", new Gson().toJson(admin.topics().getStats(sinkTopic)));
        assertEquals(admin.topics().getStats(sinkTopic).subscriptions.values().iterator().next().consumers.size(), 1);
    }

<<<<<<< HEAD
    @Test(timeOut=20000)
=======
    @Test(timeOut = 60000, enabled = false)
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testFunctionAssignmentsWithRestart() throws Exception {

        final String namespacePortion = "assignment-test";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sinkTopic = "persistent://" + replNamespace + "/my-topic1";
        final String logTopic = "persistent://" + replNamespace + "/log-topic";
        final String baseFunctionName = "assign-restart";
        final String subscriptionName = "test-sub";
        final int totalFunctions = 5;
        final int parallelism = 2;
        admin.namespaces().createNamespace(replNamespace);
<<<<<<< HEAD
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);
        final FunctionRuntimeManager runtimeManager = functionsWorkerService.getFunctionRuntimeManager();

        String jarFilePathUrl = Utils.FILE + ":" + getClass().getClassLoader().getResource("pulsar-functions-api-examples.jar").getFile();
        FunctionConfig functionConfig = null;
=======
        final Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);
        final FunctionRuntimeManager runtimeManager = functionsWorkerService.getFunctionRuntimeManager();

        final String jarFilePathUrl = Utils.FILE + ":" + getClass().getClassLoader().getResource("pulsar-functions-api-examples.jar").getFile();
        FunctionConfig functionConfig;
>>>>>>> f773c602c... Test pr 10 (#27)
        // (1) Register functions with 2 instances
        for (int i = 0; i < totalFunctions; i++) {
            String functionName = baseFunctionName + i;
            functionConfig = createFunctionConfig(tenant, namespacePortion, functionName,
                    "my.*", sinkTopic, subscriptionName);
            functionConfig.setParallelism(parallelism);
            // don't set any log topic
            admin.functions().createFunctionWithUrl(functionConfig, jarFilePathUrl);
        }
        retryStrategically((test) -> {
            try {
                Map<String, Assignment> assgn = runtimeManager.getCurrentAssignments().values().iterator().next();
                return assgn.size() == (totalFunctions * parallelism);
            } catch (Exception e) {
                return false;
            }
<<<<<<< HEAD
        }, 5, 150);
=======
        }, 50, 150);
>>>>>>> f773c602c... Test pr 10 (#27)

        // Validate registered assignments
        Map<String, Assignment> assignments = runtimeManager.getCurrentAssignments().values().iterator().next();
        assertEquals(assignments.size(), (totalFunctions * parallelism));

        // (2) Update function with prop=auto-ack and Delete 2 functions
        for (int i = 0; i < totalFunctions; i++) {
            String functionName = baseFunctionName + i;
            functionConfig = createFunctionConfig(tenant, namespacePortion, functionName,
                    "my.*", sinkTopic, subscriptionName);
            functionConfig.setParallelism(parallelism);
            // Now set the log topic
            functionConfig.setLogTopic(logTopic);
            admin.functions().updateFunctionWithUrl(functionConfig, jarFilePathUrl);
        }

<<<<<<< HEAD
        int totalDeletedFunction = 2;
=======
        final int totalDeletedFunction = 2;
>>>>>>> f773c602c... Test pr 10 (#27)
        for (int i = (totalFunctions - 1); i >= (totalFunctions - totalDeletedFunction); i--) {
            String functionName = baseFunctionName + i;
            admin.functions().deleteFunction(tenant, namespacePortion, functionName);
        }
        retryStrategically((test) -> {
            try {
                Map<String, Assignment> assgn = runtimeManager.getCurrentAssignments().values().iterator().next();
                return assgn.size() == ((totalFunctions - totalDeletedFunction) * parallelism);
            } catch (Exception e) {
                return false;
            }
<<<<<<< HEAD
        }, 5, 150);
=======
        }, 50, 150);
>>>>>>> f773c602c... Test pr 10 (#27)

        // Validate registered assignments
        assignments = runtimeManager.getCurrentAssignments().values().iterator().next();
        assertEquals(assignments.size(), ((totalFunctions - totalDeletedFunction) * parallelism));

        // (3) Restart worker service and check registered functions
<<<<<<< HEAD
        URI dlUri = functionsWorkerService.getDlogUri();
        functionsWorkerService.stop();
        functionsWorkerService = new WorkerService(workerConfig);
        functionsWorkerService.start(dlUri);
        FunctionRuntimeManager runtimeManager2 = functionsWorkerService.getFunctionRuntimeManager();
=======
        final URI dlUri = functionsWorkerService.getDlogUri();
        functionsWorkerService.stop();
        functionsWorkerService = new WorkerService(workerConfig);
        functionsWorkerService.start(dlUri, new AuthenticationService(PulsarConfigurationLoader.convertFrom(workerConfig)), null, ErrorNotifier.getDefaultImpl());
        final FunctionRuntimeManager runtimeManager2 = functionsWorkerService.getFunctionRuntimeManager();
>>>>>>> f773c602c... Test pr 10 (#27)
        retryStrategically((test) -> {
            try {
                Map<String, Assignment> assgn = runtimeManager2.getCurrentAssignments().values().iterator().next();
                return assgn.size() == ((totalFunctions - totalDeletedFunction) * parallelism);
            } catch (Exception e) {
                return false;
            }
<<<<<<< HEAD
        }, 5, 150);
=======
        }, 50, 150);
>>>>>>> f773c602c... Test pr 10 (#27)

        // Validate registered assignments
        assignments = runtimeManager2.getCurrentAssignments().values().iterator().next();
        assertEquals(assignments.size(), ((totalFunctions - totalDeletedFunction) * parallelism));

<<<<<<< HEAD
        // validate updated function prop = auto-ack=false and instnaceid
        for (int i = 0; i < (totalFunctions - totalDeletedFunction); i++) {
            String functionName = baseFunctionName + i;
=======
        // validate updated function prop = auto-ack=false and instance id
        for (int i = 0; i < (totalFunctions - totalDeletedFunction); i++) {
            final String functionName = baseFunctionName + i;
>>>>>>> f773c602c... Test pr 10 (#27)
            assertEquals(admin.functions().getFunction(tenant, namespacePortion, functionName).getLogTopic(), logTopic);
        }
    }

    protected static FunctionConfig createFunctionConfig(String tenant, String namespace,
                                                         String functionName, String sourceTopic, String sinkTopic, String subscriptionName) {

<<<<<<< HEAD
        String sourceTopicPattern = String.format("persistent://%s/%s/%s", tenant, namespace, sourceTopic);

        FunctionConfig functionConfig = new FunctionConfig();
=======
        final String sourceTopicPattern = String.format("persistent://%s/%s/%s", tenant, namespace, sourceTopic);

        final FunctionConfig functionConfig = new FunctionConfig();
>>>>>>> f773c602c... Test pr 10 (#27)
        functionConfig.setTenant(tenant);
        functionConfig.setNamespace(namespace);
        functionConfig.setName(functionName);
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
        functionConfig.setParallelism(1);
        functionConfig.setClassName("org.apache.pulsar.functions.api.examples.ExclamationFunction");

        functionConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
        functionConfig.setTopicsPattern(sourceTopicPattern);
        functionConfig.setSubName(subscriptionName);
        functionConfig.setAutoAck(true);
        functionConfig.setOutput(sinkTopic);

        return functionConfig;
    }

<<<<<<< HEAD
}
=======
}
>>>>>>> f773c602c... Test pr 10 (#27)
