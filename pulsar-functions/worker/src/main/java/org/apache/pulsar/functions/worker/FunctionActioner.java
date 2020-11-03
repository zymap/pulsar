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

<<<<<<< HEAD
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.pulsar.common.functions.Utils.FILE;
import static org.apache.pulsar.common.functions.Utils.HTTP;
import static org.apache.pulsar.functions.utils.Utils.getSourceType;
import static org.apache.pulsar.functions.utils.Utils.getSinkType;
import static org.apache.pulsar.common.functions.Utils.isFunctionPackageUrlSupported;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

=======
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.commons.lang3.StringUtils;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
<<<<<<< HEAD
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.nar.NarClassLoader;
=======
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.io.BatchSourceConfig;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.auth.FunctionAuthProvider;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.instance.InstanceUtils;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
<<<<<<< HEAD
import org.apache.pulsar.functions.proto.Function.FunctionDetailsOrBuilder;
=======
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.functions.proto.Function.FunctionMetaData;
import org.apache.pulsar.functions.proto.Function.SinkSpec;
import org.apache.pulsar.functions.proto.Function.SourceSpec;
import org.apache.pulsar.functions.runtime.RuntimeFactory;
import org.apache.pulsar.functions.runtime.RuntimeSpawner;
<<<<<<< HEAD
import org.apache.pulsar.functions.utils.FunctionDetailsUtils;
import org.apache.pulsar.functions.utils.io.ConnectorUtils;

@Data
@Setter
@Getter
@EqualsAndHashCode
@ToString
@Slf4j
public class FunctionActioner implements AutoCloseable {
=======
import org.apache.pulsar.functions.utils.Actions;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.utils.SourceConfigUtils;
import org.apache.pulsar.functions.utils.io.ConnectorUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.pulsar.common.functions.Utils.FILE;
import static org.apache.pulsar.common.functions.Utils.HTTP;
import static org.apache.pulsar.common.functions.Utils.isFunctionPackageUrlSupported;
import static org.apache.pulsar.functions.auth.FunctionAuthUtils.getFunctionAuthData;
import static org.apache.pulsar.functions.utils.FunctionCommon.getSinkType;
import static org.apache.pulsar.functions.utils.FunctionCommon.getSourceType;

@Data
@Slf4j
public class FunctionActioner {
>>>>>>> f773c602c... Test pr 10 (#27)

    private final WorkerConfig workerConfig;
    private final RuntimeFactory runtimeFactory;
    private final Namespace dlogNamespace;
<<<<<<< HEAD
    private LinkedBlockingQueue<FunctionAction> actionQueue;
    private volatile boolean running;
    private Thread actioner;
    private final ConnectorsManager connectorsManager;
=======
    private final ConnectorsManager connectorsManager;
    private final FunctionsManager functionsManager;
>>>>>>> f773c602c... Test pr 10 (#27)
    private final PulsarAdmin pulsarAdmin;

    public FunctionActioner(WorkerConfig workerConfig,
                            RuntimeFactory runtimeFactory,
                            Namespace dlogNamespace,
<<<<<<< HEAD
                            LinkedBlockingQueue<FunctionAction> actionQueue,
                            ConnectorsManager connectorsManager, PulsarAdmin pulsarAdmin) {
        this.workerConfig = workerConfig;
        this.runtimeFactory = runtimeFactory;
        this.dlogNamespace = dlogNamespace;
        this.actionQueue = actionQueue;
        this.connectorsManager = connectorsManager;
        this.pulsarAdmin = pulsarAdmin;
        actioner = new Thread(() -> {
            log.info("Starting Actioner Thread...");
            while(running) {
                try {
                    FunctionAction action = actionQueue.poll(1, TimeUnit.SECONDS);
                    processAction(action);
                } catch (InterruptedException ex) {
                }
            }
        });
        actioner.setName("FunctionActionerThread");
    }


    void processAction(FunctionAction action) {
        if (action == null) return;

        switch (action.getAction()) {
            case START:
                try {
                    startFunction(action.getFunctionRuntimeInfo());
                } catch (Exception ex) {
                    FunctionDetails details = action.getFunctionRuntimeInfo().getFunctionInstance()
                            .getFunctionMetaData().getFunctionDetails();
                    log.info("{}/{}/{} Error starting function", details.getTenant(), details.getNamespace(),
                            details.getName(), ex);
                    action.getFunctionRuntimeInfo().setStartupException(ex);
                }
                break;
            case STOP:
                stopFunction(action.getFunctionRuntimeInfo());
                break;
            case TERMINATE:
                terminateFunction(action.getFunctionRuntimeInfo());
                break;
        }
    }

    public void start() {
        this.running = true;
        actioner.start();
    }

    @Override
    public void close() {
        running = false;
    }

    public void join() throws InterruptedException {
        actioner.join();
    }

    @VisibleForTesting
    public void startFunction(FunctionRuntimeInfo functionRuntimeInfo) throws Exception {
        FunctionMetaData functionMetaData = functionRuntimeInfo.getFunctionInstance().getFunctionMetaData();
        FunctionDetails functionDetails = functionMetaData.getFunctionDetails();
        int instanceId = functionRuntimeInfo.getFunctionInstance().getInstanceId();

        log.info("{}/{}/{}-{} Starting function ...", functionDetails.getTenant(), functionDetails.getNamespace(),
                functionDetails.getName(), instanceId);

        String packageFile;

        String pkgLocation = functionMetaData.getPackageLocation().getPackagePath();
        boolean isPkgUrlProvided = isFunctionPackageUrlSupported(pkgLocation);

        if (runtimeFactory.externallyManaged()) {
            packageFile = pkgLocation;
        } else {
            if (isPkgUrlProvided && pkgLocation.startsWith(FILE)) {
                URL url = new URL(pkgLocation);
                File pkgFile = new File(url.toURI());
                packageFile = pkgFile.getAbsolutePath();
            } else if (isFunctionCodeBuiltin(functionDetails)) {
                File pkgFile = getBuiltinArchive(FunctionDetails.newBuilder(functionMetaData.getFunctionDetails()));
                packageFile = pkgFile.getAbsolutePath();
            } else {
                File pkgDir = new File(workerConfig.getDownloadDirectory(),
                        getDownloadPackagePath(functionMetaData, instanceId));
                pkgDir.mkdirs();
                File pkgFile = new File(
                        pkgDir,
                        new File(FunctionDetailsUtils.getDownloadFileName(functionMetaData.getFunctionDetails(), functionMetaData.getPackageLocation())).getName());
                downloadFile(pkgFile, isPkgUrlProvided, functionMetaData, instanceId);
                packageFile = pkgFile.getAbsolutePath();
            }
        }

        RuntimeSpawner runtimeSpawner = getRuntimeSpawner(functionRuntimeInfo.getFunctionInstance(), packageFile);
        functionRuntimeInfo.setRuntimeSpawner(runtimeSpawner);

        runtimeSpawner.start();
=======
                            ConnectorsManager connectorsManager,FunctionsManager functionsManager,PulsarAdmin pulsarAdmin) {
        this.workerConfig = workerConfig;
        this.runtimeFactory = runtimeFactory;
        this.dlogNamespace = dlogNamespace;
        this.connectorsManager = connectorsManager;
        this.functionsManager = functionsManager;
        this.pulsarAdmin = pulsarAdmin;
    }


    public void startFunction(FunctionRuntimeInfo functionRuntimeInfo) {
        try {
            FunctionMetaData functionMetaData = functionRuntimeInfo.getFunctionInstance().getFunctionMetaData();
            FunctionDetails functionDetails = functionMetaData.getFunctionDetails();
            int instanceId = functionRuntimeInfo.getFunctionInstance().getInstanceId();

            log.info("{}/{}/{}-{} Starting function ...", functionDetails.getTenant(), functionDetails.getNamespace(),
                    functionDetails.getName(), instanceId);

            String packageFile;

            String pkgLocation = functionMetaData.getPackageLocation().getPackagePath();
            boolean isPkgUrlProvided = isFunctionPackageUrlSupported(pkgLocation);

            if (runtimeFactory.externallyManaged()) {
                packageFile = pkgLocation;
            } else {
                if (isPkgUrlProvided && pkgLocation.startsWith(FILE)) {
                    URL url = new URL(pkgLocation);
                    File pkgFile = new File(url.toURI());
                    packageFile = pkgFile.getAbsolutePath();
                } else if (WorkerUtils.isFunctionCodeBuiltin(functionDetails)) {
                    File pkgFile = getBuiltinArchive(FunctionDetails.newBuilder(functionMetaData.getFunctionDetails()));
                    packageFile = pkgFile.getAbsolutePath();
                } else {
                    File pkgDir = new File(workerConfig.getDownloadDirectory(),
                            getDownloadPackagePath(functionMetaData, instanceId));
                    pkgDir.mkdirs();
                    File pkgFile = new File(
                            pkgDir,
                            new File(getDownloadFileName(functionMetaData.getFunctionDetails(), functionMetaData.getPackageLocation())).getName());
                    downloadFile(pkgFile, isPkgUrlProvided, functionMetaData, instanceId);
                    packageFile = pkgFile.getAbsolutePath();
                }
            }

            // Setup for batch sources if necessary
            setupBatchSource(functionDetails);

            RuntimeSpawner runtimeSpawner = getRuntimeSpawner(functionRuntimeInfo.getFunctionInstance(), packageFile);
            functionRuntimeInfo.setRuntimeSpawner(runtimeSpawner);

            runtimeSpawner.start();
        } catch (Exception ex) {
            FunctionDetails details = functionRuntimeInfo.getFunctionInstance()
                    .getFunctionMetaData().getFunctionDetails();
            log.info("{}/{}/{} Error starting function", details.getTenant(), details.getNamespace(),
                    details.getName(), ex);
            functionRuntimeInfo.setStartupException(ex);
        }
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    RuntimeSpawner getRuntimeSpawner(Function.Instance instance, String packageFile) {
        FunctionMetaData functionMetaData = instance.getFunctionMetaData();
        int instanceId = instance.getInstanceId();

        FunctionDetails.Builder functionDetailsBuilder = FunctionDetails.newBuilder(functionMetaData.getFunctionDetails());

<<<<<<< HEAD
        InstanceConfig instanceConfig = createInstanceConfig(functionDetailsBuilder.build(),
=======
        // check to make sure functionAuthenticationSpec has any data and authentication is enabled.
        // If not set to null, since for protobuf,
        // even if the field is not set its not going to be null. Have to use the "has" method to check
        Function.FunctionAuthenticationSpec functionAuthenticationSpec = null;
        if (workerConfig.isAuthenticationEnabled() && instance.getFunctionMetaData().hasFunctionAuthSpec()) {
            functionAuthenticationSpec = instance.getFunctionMetaData().getFunctionAuthSpec();
        }

        InstanceConfig instanceConfig = createInstanceConfig(functionDetailsBuilder.build(),
                functionAuthenticationSpec,
>>>>>>> f773c602c... Test pr 10 (#27)
                instanceId, workerConfig.getPulsarFunctionsCluster());

        RuntimeSpawner runtimeSpawner = new RuntimeSpawner(instanceConfig, packageFile,
                functionMetaData.getPackageLocation().getOriginalFileName(),
                runtimeFactory, workerConfig.getInstanceLivenessCheckFreqMs());

        return runtimeSpawner;
    }

<<<<<<< HEAD

    InstanceConfig createInstanceConfig(FunctionDetails functionDetails, int instanceId, String clusterName) {
=======
    InstanceConfig createInstanceConfig(FunctionDetails functionDetails, Function.FunctionAuthenticationSpec
            functionAuthSpec, int instanceId, String clusterName) {
>>>>>>> f773c602c... Test pr 10 (#27)
        InstanceConfig instanceConfig = new InstanceConfig();
        instanceConfig.setFunctionDetails(functionDetails);
        // TODO: set correct function id and version when features implemented
        instanceConfig.setFunctionId(UUID.randomUUID().toString());
        instanceConfig.setFunctionVersion(UUID.randomUUID().toString());
        instanceConfig.setInstanceId(instanceId);
        instanceConfig.setMaxBufferedTuples(1024);
<<<<<<< HEAD
        instanceConfig.setPort(org.apache.pulsar.functions.utils.Utils.findAvailablePort());
        instanceConfig.setClusterName(clusterName);
=======
        instanceConfig.setPort(FunctionCommon.findAvailablePort());
        instanceConfig.setClusterName(clusterName);
        instanceConfig.setFunctionAuthenticationSpec(functionAuthSpec);
        instanceConfig.setMaxPendingAsyncRequests(workerConfig.getMaxPendingAsyncRequests());
>>>>>>> f773c602c... Test pr 10 (#27)
        return instanceConfig;
    }

    private void downloadFile(File pkgFile, boolean isPkgUrlProvided, FunctionMetaData functionMetaData, int instanceId) throws FileNotFoundException, IOException {

        FunctionDetails details = functionMetaData.getFunctionDetails();
        File pkgDir = pkgFile.getParentFile();

        if (pkgFile.exists()) {
            log.warn("Function package exists already {} deleting it",
                    pkgFile);
            pkgFile.delete();
        }

        File tempPkgFile;
        while (true) {
            tempPkgFile = new File(
                    pkgDir,
                    pkgFile.getName() + "." + instanceId + "." + UUID.randomUUID().toString());
            if (!tempPkgFile.exists() && tempPkgFile.createNewFile()) {
                break;
            }
        }
        String pkgLocationPath = functionMetaData.getPackageLocation().getPackagePath();
        boolean downloadFromHttp = isPkgUrlProvided && pkgLocationPath.startsWith(HTTP);
        log.info("{}/{}/{} Function package file {} will be downloaded from {}", tempPkgFile, details.getTenant(),
                details.getNamespace(), details.getName(),
                downloadFromHttp ? pkgLocationPath : functionMetaData.getPackageLocation());

        if(downloadFromHttp) {
<<<<<<< HEAD
            Utils.downloadFromHttpUrl(pkgLocationPath, new FileOutputStream(tempPkgFile));
        } else {
            Utils.downloadFromBookkeeper(
                    dlogNamespace,
                    new FileOutputStream(tempPkgFile),
                    pkgLocationPath);
=======
            FunctionCommon.downloadFromHttpUrl(pkgLocationPath, tempPkgFile);
        } else {
            FileOutputStream tempPkgFos = new FileOutputStream(tempPkgFile);
            WorkerUtils.downloadFromBookkeeper(
                    dlogNamespace,
                    tempPkgFos,
                    pkgLocationPath);
            if (tempPkgFos != null) {
                tempPkgFos.close();
            }
>>>>>>> f773c602c... Test pr 10 (#27)
        }

        try {
            // create a hardlink, if there are two concurrent createLink operations, one will fail.
            // this ensures one instance will successfully download the package.
            try {
                Files.createLink(
                        Paths.get(pkgFile.toURI()),
                        Paths.get(tempPkgFile.toURI()));
                log.info("Function package file is linked from {} to {}",
                        tempPkgFile, pkgFile);
            } catch (FileAlreadyExistsException faee) {
                // file already exists
                log.warn("Function package has been downloaded from {} and saved at {}",
                        functionMetaData.getPackageLocation(), pkgFile);
            }
        } finally {
            tempPkgFile.delete();
        }
<<<<<<< HEAD
    }

    public void stopFunction(FunctionRuntimeInfo functionRuntimeInfo) {
        Function.Instance instance = functionRuntimeInfo.getFunctionInstance();
        FunctionMetaData functionMetaData = instance.getFunctionMetaData();
        FunctionDetails details = functionMetaData.getFunctionDetails();
        log.info("{}/{}/{}-{} Stopping function...", details.getTenant(), details.getNamespace(), details.getName(),
                instance.getInstanceId());
        if (functionRuntimeInfo.getRuntimeSpawner() != null) {
            functionRuntimeInfo.getRuntimeSpawner().close();
            functionRuntimeInfo.setRuntimeSpawner(null);
        }

=======

        if(details.getRuntime() == Function.FunctionDetails.Runtime.GO && !pkgFile.canExecute()) {
            pkgFile.setExecutable(true);
            log.info("Golang function package file {} is set to executable", pkgFile);
        }
    }

    private void cleanupFunctionFiles(FunctionRuntimeInfo functionRuntimeInfo) {
        Function.Instance instance = functionRuntimeInfo.getFunctionInstance();
        FunctionMetaData functionMetaData = instance.getFunctionMetaData();
>>>>>>> f773c602c... Test pr 10 (#27)
        // clean up function package
        File pkgDir = new File(
                workerConfig.getDownloadDirectory(),
                getDownloadPackagePath(functionMetaData, instance.getInstanceId()));

        if (pkgDir.exists()) {
            try {
                MoreFiles.deleteRecursively(
<<<<<<< HEAD
                    Paths.get(pkgDir.toURI()), RecursiveDeleteOption.ALLOW_INSECURE);
            } catch (IOException e) {
                log.warn("Failed to delete package for function: {}",
                        FunctionDetailsUtils.getFullyQualifiedName(functionMetaData.getFunctionDetails()), e);
=======
                        Paths.get(pkgDir.toURI()), RecursiveDeleteOption.ALLOW_INSECURE);
            } catch (IOException e) {
                log.warn("Failed to delete package for function: {}",
                        FunctionCommon.getFullyQualifiedName(functionMetaData.getFunctionDetails()), e);
>>>>>>> f773c602c... Test pr 10 (#27)
            }
        }
    }

<<<<<<< HEAD
    private void terminateFunction(FunctionRuntimeInfo functionRuntimeInfo) {
        FunctionDetails details = functionRuntimeInfo.getFunctionInstance().getFunctionMetaData()
                .getFunctionDetails();
        log.info("{}/{}/{}-{} Terminating function...", details.getTenant(), details.getNamespace(), details.getName(),
                functionRuntimeInfo.getFunctionInstance().getInstanceId());

        stopFunction(functionRuntimeInfo);
=======
    public void stopFunction(FunctionRuntimeInfo functionRuntimeInfo) {
        Function.Instance instance = functionRuntimeInfo.getFunctionInstance();
        FunctionMetaData functionMetaData = instance.getFunctionMetaData();
        FunctionDetails details = functionMetaData.getFunctionDetails();
        log.info("{}/{}/{}-{} Stopping function...", details.getTenant(), details.getNamespace(), details.getName(),
                instance.getInstanceId());
        if (functionRuntimeInfo.getRuntimeSpawner() != null) {
            functionRuntimeInfo.getRuntimeSpawner().close();
            functionRuntimeInfo.setRuntimeSpawner(null);
        }

        cleanupFunctionFiles(functionRuntimeInfo);
    }

    public void terminateFunction(FunctionRuntimeInfo functionRuntimeInfo) {
        FunctionDetails details = functionRuntimeInfo.getFunctionInstance().getFunctionMetaData().getFunctionDetails();
        String fqfn = FunctionCommon.getFullyQualifiedName(details);
        log.info("{}-{} Terminating function...", fqfn,functionRuntimeInfo.getFunctionInstance().getInstanceId());

        if (functionRuntimeInfo.getRuntimeSpawner() != null) {
            functionRuntimeInfo.getRuntimeSpawner().close();

            // cleanup any auth data cached
            if (workerConfig.isAuthenticationEnabled()) {
                functionRuntimeInfo.getRuntimeSpawner()
                        .getRuntimeFactory().getAuthProvider().ifPresent(functionAuthProvider -> {
                            try {
                                log.info("{}-{} Cleaning up authentication data for function...", fqfn,functionRuntimeInfo.getFunctionInstance().getInstanceId());
                                functionAuthProvider
                                        .cleanUpAuthData(
                                                details,
                                                Optional.ofNullable(getFunctionAuthData(
                                                        Optional.ofNullable(
                                                                functionRuntimeInfo.getRuntimeSpawner().getInstanceConfig().getFunctionAuthenticationSpec()))));

                            } catch (Exception e) {
                                log.error("Failed to cleanup auth data for function: {}", fqfn, e);
                            }
                        });
            }
            functionRuntimeInfo.setRuntimeSpawner(null);
        }

        cleanupFunctionFiles(functionRuntimeInfo);

>>>>>>> f773c602c... Test pr 10 (#27)
        //cleanup subscriptions
        if (details.getSource().getCleanupSubscription()) {
            Map<String, Function.ConsumerSpec> consumerSpecMap = details.getSource().getInputSpecsMap();
            consumerSpecMap.entrySet().forEach(new Consumer<Map.Entry<String, Function.ConsumerSpec>>() {
                @Override
                public void accept(Map.Entry<String, Function.ConsumerSpec> stringConsumerSpecEntry) {

                    Function.ConsumerSpec consumerSpec = stringConsumerSpecEntry.getValue();
                    String topic = stringConsumerSpecEntry.getKey();
<<<<<<< HEAD
                    String subscriptionName = functionRuntimeInfo
                            .getFunctionInstance().getFunctionMetaData()
                            .getFunctionDetails().getSource().getSubscriptionName();
                    // if user specified subscription name is empty use default subscription name
                    if (isBlank(subscriptionName)) {
                        subscriptionName =  InstanceUtils.getDefaultSubscriptionName(
                                functionRuntimeInfo.getFunctionInstance()
                                        .getFunctionMetaData().getFunctionDetails());
                    }

                    try {
                        if (consumerSpec.getIsRegexPattern()) {
                            pulsarAdmin.namespaces().unsubscribeNamespace(TopicName.get(topic).getNamespace(), subscriptionName);
                        } else {
                            pulsarAdmin.topics().deleteSubscription(topic, subscriptionName);
                        }
                    } catch (PulsarAdminException e) {
                        log.warn("Failed to cleanup {} subscription for {}", subscriptionName,
                                FunctionDetailsUtils.getFullyQualifiedName(
                                        functionRuntimeInfo.getFunctionInstance()
                                                .getFunctionMetaData().getFunctionDetails()), e);
                    }
                }
            });
        }
=======

                    String subscriptionName = isBlank(functionRuntimeInfo.getFunctionInstance().getFunctionMetaData().getFunctionDetails().getSource().getSubscriptionName())
                            ? InstanceUtils.getDefaultSubscriptionName(functionRuntimeInfo.getFunctionInstance().getFunctionMetaData().getFunctionDetails())
                            : functionRuntimeInfo.getFunctionInstance().getFunctionMetaData().getFunctionDetails().getSource().getSubscriptionName();

                    deleteSubscription(topic, consumerSpec, subscriptionName, String.format("Cleaning up subscriptions for function %s", fqfn));
                }
            });
        }

        // clean up done for batch sources if necessary
        cleanupBatchSource(details);
    }

    private void deleteSubscription(String topic, Function.ConsumerSpec consumerSpec, String subscriptionName, String msg) {
        try {
            Actions.newBuilder()
                    .addAction(
                      Actions.Action.builder()
                        .actionName(msg)
                        .numRetries(10)
                        .sleepBetweenInvocationsMs(1000)
                        .supplier(
                          getDeleteSubscriptionSupplier(topic,
                            consumerSpec.getIsRegexPattern(),
                            subscriptionName)
                        )
                        .build())
                    .run();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private Supplier<Actions.ActionResult> getDeleteSubscriptionSupplier(
      String topic, boolean isRegex, String subscriptionName) {
        return () -> {
            try {
                if (isRegex) {
                    pulsarAdmin.namespaces().unsubscribeNamespace(TopicName
                      .get(topic).getNamespace(), subscriptionName);
                } else {
                    pulsarAdmin.topics().deleteSubscription(topic,
                      subscriptionName);
                }
            } catch (PulsarAdminException e) {
                if (e instanceof PulsarAdminException.NotFoundException) {
                    return Actions.ActionResult.builder()
                      .success(true)
                      .build();
                } else {
                    // for debugging purposes
                    List<Map<String, String>> existingConsumers = Collections.emptyList();
                    SubscriptionStats sub = null;
                    try {
                        TopicStats stats = pulsarAdmin.topics().getStats(topic);
                        sub = stats.subscriptions.get(subscriptionName);
                        if (sub != null) {
                            existingConsumers = sub.consumers.stream()
                              .map(consumerStats -> consumerStats.metadata)
                              .collect(Collectors.toList());
                        }
                    } catch (PulsarAdminException e1) {

                    }

                    String errorMsg = e.getHttpError() != null ? e.getHttpError() : e.getMessage();
                    String finalErrorMsg;
                    if (sub != null) {
                        try {
                            finalErrorMsg = String.format("%s - existing consumers: %s",
                              errorMsg, ObjectMapperFactory.getThreadLocal().writeValueAsString(sub));
                        } catch (JsonProcessingException jsonProcessingException) {
                            finalErrorMsg = errorMsg;
                        }
                    } else {
                        finalErrorMsg = errorMsg;
                    }
                    return Actions.ActionResult.builder()
                      .success(false)
                      .errorMsg(finalErrorMsg)
                      .build();
                }
            }

            return Actions.ActionResult.builder()
              .success(true)
              .build();
        };
    }

    private Supplier<Actions.ActionResult> getDeleteTopicSupplier(String topic) {
        return () -> {
            try {
                pulsarAdmin.topics().delete(topic, true);
            } catch (PulsarAdminException e) {
                if (e instanceof PulsarAdminException.NotFoundException) {
                    return Actions.ActionResult.builder()
                      .success(true)
                      .build();
                } else {
                    // for debugging purposes
                    TopicStats stats = null;
                    try {
                        stats = pulsarAdmin.topics().getStats(topic);
                    } catch (PulsarAdminException e1) {

                    }

                    String errorMsg = e.getHttpError() != null ? e.getHttpError() : e.getMessage();
                    String finalErrorMsg;
                    if (stats != null) {
                        try {
                            finalErrorMsg = String.format("%s - topic stats: %s",
                              errorMsg, ObjectMapperFactory.getThreadLocal().writeValueAsString(stats));
                        } catch (JsonProcessingException jsonProcessingException) {
                            finalErrorMsg = errorMsg;
                        }
                    } else {
                        finalErrorMsg = errorMsg;
                    }

                    return Actions.ActionResult.builder()
                      .success(false)
                      .errorMsg(finalErrorMsg)
                      .build();
                }
            }

            return Actions.ActionResult.builder()
              .success(true)
              .build();
        };
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    private String getDownloadPackagePath(FunctionMetaData functionMetaData, int instanceId) {
        return StringUtils.join(
                new String[]{
                        functionMetaData.getFunctionDetails().getTenant(),
                        functionMetaData.getFunctionDetails().getNamespace(),
                        functionMetaData.getFunctionDetails().getName(),
                        Integer.toString(instanceId),
                },
                File.separatorChar);
    }

<<<<<<< HEAD
    public static boolean isFunctionCodeBuiltin(FunctionDetailsOrBuilder functionDetails) {
        if (functionDetails.hasSource()) {
            SourceSpec sourceSpec = functionDetails.getSource();
            if (!StringUtils.isEmpty(sourceSpec.getBuiltin())) {
                return true;
            }
        }

        if (functionDetails.hasSink()) {
            SinkSpec sinkSpec = functionDetails.getSink();
            if (!StringUtils.isEmpty(sinkSpec.getBuiltin())) {
                return true;
            }
        }

        return false;
    }

    private File getBuiltinArchive(FunctionDetails.Builder functionDetails) throws IOException {
=======
    private File getBuiltinArchive(FunctionDetails.Builder functionDetails) throws IOException, ClassNotFoundException {
>>>>>>> f773c602c... Test pr 10 (#27)
        if (functionDetails.hasSource()) {
            SourceSpec sourceSpec = functionDetails.getSource();
            if (!StringUtils.isEmpty(sourceSpec.getBuiltin())) {
                File archive = connectorsManager.getSourceArchive(sourceSpec.getBuiltin()).toFile();
<<<<<<< HEAD
                String sourceClass = ConnectorUtils.getConnectorDefinition(archive.toString()).getSourceClass();
=======
                String sourceClass = ConnectorUtils.getConnectorDefinition(archive.toString(), workerConfig.getNarExtractionDirectory()).getSourceClass();
>>>>>>> f773c602c... Test pr 10 (#27)
                SourceSpec.Builder builder = SourceSpec.newBuilder(functionDetails.getSource());
                builder.setClassName(sourceClass);
                functionDetails.setSource(builder);

                fillSourceTypeClass(functionDetails, archive, sourceClass);
                return archive;
            }
        }

        if (functionDetails.hasSink()) {
            SinkSpec sinkSpec = functionDetails.getSink();
            if (!StringUtils.isEmpty(sinkSpec.getBuiltin())) {
                File archive = connectorsManager.getSinkArchive(sinkSpec.getBuiltin()).toFile();
<<<<<<< HEAD
                String sinkClass = ConnectorUtils.getConnectorDefinition(archive.toString()).getSinkClass();
=======
                String sinkClass = ConnectorUtils.getConnectorDefinition(archive.toString(), workerConfig.getNarExtractionDirectory()).getSinkClass();
>>>>>>> f773c602c... Test pr 10 (#27)
                SinkSpec.Builder builder = SinkSpec.newBuilder(functionDetails.getSink());
                builder.setClassName(sinkClass);
                functionDetails.setSink(builder);

                fillSinkTypeClass(functionDetails, archive, sinkClass);
                return archive;
            }
        }

<<<<<<< HEAD
=======
        if (!StringUtils.isEmpty(functionDetails.getBuiltin())) {
            return functionsManager.getFunctionArchive(functionDetails.getBuiltin()).toFile();
        }

>>>>>>> f773c602c... Test pr 10 (#27)
        throw new IOException("Could not find built in archive definition");
    }

    private void fillSourceTypeClass(FunctionDetails.Builder functionDetails, File archive, String className)
<<<<<<< HEAD
            throws IOException {
        try (NarClassLoader ncl = NarClassLoader.getFromArchive(archive, Collections.emptySet())) {
=======
            throws IOException, ClassNotFoundException {
        try (NarClassLoader ncl = NarClassLoader.getFromArchive(archive, Collections.emptySet(), workerConfig.getNarExtractionDirectory())) {
>>>>>>> f773c602c... Test pr 10 (#27)
            String typeArg = getSourceType(className, ncl).getName();

            SourceSpec.Builder sourceBuilder = SourceSpec.newBuilder(functionDetails.getSource());
            sourceBuilder.setTypeClassName(typeArg);
            functionDetails.setSource(sourceBuilder);

            SinkSpec sinkSpec = functionDetails.getSink();
            if (null == sinkSpec || StringUtils.isEmpty(sinkSpec.getTypeClassName())) {
                SinkSpec.Builder sinkBuilder = SinkSpec.newBuilder(sinkSpec);
                sinkBuilder.setTypeClassName(typeArg);
                functionDetails.setSink(sinkBuilder);
            }
        }
    }

    private void fillSinkTypeClass(FunctionDetails.Builder functionDetails, File archive, String className)
<<<<<<< HEAD
            throws IOException {
        try (NarClassLoader ncl = NarClassLoader.getFromArchive(archive, Collections.emptySet())) {
=======
            throws IOException, ClassNotFoundException {
        try (NarClassLoader ncl = NarClassLoader.getFromArchive(archive, Collections.emptySet(), workerConfig.getNarExtractionDirectory())) {
>>>>>>> f773c602c... Test pr 10 (#27)
            String typeArg = getSinkType(className, ncl).getName();

            SinkSpec.Builder sinkBuilder = SinkSpec.newBuilder(functionDetails.getSink());
            sinkBuilder.setTypeClassName(typeArg);
            functionDetails.setSink(sinkBuilder);

            SourceSpec sourceSpec = functionDetails.getSource();
            if (null == sourceSpec || StringUtils.isEmpty(sourceSpec.getTypeClassName())) {
                SourceSpec.Builder sourceBuilder = SourceSpec.newBuilder(sourceSpec);
                sourceBuilder.setTypeClassName(typeArg);
                functionDetails.setSource(sourceBuilder);
            }
        }
    }

<<<<<<< HEAD
}
=======
    private static String getDownloadFileName(FunctionDetails FunctionDetails,
                                             Function.PackageLocationMetaData packageLocation) {
        if (!org.apache.commons.lang.StringUtils.isEmpty(packageLocation.getOriginalFileName())) {
            return packageLocation.getOriginalFileName();
        }
        String[] hierarchy = FunctionDetails.getClassName().split("\\.");
        String fileName;
        if (hierarchy.length <= 0) {
            fileName = FunctionDetails.getClassName();
        } else if (hierarchy.length == 1) {
            fileName =  hierarchy[0];
        } else {
            fileName = hierarchy[hierarchy.length - 2];
        }
        switch (FunctionDetails.getRuntime()) {
            case JAVA:
                return fileName + ".jar";
            case PYTHON:
                return fileName + ".py";
            case GO:
                return fileName + ".go";
            default:
                throw new RuntimeException("Unknown runtime " + FunctionDetails.getRuntime());
        }
    }

    private void setupBatchSource(Function.FunctionDetails functionDetails) {
        if (isBatchSource(functionDetails)) {

            String intermediateTopicName = SourceConfigUtils.computeBatchSourceIntermediateTopicName(
              functionDetails.getTenant(), functionDetails.getNamespace(), functionDetails.getName()).toString();

            String intermediateTopicSubscription = SourceConfigUtils.computeBatchSourceInstanceSubscriptionName(
              functionDetails.getTenant(), functionDetails.getNamespace(), functionDetails.getName());
            String fqfn = FunctionCommon.getFullyQualifiedName(
              functionDetails.getTenant(), functionDetails.getNamespace(), functionDetails.getName());
            try {
                Actions.newBuilder()
                  .addAction(
                    Actions.Action.builder()
                      .actionName(String.format("Creating intermediate topic %s with subscription %s for Batch Source %s",
                        intermediateTopicName, intermediateTopicSubscription, fqfn))
                      .numRetries(10)
                      .sleepBetweenInvocationsMs(1000)
                      .supplier(() -> {
                          try {
                              pulsarAdmin.topics().createSubscription(intermediateTopicName, intermediateTopicSubscription, MessageId.latest);
                              return Actions.ActionResult.builder()
                                .success(true)
                                .build();
                          } catch (PulsarAdminException.ConflictException e) {
                              // topic and subscription already exists so just continue
                              return Actions.ActionResult.builder()
                                .success(true)
                                .build();
                          } catch (Exception e) {
                              return Actions.ActionResult.builder()
                                .errorMsg(e.getMessage())
                                .success(false)
                                .build();
                          }
                      })
                      .build())
                  .run();
            } catch (InterruptedException e) {
                log.error("Error setting up instance subscription for intermediate topic", e);
                throw new RuntimeException(e);
            }
        }
    }

    private void cleanupBatchSource(Function.FunctionDetails functionDetails) {
        if (isBatchSource(functionDetails)) {
            // clean up intermediate topic
            String intermediateTopicName = SourceConfigUtils.computeBatchSourceIntermediateTopicName(functionDetails.getTenant(),
              functionDetails.getNamespace(), functionDetails.getName()).toString();
            String intermediateTopicSubscription = SourceConfigUtils.computeBatchSourceInstanceSubscriptionName(
              functionDetails.getTenant(), functionDetails.getNamespace(), functionDetails.getName());
            String fqfn = FunctionCommon.getFullyQualifiedName(functionDetails);
            try {
                Actions.newBuilder()
                  .addAction(
                    // Unsubscribe and allow time for consumers to close
                    Actions.Action.builder()
                      .actionName(String.format("Removing intermediate topic subscription %s for Batch Source %s",
                        intermediateTopicSubscription, fqfn))
                      .numRetries(10)
                      .sleepBetweenInvocationsMs(1000)
                      .supplier(
                        getDeleteSubscriptionSupplier(intermediateTopicName,
                          false,
                          intermediateTopicSubscription)
                      )
                      .build())
                  .addAction(
                    // Delete topic forcibly regardless whether unsubscribe succeeded or not
                    Actions.Action.builder()
                    .actionName(String.format("Deleting intermediate topic %s for Batch Source %s",
                      intermediateTopicName, fqfn))
                    .numRetries(10)
                    .sleepBetweenInvocationsMs(1000)
                    .supplier(getDeleteTopicSupplier(intermediateTopicName))
                    .build())
                  .run();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static boolean isBatchSource(Function.FunctionDetails functionDetails) {
        if (InstanceUtils.calculateSubjectType(functionDetails) == FunctionDetails.ComponentType.SOURCE) {
            String fqfn = FunctionCommon.getFullyQualifiedName(functionDetails);
            Map<String, Object> configMap = SourceConfigUtils.extractSourceConfig(functionDetails.getSource(), fqfn);
            if (configMap != null) {
                BatchSourceConfig batchSourceConfig = SourceConfigUtils.extractBatchSourceConfig(configMap);
                if (batchSourceConfig != null) {
                    return true;
                }
            }
        }
        return false;
    }
}
>>>>>>> f773c602c... Test pr 10 (#27)
