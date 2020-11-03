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

package org.apache.pulsar.functions.instance;

import com.google.common.base.Stopwatch;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.netty.buffer.ByteBuf;
import io.prometheus.client.CollectorRegistry;
<<<<<<< HEAD
import lombok.AccessLevel;
=======

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

>>>>>>> f773c602c... Test pr 10 (#27)
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.jodah.typetools.TypeResolver;
import org.apache.bookkeeper.api.StorageClient;
import org.apache.bookkeeper.api.kv.Table;
import org.apache.bookkeeper.clients.StorageClientBuilder;
<<<<<<< HEAD
=======
import org.apache.bookkeeper.clients.admin.SimpleStorageAdminClientImpl;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.bookkeeper.clients.admin.StorageAdminClient;
import org.apache.bookkeeper.clients.config.StorageClientSettings;
import org.apache.bookkeeper.clients.exceptions.ClientException;
import org.apache.bookkeeper.clients.exceptions.InternalServerException;
import org.apache.bookkeeper.clients.exceptions.NamespaceNotFoundException;
import org.apache.bookkeeper.clients.exceptions.StreamNotFoundException;
<<<<<<< HEAD
=======
import org.apache.bookkeeper.clients.utils.ClientResources;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.bookkeeper.common.util.Backoff.Jitter;
import org.apache.bookkeeper.common.util.Backoff.Jitter.Type;
import org.apache.bookkeeper.stream.proto.NamespaceConfiguration;
import org.apache.bookkeeper.stream.proto.StorageType;
import org.apache.bookkeeper.stream.proto.StreamConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.pulsar.client.api.PulsarClient;
<<<<<<< HEAD
=======
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.functions.ConsumerConfig;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.Record;
<<<<<<< HEAD
import org.apache.pulsar.functions.instance.state.StateContextImpl;
import org.apache.pulsar.functions.instance.stats.ComponentStatsManager;
import org.apache.pulsar.functions.instance.stats.FunctionStatsManager;
=======
import org.apache.pulsar.functions.instance.stats.ComponentStatsManager;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.functions.proto.Function.SinkSpec;
import org.apache.pulsar.functions.proto.Function.SourceSpec;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.proto.InstanceCommunication.MetricsData.Builder;
import org.apache.pulsar.functions.secretsprovider.SecretsProvider;
import org.apache.pulsar.functions.sink.PulsarSink;
import org.apache.pulsar.functions.sink.PulsarSinkConfig;
import org.apache.pulsar.functions.sink.PulsarSinkDisable;
import org.apache.pulsar.functions.source.PulsarSource;
import org.apache.pulsar.functions.source.PulsarSourceConfig;
<<<<<<< HEAD
import org.apache.pulsar.functions.utils.FunctionDetailsUtils;
import org.apache.pulsar.functions.utils.Reflections;
import org.apache.pulsar.functions.utils.StateUtils;
import org.apache.pulsar.functions.utils.Utils;
=======
import org.apache.pulsar.common.util.Reflections;
import org.apache.pulsar.functions.source.batch.BatchSourceExecutor;
import org.apache.pulsar.functions.utils.FunctionCommon;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.functions.utils.functioncache.FunctionCacheManager;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
<<<<<<< HEAD
import java.util.List;
=======
>>>>>>> f773c602c... Test pr 10 (#27)
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.DEFAULT_STREAM_CONF;

/**
 * A function container implemented using java thread.
 */
@Slf4j
public class JavaInstanceRunnable implements AutoCloseable, Runnable {

<<<<<<< HEAD
    // The class loader that used for loading functions
    private ClassLoader fnClassLoader;
=======
>>>>>>> f773c602c... Test pr 10 (#27)
    private final InstanceConfig instanceConfig;
    private final FunctionCacheManager fnCache;
    private final String jarFile;

    // input topic consumer & output topic producer
    private final PulsarClientImpl client;

    private LogAppender logAppender;

    // provide tables for storing states
    private final String stateStorageServiceUrl;
<<<<<<< HEAD
    @Getter(AccessLevel.PACKAGE)
    private StorageClient storageClient;
    @Getter(AccessLevel.PACKAGE)
=======
    private StorageClient storageClient;
>>>>>>> f773c602c... Test pr 10 (#27)
    private Table<ByteBuf, ByteBuf> stateTable;

    private JavaInstance javaInstance;
    @Getter
    private Throwable deathException;

    // function stats
<<<<<<< HEAD
    @Getter
=======
>>>>>>> f773c602c... Test pr 10 (#27)
    private ComponentStatsManager stats;

    private Record<?> currentRecord;

    private Source source;
    private Sink sink;

    private final SecretsProvider secretsProvider;

    private CollectorRegistry collectorRegistry;
    private final String[] metricsLabels;

    private InstanceCache instanceCache;

<<<<<<< HEAD
    private final Utils.ComponentType componentType;

    private final Map<String, String> properties;

=======
    private final org.apache.pulsar.functions.proto.Function.FunctionDetails.ComponentType componentType;

    private final Map<String, String> properties;

    private final ClassLoader instanceClassLoader;
    private ClassLoader functionClassLoader;
    private String narExtractionDirectory;

>>>>>>> f773c602c... Test pr 10 (#27)
    public JavaInstanceRunnable(InstanceConfig instanceConfig,
                                FunctionCacheManager fnCache,
                                String jarFile,
                                PulsarClient pulsarClient,
                                String stateStorageServiceUrl,
                                SecretsProvider secretsProvider,
<<<<<<< HEAD
                                CollectorRegistry collectorRegistry) {
=======
                                CollectorRegistry collectorRegistry,
                                String narExtractionDirectory) {
>>>>>>> f773c602c... Test pr 10 (#27)
        this.instanceConfig = instanceConfig;
        this.fnCache = fnCache;
        this.jarFile = jarFile;
        this.client = (PulsarClientImpl) pulsarClient;
        this.stateStorageServiceUrl = stateStorageServiceUrl;
        this.secretsProvider = secretsProvider;
        this.collectorRegistry = collectorRegistry;
<<<<<<< HEAD
=======
        this.narExtractionDirectory = narExtractionDirectory;
>>>>>>> f773c602c... Test pr 10 (#27)
        this.metricsLabels = new String[]{
                instanceConfig.getFunctionDetails().getTenant(),
                String.format("%s/%s", instanceConfig.getFunctionDetails().getTenant(),
                        instanceConfig.getFunctionDetails().getNamespace()),
                instanceConfig.getFunctionDetails().getName(),
                String.valueOf(instanceConfig.getInstanceId()),
                instanceConfig.getClusterName(),
<<<<<<< HEAD
                FunctionDetailsUtils.getFullyQualifiedName(instanceConfig.getFunctionDetails())
=======
                FunctionCommon.getFullyQualifiedName(instanceConfig.getFunctionDetails())
>>>>>>> f773c602c... Test pr 10 (#27)
        };

        this.componentType = InstanceUtils.calculateSubjectType(instanceConfig.getFunctionDetails());

        this.properties = InstanceUtils.getProperties(this.componentType,
<<<<<<< HEAD
                FunctionDetailsUtils.getFullyQualifiedName(instanceConfig.getFunctionDetails()),
=======
                FunctionCommon.getFullyQualifiedName(instanceConfig.getFunctionDetails()),
>>>>>>> f773c602c... Test pr 10 (#27)
                this.instanceConfig.getInstanceId());

        // Declare function local collector registry so that it will not clash with other function instances'
        // metrics collection especially in threaded mode
        // In process mode the JavaInstanceMain will declare a CollectorRegistry and pass it down
        this.collectorRegistry = collectorRegistry;
<<<<<<< HEAD
=======

        this.instanceClassLoader = Thread.currentThread().getContextClassLoader();
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    /**
     * NOTE: this method should be called in the instance thread, in order to make class loading work.
     */
<<<<<<< HEAD
    JavaInstance setupJavaInstance(ContextImpl contextImpl) throws Exception {
        // initialize the thread context
        ThreadContext.put("function", FunctionDetailsUtils.getFullyQualifiedName(instanceConfig.getFunctionDetails()));
=======
    synchronized private void setup() throws Exception {

        this.instanceCache = InstanceCache.getInstanceCache();

        if (this.collectorRegistry == null) {
            this.collectorRegistry = new CollectorRegistry();
        }
        this.stats = ComponentStatsManager.getStatsManager(this.collectorRegistry, this.metricsLabels,
                this.instanceCache.getScheduledExecutorService(),
                this.componentType);

        // initialize the thread context
        ThreadContext.put("function", FunctionCommon.getFullyQualifiedName(instanceConfig.getFunctionDetails()));
>>>>>>> f773c602c... Test pr 10 (#27)
        ThreadContext.put("functionname", instanceConfig.getFunctionDetails().getName());
        ThreadContext.put("instance", instanceConfig.getInstanceName());

        log.info("Starting Java Instance {} : \n Details = {}",
            instanceConfig.getFunctionDetails().getName(), instanceConfig.getFunctionDetails());

        // start the function thread
<<<<<<< HEAD
        loadJars();

        ClassLoader clsLoader = Thread.currentThread().getContextClassLoader();
        Object object = Reflections.createInstance(
                instanceConfig.getFunctionDetails().getClassName(),
                clsLoader);
=======
        functionClassLoader = loadJars();

        Object object;
        if (instanceConfig.getFunctionDetails().getClassName().equals(org.apache.pulsar.functions.windowing.WindowFunctionExecutor.class.getName())) {
            object = Reflections.createInstance(
                    instanceConfig.getFunctionDetails().getClassName(),
                    instanceClassLoader);
        } else {
            object = Reflections.createInstance(
                    instanceConfig.getFunctionDetails().getClassName(),
                    functionClassLoader);
        }


>>>>>>> f773c602c... Test pr 10 (#27)
        if (!(object instanceof Function) && !(object instanceof java.util.function.Function)) {
            throw new RuntimeException("User class must either be Function or java.util.Function");
        }

        // start the state table
        setupStateTable();
<<<<<<< HEAD
=======

        ContextImpl contextImpl = setupContext();

>>>>>>> f773c602c... Test pr 10 (#27)
        // start the output producer
        setupOutput(contextImpl);
        // start the input consumer
        setupInput(contextImpl);
        // start any log topic handler
        setupLogHandler();

<<<<<<< HEAD
        return new JavaInstance(contextImpl, object);
    }

    ContextImpl setupContext() {
        List<String> inputTopics = null;
        if (source instanceof PulsarSource) {
            inputTopics = ((PulsarSource<?>) source).getInputTopics();
        }
        Logger instanceLog = LoggerFactory.getLogger(
                "function-" + instanceConfig.getFunctionDetails().getName());
        return new ContextImpl(instanceConfig, instanceLog, client, inputTopics, secretsProvider,
                collectorRegistry, metricsLabels, this.componentType);
    }

    /**
     * The core logic that initialize the instance thread and executes the function
=======
        javaInstance = new JavaInstance(contextImpl, object, instanceConfig);
    }

    ContextImpl setupContext() {
        Logger instanceLog = LoggerFactory.getLogger(
                "function-" + instanceConfig.getFunctionDetails().getName());
        return new ContextImpl(instanceConfig, instanceLog, client, secretsProvider,
                collectorRegistry, metricsLabels, this.componentType, this.stats, stateTable);
    }

    /**
     * The core logic that initialize the instance thread and executes the function.
>>>>>>> f773c602c... Test pr 10 (#27)
     */
    @Override
    public void run() {
        try {
<<<<<<< HEAD
            this.instanceCache = InstanceCache.getInstanceCache();

            if (this.collectorRegistry == null) {
                this.collectorRegistry = new CollectorRegistry();
            }
            this.stats = ComponentStatsManager.getStatsManager(this.collectorRegistry, this.metricsLabels,
                    this.instanceCache.getScheduledExecutorService(),
                    this.componentType);

            ContextImpl contextImpl = setupContext();
            javaInstance = setupJavaInstance(contextImpl);
            if (null != stateTable) {
                StateContextImpl stateContext = new StateContextImpl(stateTable);
                javaInstance.getContext().setStateContext(stateContext);
            }
=======
            setup();
            
>>>>>>> f773c602c... Test pr 10 (#27)
            while (true) {
                currentRecord = readInput();

                // increment number of records received from source
                stats.incrTotalReceived();

                if (instanceConfig.getFunctionDetails().getProcessingGuarantees() == org.apache.pulsar.functions
                        .proto.Function.ProcessingGuarantees.ATMOST_ONCE) {
                    if (instanceConfig.getFunctionDetails().getAutoAck()) {
                        currentRecord.ack();
                    }
                }

                addLogTopicHandler();
<<<<<<< HEAD
                JavaExecutionResult result;
=======
                CompletableFuture<JavaExecutionResult> result;
>>>>>>> f773c602c... Test pr 10 (#27)

                // set last invocation time
                stats.setLastInvocation(System.currentTimeMillis());

                // start time for process latency stat
                stats.processTimeStart();

                // process the message
<<<<<<< HEAD
                result = javaInstance.handleMessage(currentRecord, currentRecord.getValue());
=======
                Thread.currentThread().setContextClassLoader(functionClassLoader);
                result = javaInstance.handleMessage(currentRecord, currentRecord.getValue());
                Thread.currentThread().setContextClassLoader(instanceClassLoader);
>>>>>>> f773c602c... Test pr 10 (#27)

                // register end time
                stats.processTimeEnd();

                removeLogTopicHandler();

<<<<<<< HEAD
                if (log.isDebugEnabled()) {
                    log.debug("Got result: {}", result.getResult());
                }

=======
>>>>>>> f773c602c... Test pr 10 (#27)
                try {
                    processResult(currentRecord, result);
                } catch (Exception e) {
                    log.warn("Failed to process result of message {}", currentRecord, e);
                    currentRecord.fail();
                }
            }
        } catch (Throwable t) {
<<<<<<< HEAD
            log.error("[{}] Uncaught exception in Java Instance", Utils.getFullyQualifiedInstanceId(
=======
            log.error("[{}] Uncaught exception in Java Instance", FunctionCommon.getFullyQualifiedInstanceId(
>>>>>>> f773c602c... Test pr 10 (#27)
                    instanceConfig.getFunctionDetails().getTenant(),
                    instanceConfig.getFunctionDetails().getNamespace(),
                    instanceConfig.getFunctionDetails().getName(),
                    instanceConfig.getInstanceId()), t);
            deathException = t;
            if (stats != null) {
                stats.incrSysExceptions(t);
            }
<<<<<<< HEAD
            return;
=======
>>>>>>> f773c602c... Test pr 10 (#27)
        } finally {
            log.info("Closing instance");
            close();
        }
    }

<<<<<<< HEAD
    private void loadJars() throws Exception {
=======
    private ClassLoader loadJars() throws Exception {
        ClassLoader fnClassLoader;
>>>>>>> f773c602c... Test pr 10 (#27)
        try {
            log.info("Load JAR: {}", jarFile);
            // Let's first try to treat it as a nar archive
            fnCache.registerFunctionInstanceWithArchive(
                instanceConfig.getFunctionId(),
                instanceConfig.getInstanceName(),
<<<<<<< HEAD
                jarFile);
=======
                jarFile, narExtractionDirectory);
>>>>>>> f773c602c... Test pr 10 (#27)
        } catch (FileNotFoundException e) {
            // create the function class loader
            fnCache.registerFunctionInstance(
                    instanceConfig.getFunctionId(),
                    instanceConfig.getInstanceName(),
                    Arrays.asList(jarFile),
                    Collections.emptyList());
        }

<<<<<<< HEAD
        log.info("Initialize function class loader for function {} at function cache manager",
                instanceConfig.getFunctionDetails().getName());

        this.fnClassLoader = fnCache.getClassLoader(instanceConfig.getFunctionId());
=======
        log.info("Initialize function class loader for function {} at function cache manager, functionClassLoader: {}",
                instanceConfig.getFunctionDetails().getName(), fnCache.getClassLoader(instanceConfig.getFunctionId()));

        fnClassLoader = fnCache.getClassLoader(instanceConfig.getFunctionId());
>>>>>>> f773c602c... Test pr 10 (#27)
        if (null == fnClassLoader) {
            throw new Exception("No function class loader available.");
        }

<<<<<<< HEAD
        // make sure the function class loader is accessible thread-locally
        Thread.currentThread().setContextClassLoader(fnClassLoader);
    }

    private void createStateTable(String tableNs, String tableName, StorageClientSettings settings) throws Exception {
        try (StorageAdminClient storageAdminClient = StorageClientBuilder.newBuilder()
            .withSettings(settings)
            .buildAdmin()) {
=======
        return fnClassLoader;
    }

    private void createStateTable(String tableNs, String tableName, StorageClientSettings settings) throws Exception {
    	try (StorageAdminClient storageAdminClient = new SimpleStorageAdminClientImpl(
             StorageClientSettings.newBuilder().serviceUri(stateStorageServiceUrl).build(),
             ClientResources.create().scheduler())){
>>>>>>> f773c602c... Test pr 10 (#27)
            StreamConfiguration streamConf = StreamConfiguration.newBuilder(DEFAULT_STREAM_CONF)
                .setInitialNumRanges(4)
                .setMinNumRanges(4)
                .setStorageType(StorageType.TABLE)
                .build();
            Stopwatch elapsedWatch = Stopwatch.createStarted();
            while (elapsedWatch.elapsed(TimeUnit.MINUTES) < 1) {
                try {
                    result(storageAdminClient.getStream(tableNs, tableName));
                    return;
                } catch (NamespaceNotFoundException nnfe) {
                    try {
                        result(storageAdminClient.createNamespace(tableNs, NamespaceConfiguration.newBuilder()
                            .setDefaultStreamConf(streamConf)
                            .build()));
                        result(storageAdminClient.createStream(tableNs, tableName, streamConf));
                    } catch (Exception e) {
                        // there might be two clients conflicting at creating table, so let's retrieve the table again
                        // to make sure the table is created.
                    }
                } catch (StreamNotFoundException snfe) {
                    try {
                        result(storageAdminClient.createStream(tableNs, tableName, streamConf));
                    } catch (Exception e) {
                        // there might be two client conflicting at creating table, so let's retrieve it to make
                        // sure the table is created.
                    }
                } catch (ClientException ce) {
<<<<<<< HEAD
                    log.warn("Encountered issue on fetching state stable metadata, re-attempting in 100 milliseconds",
=======
                    log.warn("Encountered issue {} on fetching state stable metadata, re-attempting in 100 milliseconds",
>>>>>>> f773c602c... Test pr 10 (#27)
                        ce.getMessage());
                    TimeUnit.MILLISECONDS.sleep(100);
                }
            }
        }
    }

    private void setupStateTable() throws Exception {
        if (null == stateStorageServiceUrl) {
            return;
        }

<<<<<<< HEAD
        String tableNs = StateUtils.getStateNamespace(
=======
        String tableNs = FunctionCommon.getStateNamespace(
>>>>>>> f773c602c... Test pr 10 (#27)
            instanceConfig.getFunctionDetails().getTenant(),
            instanceConfig.getFunctionDetails().getNamespace()
        );
        String tableName = instanceConfig.getFunctionDetails().getName();

        StorageClientSettings settings = StorageClientSettings.newBuilder()
                .serviceUri(stateStorageServiceUrl)
                .clientName("function-" + tableNs + "/" + tableName)
                // configure a maximum 2 minutes jitter backoff for accessing table service
                .backoffPolicy(Jitter.of(
                    Type.EXPONENTIAL,
                    100,
                    2000,
                    60
                ))
                .build();

        // we defer creation of the state table until a java instance is running here.
        createStateTable(tableNs, tableName, settings);

        log.info("Starting state table for function {}", instanceConfig.getFunctionDetails().getName());
        this.storageClient = StorageClientBuilder.newBuilder()
                .withSettings(settings)
                .withNamespace(tableNs)
                .build();
        // NOTE: this is a workaround until we bump bk version to 4.9.0
        // table might just be created above, so it might not be ready for serving traffic
        Stopwatch openSw = Stopwatch.createStarted();
        while (openSw.elapsed(TimeUnit.MINUTES) < 1) {
            try {
                this.stateTable = result(storageClient.openTable(tableName));
                break;
            } catch (InternalServerException ise) {
                log.warn("Encountered internal server on opening table '{}', re-attempt in 100 milliseconds : {}",
                    tableName, ise.getMessage());
                TimeUnit.MILLISECONDS.sleep(100);
            }
        }
    }

    private void processResult(Record srcRecord,
<<<<<<< HEAD
                               JavaExecutionResult result) throws Exception {
        if (result.getUserException() != null) {
            log.info("Encountered user exception when processing message {}", srcRecord, result.getUserException());
            stats.incrUserExceptions(result.getUserException());
            srcRecord.fail();
        } else {
            if (result.getResult() != null) {
                sendOutputMessage(srcRecord, result.getResult());
            } else {
                if (instanceConfig.getFunctionDetails().getAutoAck()) {
                    // the function doesn't produce any result or the user doesn't want the result.
                    srcRecord.ack();
                }
            }
            // increment total successfully processed
            stats.incrTotalProcessedSuccessfully();
        }
    }

    private void sendOutputMessage(Record srcRecord, Object output) {
=======
                               CompletableFuture<JavaExecutionResult> result) throws Exception {
        result.whenComplete((result1, throwable) -> {
            if (throwable != null || result1.getUserException() != null) {
                Throwable t = throwable != null ? throwable : result1.getUserException();
                log.warn("Encountered exception when processing message {}",
                        srcRecord, t);
                stats.incrUserExceptions(t);
                srcRecord.fail();
            } else {
                if (result1.getResult() != null) {
                    sendOutputMessage(srcRecord, result1.getResult());
                } else {
                    if (instanceConfig.getFunctionDetails().getAutoAck()) {
                        // the function doesn't produce any result or the user doesn't want the result.
                        srcRecord.ack();
                    }
                }
                // increment total successfully processed
                stats.incrTotalProcessedSuccessfully();
            }
        });
    }

    private void sendOutputMessage(Record srcRecord, Object output) {
        if (!(this.sink instanceof PulsarSink)) {
            Thread.currentThread().setContextClassLoader(functionClassLoader);
        }
>>>>>>> f773c602c... Test pr 10 (#27)
        try {
            this.sink.write(new SinkRecord<>(srcRecord, output));
        } catch (Exception e) {
            log.info("Encountered exception in sink write: ", e);
            stats.incrSinkExceptions(e);
            throw new RuntimeException(e);
<<<<<<< HEAD
=======
        } finally {
            Thread.currentThread().setContextClassLoader(instanceClassLoader);
>>>>>>> f773c602c... Test pr 10 (#27)
        }
    }

    private Record readInput() {
        Record record;
<<<<<<< HEAD
=======
        if (!(this.source instanceof PulsarSource)) {
            Thread.currentThread().setContextClassLoader(functionClassLoader);
        }
>>>>>>> f773c602c... Test pr 10 (#27)
        try {
            record = this.source.read();
        } catch (Exception e) {
            stats.incrSourceExceptions(e);
            log.info("Encountered exception in source read: ", e);
            throw new RuntimeException(e);
<<<<<<< HEAD
=======
        } finally {
            Thread.currentThread().setContextClassLoader(instanceClassLoader);
>>>>>>> f773c602c... Test pr 10 (#27)
        }

        // check record is valid
        if (record == null) {
            throw new IllegalArgumentException("The record returned by the source cannot be null");
        } else if (record.getValue() == null) {
            throw new IllegalArgumentException("The value in the record returned by the source cannot be null");
        }
        return record;
    }

<<<<<<< HEAD
    @Override
    public void close() {

        if (stats != null) {
            stats.close();
        }

        if (source != null) {
            try {
                source.close();
            } catch (Exception e) {
                log.error("Failed to close source {}", instanceConfig.getFunctionDetails().getSource().getClassName(), e);

            }
        }

        if (sink != null) {
            try {
                sink.close();
            } catch (Exception e) {
                log.error("Failed to close sink {}", instanceConfig.getFunctionDetails().getSource().getClassName(), e);
            }
=======
    /**
     * NOTE: this method is be syncrhonized because it is potentially called by two different places
     *       one inside the run/finally clause and one inside the ThreadRuntime::stop
     */
    @Override
    synchronized public void close() {

        if (stats != null) {
            stats.close();
            stats = null;
        }

        if (source != null) {
            if (!(this.source instanceof PulsarSource)) {
                Thread.currentThread().setContextClassLoader(functionClassLoader);
            }
            try {
                source.close();
            } catch (Throwable e) {
                log.error("Failed to close source {}", instanceConfig.getFunctionDetails().getSource().getClassName(), e);
            } finally {
                Thread.currentThread().setContextClassLoader(instanceClassLoader);
            }
            source = null;
        }

        if (sink != null) {
            if (!(this.sink instanceof PulsarSink)) {
                Thread.currentThread().setContextClassLoader(functionClassLoader);
            }
            try {
                sink.close();
            } catch (Throwable e) {
                log.error("Failed to close sink {}", instanceConfig.getFunctionDetails().getSource().getClassName(), e);
            } finally {
                Thread.currentThread().setContextClassLoader(instanceClassLoader);
            }
            sink = null;
>>>>>>> f773c602c... Test pr 10 (#27)
        }

        if (null != javaInstance) {
            javaInstance.close();
<<<<<<< HEAD
=======
            javaInstance = null;
>>>>>>> f773c602c... Test pr 10 (#27)
        }

        // kill the state table
        if (null != stateTable) {
            stateTable.close();
            stateTable = null;
        }
        if (null != storageClient) {
            storageClient.closeAsync()
                .exceptionally(cause -> {
                    log.warn("Failed to close state storage client", cause);
                    return null;
                });
<<<<<<< HEAD
        }

        // once the thread quits, clean up the instance
        fnCache.unregisterFunctionInstance(
                instanceConfig.getFunctionId(),
                instanceConfig.getInstanceName());
        log.info("Unloading JAR files for function {}", instanceConfig);
    }

    public InstanceCommunication.MetricsData getAndResetMetrics() {
        InstanceCommunication.MetricsData metricsData = getMetrics();
        stats.reset();
        return metricsData;
    }

    public InstanceCommunication.MetricsData getMetrics() {
=======
            storageClient = null;
        }

        if (instanceCache != null) {
            // once the thread quits, clean up the instance
            fnCache.unregisterFunctionInstance(
                    instanceConfig.getFunctionId(),
                    instanceConfig.getInstanceName());
            log.info("Unloading JAR files for function {}", instanceConfig);
            instanceCache = null;
        }
    }

    synchronized public String getStatsAsString() throws IOException {
        if (stats != null) {
            return stats.getStatsAsString();
        } else {
            return "";
        }
    }

    // This method is synchronized because it is using the stats variable
    synchronized public InstanceCommunication.MetricsData getAndResetMetrics() {
        InstanceCommunication.MetricsData metricsData = internalGetMetrics();
        internalResetMetrics();
        return metricsData;
    }

    // This method is synchronized because it is using the stats and javaInstance variables
    synchronized public InstanceCommunication.MetricsData getMetrics() {
        return internalGetMetrics();
    }

    // This method is synchronized because it is using the stats and javaInstance variables
    synchronized public void resetMetrics() {
        internalResetMetrics();
    }

    private InstanceCommunication.MetricsData internalGetMetrics() {
>>>>>>> f773c602c... Test pr 10 (#27)
        InstanceCommunication.MetricsData.Builder bldr = createMetricsDataBuilder();
        if (javaInstance != null) {
            Map<String, Double> userMetrics =  javaInstance.getMetrics();
            if (userMetrics != null) {
                bldr.putAllUserMetrics(userMetrics);
            }
        }
        return bldr.build();
    }

<<<<<<< HEAD
    public void resetMetrics() {
        stats.reset();
        javaInstance.resetMetrics();
=======
    private void internalResetMetrics() {
        if (stats != null) {
            stats.reset();
        }
        if (javaInstance != null) {
            javaInstance.resetMetrics();
        }
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    private Builder createMetricsDataBuilder() {
        InstanceCommunication.MetricsData.Builder bldr = InstanceCommunication.MetricsData.newBuilder();
<<<<<<< HEAD

        bldr.setProcessedSuccessfullyTotal((long) stats.getTotalProcessedSuccessfully());
        bldr.setSystemExceptionsTotal((long) stats.getTotalSysExceptions());
        bldr.setUserExceptionsTotal((long) stats.getTotalUserExceptions());
        bldr.setReceivedTotal((long) stats.getTotalRecordsReceived());
        bldr.setAvgProcessLatency(stats.getAvgProcessLatency());
        bldr.setLastInvocation((long) stats.getLastInvocation());

        bldr.setProcessedSuccessfullyTotal1Min((long) stats.getTotalProcessedSuccessfully1min());
        bldr.setSystemExceptionsTotal1Min((long) stats.getTotalSysExceptions1min());
        bldr.setUserExceptionsTotal1Min((long) stats.getTotalUserExceptions1min());
        bldr.setReceivedTotal1Min((long) stats.getTotalRecordsReceived1min());
        bldr.setAvgProcessLatency1Min(stats.getAvgProcessLatency1min());
=======
        if (stats != null) {
            bldr.setProcessedSuccessfullyTotal((long) stats.getTotalProcessedSuccessfully());
            bldr.setSystemExceptionsTotal((long) stats.getTotalSysExceptions());
            bldr.setUserExceptionsTotal((long) stats.getTotalUserExceptions());
            bldr.setReceivedTotal((long) stats.getTotalRecordsReceived());
            bldr.setAvgProcessLatency(stats.getAvgProcessLatency());
            bldr.setLastInvocation((long) stats.getLastInvocation());

            bldr.setProcessedSuccessfullyTotal1Min((long) stats.getTotalProcessedSuccessfully1min());
            bldr.setSystemExceptionsTotal1Min((long) stats.getTotalSysExceptions1min());
            bldr.setUserExceptionsTotal1Min((long) stats.getTotalUserExceptions1min());
            bldr.setReceivedTotal1Min((long) stats.getTotalRecordsReceived1min());
            bldr.setAvgProcessLatency1Min(stats.getAvgProcessLatency1min());
        }
>>>>>>> f773c602c... Test pr 10 (#27)

        return bldr;
    }

<<<<<<< HEAD
    public InstanceCommunication.FunctionStatus.Builder getFunctionStatus() {
        InstanceCommunication.FunctionStatus.Builder functionStatusBuilder = InstanceCommunication.FunctionStatus.newBuilder();
        functionStatusBuilder.setNumReceived((long)stats.getTotalRecordsReceived());
        functionStatusBuilder.setNumSuccessfullyProcessed((long) stats.getTotalProcessedSuccessfully());
        functionStatusBuilder.setNumUserExceptions((long) stats.getTotalUserExceptions());
        stats.getLatestUserExceptions().forEach(ex -> {
            functionStatusBuilder.addLatestUserExceptions(ex);
        });
        functionStatusBuilder.setNumSystemExceptions((long) stats.getTotalSysExceptions());
        stats.getLatestSystemExceptions().forEach(ex -> {
            functionStatusBuilder.addLatestSystemExceptions(ex);
        });
        stats.getLatestSourceExceptions().forEach(ex -> {
            functionStatusBuilder.addLatestSourceExceptions(ex);
        });
        stats.getLatestSinkExceptions().forEach(ex -> {
            functionStatusBuilder.addLatestSinkExceptions(ex);
        });
        functionStatusBuilder.setAverageLatency(stats.getAvgProcessLatency());
        functionStatusBuilder.setLastInvocationTime((long) stats.getLastInvocation());
=======
    // This method is synchronized because it is using the stats variable
    synchronized public InstanceCommunication.FunctionStatus.Builder getFunctionStatus() {
        InstanceCommunication.FunctionStatus.Builder functionStatusBuilder = InstanceCommunication.FunctionStatus.newBuilder();
        if (stats != null) {
            functionStatusBuilder.setNumReceived((long) stats.getTotalRecordsReceived());
            functionStatusBuilder.setNumSuccessfullyProcessed((long) stats.getTotalProcessedSuccessfully());
            functionStatusBuilder.setNumUserExceptions((long) stats.getTotalUserExceptions());
            stats.getLatestUserExceptions().forEach(ex -> {
                functionStatusBuilder.addLatestUserExceptions(ex);
            });
            functionStatusBuilder.setNumSystemExceptions((long) stats.getTotalSysExceptions());
            stats.getLatestSystemExceptions().forEach(ex -> {
                functionStatusBuilder.addLatestSystemExceptions(ex);
            });
            stats.getLatestSourceExceptions().forEach(ex -> {
                functionStatusBuilder.addLatestSourceExceptions(ex);
            });
            stats.getLatestSinkExceptions().forEach(ex -> {
                functionStatusBuilder.addLatestSinkExceptions(ex);
            });
            functionStatusBuilder.setAverageLatency(stats.getAvgProcessLatency());
            functionStatusBuilder.setLastInvocationTime((long) stats.getLastInvocation());
        }
>>>>>>> f773c602c... Test pr 10 (#27)
        return functionStatusBuilder;
    }

    private void setupLogHandler() {
        if (instanceConfig.getFunctionDetails().getLogTopic() != null &&
                !instanceConfig.getFunctionDetails().getLogTopic().isEmpty()) {
            logAppender = new LogAppender(client, instanceConfig.getFunctionDetails().getLogTopic(),
<<<<<<< HEAD
                    FunctionDetailsUtils.getFullyQualifiedName(instanceConfig.getFunctionDetails()));
=======
                    FunctionCommon.getFullyQualifiedName(instanceConfig.getFunctionDetails()));
>>>>>>> f773c602c... Test pr 10 (#27)
            logAppender.start();
        }
    }

    private void addLogTopicHandler() {
        if (logAppender == null) return;
        LoggerContext context = LoggerContext.getContext(false);
        Configuration config = context.getConfiguration();
        config.addAppender(logAppender);
        for (final LoggerConfig loggerConfig : config.getLoggers().values()) {
            loggerConfig.addAppender(logAppender, null, null);
        }
        config.getRootLogger().addAppender(logAppender, null, null);
    }

    private void removeLogTopicHandler() {
        if (logAppender == null) return;
        LoggerContext context = LoggerContext.getContext(false);
        Configuration config = context.getConfiguration();
        for (final LoggerConfig loggerConfig : config.getLoggers().values()) {
            loggerConfig.removeAppender(logAppender.getName());
        }
        config.getRootLogger().removeAppender(logAppender.getName());
    }

<<<<<<< HEAD
    public void setupInput(ContextImpl contextImpl) throws Exception {
=======
    private void setupInput(ContextImpl contextImpl) throws Exception {
>>>>>>> f773c602c... Test pr 10 (#27)

        SourceSpec sourceSpec = this.instanceConfig.getFunctionDetails().getSource();
        Object object;
        // If source classname is not set, we default pulsar source
        if (sourceSpec.getClassName().isEmpty()) {
            PulsarSourceConfig pulsarSourceConfig = new PulsarSourceConfig();
            sourceSpec.getInputSpecsMap().forEach((topic, conf) -> {
                ConsumerConfig consumerConfig = ConsumerConfig.builder().isRegexPattern(conf.getIsRegexPattern()).build();
                if (conf.getSchemaType() != null && !conf.getSchemaType().isEmpty()) {
                    consumerConfig.setSchemaType(conf.getSchemaType());
                } else if (conf.getSerdeClassName() != null && !conf.getSerdeClassName().isEmpty()) {
                    consumerConfig.setSerdeClassName(conf.getSerdeClassName());
                }
<<<<<<< HEAD
=======
                consumerConfig.setSchemaProperties(conf.getSchemaPropertiesMap());
                consumerConfig.setConsumerProperties(conf.getConsumerPropertiesMap());
                if (conf.hasReceiverQueueSize()) {
                    consumerConfig.setReceiverQueueSize(conf.getReceiverQueueSize().getValue());
                }
>>>>>>> f773c602c... Test pr 10 (#27)
                pulsarSourceConfig.getTopicSchema().put(topic, consumerConfig);
            });

            sourceSpec.getTopicsToSerDeClassNameMap().forEach((topic, serde) -> {
                pulsarSourceConfig.getTopicSchema().put(topic,
                        ConsumerConfig.builder()
                                .serdeClassName(serde)
                                .isRegexPattern(false)
                                .build());
            });

            if (!StringUtils.isEmpty(sourceSpec.getTopicsPattern())) {
                pulsarSourceConfig.getTopicSchema().get(sourceSpec.getTopicsPattern()).setRegexPattern(true);
            }

            pulsarSourceConfig.setSubscriptionName(
                    StringUtils.isNotBlank(sourceSpec.getSubscriptionName()) ? sourceSpec.getSubscriptionName()
                            : InstanceUtils.getDefaultSubscriptionName(instanceConfig.getFunctionDetails()));
            pulsarSourceConfig.setProcessingGuarantees(
                    FunctionConfig.ProcessingGuarantees.valueOf(
                            this.instanceConfig.getFunctionDetails().getProcessingGuarantees().name()));

<<<<<<< HEAD
=======
            switch (sourceSpec.getSubscriptionPosition()) {
                case EARLIEST:
                    pulsarSourceConfig.setSubscriptionPosition(SubscriptionInitialPosition.Earliest);
                    break;
                default:
                    pulsarSourceConfig.setSubscriptionPosition(SubscriptionInitialPosition.Latest);
                    break;
            }

>>>>>>> f773c602c... Test pr 10 (#27)
            switch (sourceSpec.getSubscriptionType()) {
                case FAILOVER:
                    pulsarSourceConfig.setSubscriptionType(SubscriptionType.Failover);
                    break;
<<<<<<< HEAD
=======
                case KEY_SHARED:
                    pulsarSourceConfig.setSubscriptionType(SubscriptionType.Key_Shared);
                    break;
>>>>>>> f773c602c... Test pr 10 (#27)
                default:
                    pulsarSourceConfig.setSubscriptionType(SubscriptionType.Shared);
                    break;
            }

            pulsarSourceConfig.setTypeClassName(sourceSpec.getTypeClassName());

            if (sourceSpec.getTimeoutMs() > 0 ) {
                pulsarSourceConfig.setTimeoutMs(sourceSpec.getTimeoutMs());
            }
<<<<<<< HEAD
=======
            if (sourceSpec.getNegativeAckRedeliveryDelayMs() > 0) {
                pulsarSourceConfig.setNegativeAckRedeliveryDelayMs(sourceSpec.getNegativeAckRedeliveryDelayMs());
            }
>>>>>>> f773c602c... Test pr 10 (#27)

            if (this.instanceConfig.getFunctionDetails().hasRetryDetails()) {
                pulsarSourceConfig.setMaxMessageRetries(this.instanceConfig.getFunctionDetails().getRetryDetails().getMaxMessageRetries());
                pulsarSourceConfig.setDeadLetterTopic(this.instanceConfig.getFunctionDetails().getRetryDetails().getDeadLetterTopic());
            }
<<<<<<< HEAD
            object = new PulsarSource(this.client, pulsarSourceConfig, this.properties);
        } else {
            object = Reflections.createInstance(
                    sourceSpec.getClassName(),
                    Thread.currentThread().getContextClassLoader());
=======
            object = new PulsarSource(this.client, pulsarSourceConfig, this.properties, this.functionClassLoader);
        } else {

            // check if source is a batch source
            if (sourceSpec.getClassName().equals(BatchSourceExecutor.class.getName())) {
                object = Reflections.createInstance(
                  sourceSpec.getClassName(),
                  this.instanceClassLoader);
            } else {
                object = Reflections.createInstance(
                  sourceSpec.getClassName(),
                  this.functionClassLoader);
            }
>>>>>>> f773c602c... Test pr 10 (#27)
        }

        Class<?>[] typeArgs;
        if (object instanceof Source) {
            typeArgs = TypeResolver.resolveRawArguments(Source.class, object.getClass());
            assert typeArgs.length > 0;
        } else {
            throw new RuntimeException("Source does not implement correct interface");
        }
        this.source = (Source<?>) object;

<<<<<<< HEAD
        if (sourceSpec.getConfigs().isEmpty()) {
            this.source.open(new HashMap<>(), contextImpl);
        } else {
            this.source.open(new Gson().fromJson(sourceSpec.getConfigs(),
                    new TypeToken<Map<String, Object>>(){}.getType()), contextImpl);
        }
    }

    public void setupOutput(ContextImpl contextImpl) throws Exception {
=======
        if (!(this.source instanceof PulsarSource)) {
            Thread.currentThread().setContextClassLoader(this.functionClassLoader);
        }
        try {
            if (sourceSpec.getConfigs().isEmpty()) {
                this.source.open(new HashMap<>(), contextImpl);
            } else {
                this.source.open(new Gson().fromJson(sourceSpec.getConfigs(),
                        new TypeToken<Map<String, Object>>() {
                        }.getType()), contextImpl);
            }
        } catch (Exception e) {
            log.error("Source open produced uncaught exception: ", e);
            throw e;
        } finally {
            Thread.currentThread().setContextClassLoader(this.instanceClassLoader);
        }
    }

    private void setupOutput(ContextImpl contextImpl) throws Exception {
>>>>>>> f773c602c... Test pr 10 (#27)

        SinkSpec sinkSpec = this.instanceConfig.getFunctionDetails().getSink();
        Object object;
        // If sink classname is not set, we default pulsar sink
        if (sinkSpec.getClassName().isEmpty()) {
            if (StringUtils.isEmpty(sinkSpec.getTopic())) {
                object = PulsarSinkDisable.INSTANCE;
            } else {
                PulsarSinkConfig pulsarSinkConfig = new PulsarSinkConfig();
                pulsarSinkConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.valueOf(
                        this.instanceConfig.getFunctionDetails().getProcessingGuarantees().name()));
                pulsarSinkConfig.setTopic(sinkSpec.getTopic());
<<<<<<< HEAD
=======
                pulsarSinkConfig.setForwardSourceMessageProperty(
                        this.instanceConfig.getFunctionDetails().getSink().getForwardSourceMessageProperty());
>>>>>>> f773c602c... Test pr 10 (#27)

                if (!StringUtils.isEmpty(sinkSpec.getSchemaType())) {
                    pulsarSinkConfig.setSchemaType(sinkSpec.getSchemaType());
                } else if (!StringUtils.isEmpty(sinkSpec.getSerDeClassName())) {
                    pulsarSinkConfig.setSerdeClassName(sinkSpec.getSerDeClassName());
                }

                pulsarSinkConfig.setTypeClassName(sinkSpec.getTypeClassName());
<<<<<<< HEAD

                object = new PulsarSink(this.client, pulsarSinkConfig, this.properties);
=======
                pulsarSinkConfig.setSchemaProperties(sinkSpec.getSchemaPropertiesMap());

                if (this.instanceConfig.getFunctionDetails().getSink().getProducerSpec() != null) {
                    pulsarSinkConfig.setProducerSpec(this.instanceConfig.getFunctionDetails().getSink().getProducerSpec());
                }

                object = new PulsarSink(this.client, pulsarSinkConfig, this.properties, this.stats, this.functionClassLoader);
>>>>>>> f773c602c... Test pr 10 (#27)
            }
        } else {
            object = Reflections.createInstance(
                    sinkSpec.getClassName(),
<<<<<<< HEAD
                    Thread.currentThread().getContextClassLoader());
=======
                    this.functionClassLoader);
>>>>>>> f773c602c... Test pr 10 (#27)
        }

        if (object instanceof Sink) {
            this.sink = (Sink) object;
        } else {
            throw new RuntimeException("Sink does not implement correct interface");
        }
<<<<<<< HEAD
        if (sinkSpec.getConfigs().isEmpty()) {
            this.sink.open(new HashMap<>(), contextImpl);
        } else {
            this.sink.open(new Gson().fromJson(sinkSpec.getConfigs(),
                    new TypeToken<Map<String, Object>>() {}.getType()), contextImpl);
=======

        if (!(this.sink instanceof PulsarSink)) {
            Thread.currentThread().setContextClassLoader(this.functionClassLoader);
        }
        try {
            if (sinkSpec.getConfigs().isEmpty()) {
                this.sink.open(new HashMap<>(), contextImpl);
            } else {
                this.sink.open(new Gson().fromJson(sinkSpec.getConfigs(),
                        new TypeToken<Map<String, Object>>() {
                        }.getType()), contextImpl);
            }
        } catch (Exception e) {
            log.error("Sink open produced uncaught exception: ", e);
            throw e;
        } finally {
            Thread.currentThread().setContextClassLoader(this.instanceClassLoader);
>>>>>>> f773c602c... Test pr 10 (#27)
        }
    }
}
