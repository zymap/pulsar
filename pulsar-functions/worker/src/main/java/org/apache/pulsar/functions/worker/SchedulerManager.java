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
import com.google.common.base.Stopwatch;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

=======
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.CompressionType;
<<<<<<< HEAD
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
=======
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.util.Reflections;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.Assignment;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.Function.FunctionMetaData;
import org.apache.pulsar.functions.proto.Function.Instance;
<<<<<<< HEAD
import org.apache.pulsar.functions.utils.Reflections;
import org.apache.pulsar.functions.utils.Utils;
import org.apache.pulsar.functions.worker.scheduler.IScheduler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SchedulerManager implements AutoCloseable {

    private final WorkerConfig workerConfig;
=======
import org.apache.pulsar.functions.utils.Actions;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.worker.scheduler.IScheduler;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

@Slf4j
/**
 * The scheduler manager is used to compute scheduling of function instances
 * Only the leader computes new schedulings and writes assignments to the assignment topic
 * The lifecyle of this class is the following:
 *  1. When worker becomes leader, this class with me initialized
 *  2. When worker loses leadership, this class will be closed which also closes the worker's producer to the assignments topic
 */
public class SchedulerManager implements AutoCloseable {

    private final WorkerConfig workerConfig;
    private final ErrorNotifier errorNotifier;
    private final WorkerStatsManager workerStatsManager;
    private ThreadPoolExecutor executorService;
    private final PulsarClient pulsarClient;
>>>>>>> f773c602c... Test pr 10 (#27)

    @Setter
    private FunctionMetaDataManager functionMetaDataManager;

    @Setter
<<<<<<< HEAD
=======
    private LeaderService leaderService;

    @Setter
>>>>>>> f773c602c... Test pr 10 (#27)
    private MembershipManager membershipManager;

    @Setter
    private FunctionRuntimeManager functionRuntimeManager;

    private final IScheduler scheduler;

<<<<<<< HEAD
    private final Producer<byte[]> producer;

    private final ScheduledExecutorService executorService;
    
    private final PulsarAdmin admin;
    
=======
    private Producer<byte[]> producer;

    private ScheduledExecutorService scheduledExecutorService;
    
    private final PulsarAdmin admin;

    @Getter
    private Lock schedulerLock = new ReentrantLock(true);

    private volatile boolean isRunning = false;

>>>>>>> f773c602c... Test pr 10 (#27)
    AtomicBoolean isCompactionNeeded = new AtomicBoolean(false);
    private static final long DEFAULT_ADMIN_API_BACKOFF_SEC = 60; 
    public static final String HEARTBEAT_TENANT = "pulsar-function";
    public static final String HEARTBEAT_NAMESPACE = "heartbeat";

<<<<<<< HEAD
    public SchedulerManager(WorkerConfig workerConfig, PulsarClient pulsarClient, PulsarAdmin admin, ScheduledExecutorService executor) {
        this.workerConfig = workerConfig;
        this.admin = admin;
        this.scheduler = Reflections.createInstance(workerConfig.getSchedulerClassName(), IScheduler.class,
                Thread.currentThread().getContextClassLoader());

        this.producer = createProducer(pulsarClient, workerConfig);
        this.executorService = executor;
        
        scheduleCompaction(executor, workerConfig.getTopicCompactionFrequencySec());
    }

    private static Producer<byte[]> createProducer(PulsarClient client, WorkerConfig config) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        for (int i = 0; i < 6; i++) {
            try {
                return client.newProducer().topic(config.getFunctionAssignmentTopic())
                    .enableBatching(false)
                    .blockIfQueueFull(true)
                    .compressionType(CompressionType.LZ4)
                    .sendTimeout(0, TimeUnit.MILLISECONDS)
                    .createAsync().get(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.error("Interrupted at creating producer to topic {}", config.getFunctionAssignmentTopic(), e);
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                log.error("Encountered exceptions at creating producer for topic {}",
                    config.getFunctionAssignmentTopic(), e);
                throw new RuntimeException(e);
            } catch (TimeoutException e) {
                try {
                    log.info("Can't create a producer on assignment topic {} in {} seconds, retry in 10 seconds ...",
                        stopwatch.elapsed(TimeUnit.SECONDS));
                    TimeUnit.SECONDS.sleep(10);
                } catch (InterruptedException e1) {
                    log.error("Interrupted at creating producer to topic {}", config.getFunctionAssignmentTopic(), e);
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                continue;
            }
        }
        throw new RuntimeException("Can't create a producer on assignment topic "
            + config.getFunctionAssignmentTopic() + " in " + stopwatch.elapsed(TimeUnit.SECONDS)
            + " seconds, fail fast ...");
    }

    public Future<?> schedule() {
        return executorService.submit(() -> {
            synchronized (SchedulerManager.this) {
                boolean isLeader = membershipManager.isLeader();
                if (isLeader) {
                    try {
                        invokeScheduler();
                    } catch (Exception e) {
                        log.warn("Failed to invoke scheduler", e);
                        throw e;
                    }
                }
            }
        });
    }

    private void scheduleCompaction(ScheduledExecutorService executor, long scheduleFrequencySec) {
        if (executor != null) {
            executor.scheduleWithFixedDelay(() -> {
                if (membershipManager.isLeader() && isCompactionNeeded.get()) {
                    compactAssignmentTopic();
                    isCompactionNeeded.set(false);
                }
            }, scheduleFrequencySec, scheduleFrequencySec, TimeUnit.SECONDS);
=======
    @Getter
    private MessageId lastMessageProduced = null;

    private MessageId metadataTopicLastMessage = MessageId.earliest;
    private Future<?> currentRebalanceFuture;
    private AtomicBoolean rebalanceInProgess = new AtomicBoolean(false);

    public SchedulerManager(WorkerConfig workerConfig,
                            PulsarClient pulsarClient,
                            PulsarAdmin admin,
                            WorkerStatsManager workerStatsManager,
                            ErrorNotifier errorNotifier) {
        this.workerConfig = workerConfig;
        this.pulsarClient = pulsarClient;
        this.admin = admin;
        this.scheduler = Reflections.createInstance(workerConfig.getSchedulerClassName(), IScheduler.class,
                Thread.currentThread().getContextClassLoader());
        this.workerStatsManager = workerStatsManager;
        this.errorNotifier = errorNotifier;
    }

    private static Producer<byte[]> createProducer(PulsarClient client, WorkerConfig config) {
        Actions.Action createProducerAction = Actions.Action.builder()
                .actionName(String.format("Creating producer for assignment topic %s", config.getFunctionAssignmentTopic()))
                .numRetries(5)
                .sleepBetweenInvocationsMs(10000)
                .supplier(() -> {
                    try {
                        // TODO set producer to be in exclusive mode
                        Producer<byte[]> producer = client.newProducer().topic(config.getFunctionAssignmentTopic())
                                .enableBatching(false)
                                .blockIfQueueFull(true)
                                .compressionType(CompressionType.LZ4)
                                .sendTimeout(0, TimeUnit.MILLISECONDS)
                                .producerName(config.getWorkerId() + "-scheduler-manager")
                                .createAsync().get(10, TimeUnit.SECONDS);
                        return Actions.ActionResult.builder().success(true).result(producer).build();
                    } catch (Exception e) {
                        log.error("Exception while at creating producer to topic {}", config.getFunctionAssignmentTopic(), e);
                        return Actions.ActionResult.builder()
                                .success(false)
                                .build();
                    }
                })
                .build();
        AtomicReference<Producer<byte[]>> producer = new AtomicReference<>();
        try {
            Actions.newBuilder()
                    .addAction(createProducerAction.toBuilder()
                            .onSuccess((actionResult) -> producer.set((Producer<byte[]>) actionResult.getResult()))
                            .build())
                    .run();
        } catch (InterruptedException e) {
            log.error("Interrupted at creating producer to topic {}", config.getFunctionAssignmentTopic(), e);
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        if (producer.get() == null) {
            throw new RuntimeException("Can't create a producer on assignment topic "
                    + config.getFunctionAssignmentTopic());
        }
        return producer.get();
    }

    public synchronized void initialize() {
        if (!isRunning) {
            log.info("Initializing scheduler manager");
            // creates exclusive producer for assignment topic
            producer = createProducer(pulsarClient, workerConfig);

            executorService = new ThreadPoolExecutor(1, 5, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(5));
            executorService.setThreadFactory(new ThreadFactoryBuilder().setNameFormat("worker-scheduler-%d").build());
            scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("worker-assignment-topic-compactor"));
            if (workerConfig.getTopicCompactionFrequencySec() > 0) {
                scheduleCompaction(this.scheduledExecutorService, workerConfig.getTopicCompactionFrequencySec());
            }

            isRunning = true;
            lastMessageProduced = null;
        }
    }

    private Future<?> scheduleInternal(Runnable runnable, String errMsg) {
        if (!leaderService.isLeader()) {
            return CompletableFuture.completedFuture(null);
        }

        try {
            return executorService.submit(() -> {
                try {
                    schedulerLock.lock();

                    boolean isLeader = leaderService.isLeader();
                    if (isLeader) {
                        try {
                            runnable.run();
                        } catch (Throwable th) {
                            log.error("Encountered error when invoking scheduler", errMsg);
                            errorNotifier.triggerError(th);
                        }
                    }
                } finally {
                    schedulerLock.unlock();
                }
            });
        } catch (RejectedExecutionException e) {
            // task queue is full so just ignore
            log.debug("Rejected task to invoke scheduler since task queue is already full");
            return CompletableFuture.completedFuture(null);
        }
    }

    public Future<?> schedule() {
        return scheduleInternal(() -> {
            workerStatsManager.scheduleTotalExecTimeStart();
            invokeScheduler();
            workerStatsManager.scheduleTotalExecTimeEnd();
        }, "Encountered error when invoking scheduler");
    }

    private Future<?> rebalance() {
        return scheduleInternal(() -> {
            workerStatsManager.rebalanceTotalExecTimeStart();
            invokeRebalance();
            workerStatsManager.rebalanceTotalExecTimeEnd();
        }, "Encountered error when invoking rebalance");
    }

    public Future<?> rebalanceIfNotInprogress() {
        if (rebalanceInProgess.compareAndSet(false, true)) {
            currentRebalanceFuture = rebalance();
            return currentRebalanceFuture;
        } else {
            throw new RebalanceInProgressException();
>>>>>>> f773c602c... Test pr 10 (#27)
        }
    }
    
    @VisibleForTesting
<<<<<<< HEAD
    public void invokeScheduler() {
        
        Set<String> currentMembership = this.membershipManager.getCurrentMembership()
                .stream().map(workerInfo -> workerInfo.getWorkerId()).collect(Collectors.toSet());

        List<FunctionMetaData> allFunctions = this.functionMetaDataManager.getAllFunctionMetaData();
        Map<String, Function.Instance> allInstances = computeAllInstances(allFunctions, functionRuntimeManager.getRuntimeFactory().externallyManaged());
        Map<String, Map<String, Assignment>> workerIdToAssignments = this.functionRuntimeManager
                .getCurrentAssignments();

=======
    void invokeScheduler() {
        long startTime = System.nanoTime();

        Set<String> currentMembership = membershipManager.getCurrentMembership()
                .stream().map(workerInfo -> workerInfo.getWorkerId()).collect(Collectors.toSet());

        List<FunctionMetaData> allFunctions = functionMetaDataManager.getAllFunctionMetaData();
        Map<String, Function.Instance> allInstances = computeAllInstances(allFunctions, functionRuntimeManager.getRuntimeFactory().externallyManaged());
        Map<String, Map<String, Assignment>> workerIdToAssignments = functionRuntimeManager
                .getCurrentAssignments();

        // initialize stats collection
        SchedulerStats schedulerStats = new SchedulerStats(workerIdToAssignments, currentMembership);

>>>>>>> f773c602c... Test pr 10 (#27)
        //delete assignments of functions and instances that don't exist anymore
        Iterator<Map.Entry<String, Map<String, Assignment>>> it = workerIdToAssignments.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Map<String, Assignment>> workerIdToAssignmentEntry = it.next();
            Map<String, Assignment> functionMap = workerIdToAssignmentEntry.getValue();

            // remove instances that don't exist anymore
            functionMap.entrySet().removeIf(entry -> {
                String fullyQualifiedInstanceId = entry.getKey();
                boolean deleted = !allInstances.containsKey(fullyQualifiedInstanceId);
                if (deleted) {
<<<<<<< HEAD
                    publishNewAssignment(entry.getValue().toBuilder().build(), true);
=======
                    Assignment assignment = entry.getValue();
                    MessageId messageId = publishNewAssignment(assignment.toBuilder().build(), true);
                    
                    // Directly update in memory assignment cache since I am leader
                    log.info("Deleting assignment: {}", assignment);
                    functionRuntimeManager.deleteAssignment(fullyQualifiedInstanceId);
                    // update message id associated with current view of assignments map
                    lastMessageProduced = messageId;
                    // update stats
                    schedulerStats.removedAssignment(assignment);
>>>>>>> f773c602c... Test pr 10 (#27)
                }
                return deleted;
            });

            // update assignment instances in case attributes of a function gets updated
            for (Map.Entry<String, Assignment> entry : functionMap.entrySet()) {
                String fullyQualifiedInstanceId = entry.getKey();
                Assignment assignment = entry.getValue();
                Function.Instance instance = allInstances.get(fullyQualifiedInstanceId);

                if (!assignment.getInstance().equals(instance)) {
                    functionMap.put(fullyQualifiedInstanceId, assignment.toBuilder().setInstance(instance).build());
<<<<<<< HEAD
                    publishNewAssignment(assignment.toBuilder().setInstance(instance).build().toBuilder().build(), false);
                }
            }
            if (functionMap.isEmpty()) {
                it.remove();
=======
                    Assignment newAssignment = assignment.toBuilder().setInstance(instance).build().toBuilder().build();
                    MessageId messageId = publishNewAssignment(newAssignment, false);

                    // Directly update in memory assignment cache since I am leader
                    log.info("Updating assignment: {}", newAssignment);
                    functionRuntimeManager.processAssignment(newAssignment);
                    // update message id associated with current view of assignments map
                    lastMessageProduced = messageId;
                    //update stats
                    schedulerStats.updatedAssignment(newAssignment);
                }
                if (functionMap.isEmpty()) {
                    it.remove();
                }
>>>>>>> f773c602c... Test pr 10 (#27)
            }
        }

        List<Assignment> currentAssignments = workerIdToAssignments
                .entrySet()
                .stream()
                .filter(workerIdToAssignmentEntry -> {
                    String workerId = workerIdToAssignmentEntry.getKey();
                    // remove assignments to workers that don't exist / died for now.
                    // wait for failure detector to unassign them in the future for re-scheduling
                    if (!currentMembership.contains(workerId)) {
                        return false;
                    }

                    return true;
                })
                .flatMap(stringMapEntry -> stringMapEntry.getValue().values().stream())
                .collect(Collectors.toList());

<<<<<<< HEAD
        Pair<List<Function.Instance>, List<Assignment>> unassignedInstances = this.getUnassignedFunctionInstances(workerIdToAssignments,
                allInstances);

        List<Assignment> assignments = this.scheduler.schedule(unassignedInstances.getLeft(), currentAssignments, currentMembership);
=======
        Pair<List<Function.Instance>, List<Assignment>> unassignedInstances 
                = getUnassignedFunctionInstances(workerIdToAssignments, allInstances);

        workerStatsManager.scheduleStrategyExecTimeStartStart();
        List<Assignment> assignments = scheduler.schedule(unassignedInstances.getLeft(), currentAssignments, currentMembership);
        workerStatsManager.scheduleStrategyExecTimeStartEnd();

>>>>>>> f773c602c... Test pr 10 (#27)
        assignments.addAll(unassignedInstances.getRight());

        if (log.isDebugEnabled()) {
            log.debug("New assignments computed: {}", assignments);
        }

        isCompactionNeeded.set(!assignments.isEmpty());

        for(Assignment assignment : assignments) {
<<<<<<< HEAD
            publishNewAssignment(assignment, false);
        }
        
    }

    public void compactAssignmentTopic() {
=======
            MessageId messageId = publishNewAssignment(assignment, false);

            // Directly update in memory assignment cache since I am leader
            log.info("Adding assignment: {}", assignment);
            functionRuntimeManager.processAssignment(assignment);
            // update message id associated with current view of assignments map
            lastMessageProduced = messageId;
            // update stats
            schedulerStats.newAssignment(assignment);
        }

        log.info("Schedule summary - execution time: {} sec | total unassigned: {} | stats: {}\n{}",
                (System.nanoTime() - startTime) / Math.pow(10, 9),
                unassignedInstances.getLeft().size(), schedulerStats.getSummary(), schedulerStats);
    }

    private void invokeRebalance() {
        long startTime = System.nanoTime();

        Set<String> currentMembership = membershipManager.getCurrentMembership()
                .stream().map(workerInfo -> workerInfo.getWorkerId()).collect(Collectors.toSet());

        Map<String, Map<String, Assignment>> workerIdToAssignments = functionRuntimeManager.getCurrentAssignments();

        // initialize stats collection
        SchedulerStats schedulerStats = new SchedulerStats(workerIdToAssignments, currentMembership);

        // filter out assignments of workers that are not currently in the active membership
        List<Assignment> currentAssignments = workerIdToAssignments
                .entrySet()
                .stream()
                .filter(workerIdToAssignmentEntry -> {
                    String workerId = workerIdToAssignmentEntry.getKey();
                    // remove assignments to workers that don't exist / died for now.
                    // wait for failure detector to unassign them in the future for re-scheduling
                    if (!currentMembership.contains(workerId)) {
                        return false;
                    }

                    return true;
                })
                .flatMap(stringMapEntry -> stringMapEntry.getValue().values().stream())
                .collect(Collectors.toList());

        workerStatsManager.rebalanceStrategyExecTimeStart();
        List<Assignment> rebalancedAssignments = scheduler.rebalance(currentAssignments, currentMembership);
        workerStatsManager.rebalanceStrategyExecTimeEnd();

        for (Assignment assignment : rebalancedAssignments) {
            MessageId messageId = publishNewAssignment(assignment, false);
            // Directly update in memory assignment cache since I am leader
            log.info("Rebalance - new assignment: {}", assignment);
            functionRuntimeManager.processAssignment(assignment);
            // update message id associated with current view of assignments map
            lastMessageProduced = messageId;
            // update stats
            schedulerStats.newAssignment(assignment);
        }

        log.info("Rebalance summary - execution time: {} sec | stats: {}\n{}",
                (System.nanoTime() - startTime) / Math.pow(10, 9), schedulerStats.getSummary(), schedulerStats);

        rebalanceInProgess.set(false);
    }

    private void scheduleCompaction(ScheduledExecutorService executor, long scheduleFrequencySec) {
        if (executor != null) {
            executor.scheduleWithFixedDelay(() -> {
                if (leaderService.isLeader() && isCompactionNeeded.get()) {
                    compactAssignmentTopic();
                    isCompactionNeeded.set(false);
                }
            }, scheduleFrequencySec, scheduleFrequencySec, TimeUnit.SECONDS);

            executor.scheduleWithFixedDelay(() -> {
                if (leaderService.isLeader() && metadataTopicLastMessage.compareTo(functionMetaDataManager.getLastMessageSeen()) != 0) {
                    metadataTopicLastMessage = functionMetaDataManager.getLastMessageSeen();
                    compactFunctionMetadataTopic();
                }
            }, scheduleFrequencySec, scheduleFrequencySec, TimeUnit.SECONDS);
        }
    }

    private void compactAssignmentTopic() {
>>>>>>> f773c602c... Test pr 10 (#27)
        if (this.admin != null) {
            try {
                this.admin.topics().triggerCompaction(workerConfig.getFunctionAssignmentTopic());
            } catch (PulsarAdminException e) {
<<<<<<< HEAD
                log.error("Failed to trigger compaction {}", e);
                executorService.schedule(() -> compactAssignmentTopic(), DEFAULT_ADMIN_API_BACKOFF_SEC,
=======
                log.error("Failed to trigger compaction", e);
                scheduledExecutorService.schedule(() -> compactAssignmentTopic(), DEFAULT_ADMIN_API_BACKOFF_SEC,
>>>>>>> f773c602c... Test pr 10 (#27)
                        TimeUnit.SECONDS);
            }
        }
    }

<<<<<<< HEAD
    private void publishNewAssignment(Assignment assignment, boolean deleted) {
        try {
            String fullyQualifiedInstanceId = org.apache.pulsar.functions.utils.Utils.getFullyQualifiedInstanceId(assignment.getInstance());
            // publish empty message with instance-id key so, compactor can delete and skip delivery of this instance-id
            // message
            producer.newMessage().key(fullyQualifiedInstanceId)
                    .value(deleted ? "".getBytes() : assignment.toByteArray()).sendAsync().get();
=======
    private void compactFunctionMetadataTopic() {
        if (this.admin != null) {
            try {
                this.admin.topics().triggerCompaction(workerConfig.getFunctionMetadataTopic());
            } catch (PulsarAdminException e) {
                log.error("Failed to trigger compaction", e);
                scheduledExecutorService.schedule(() -> compactFunctionMetadataTopic(), DEFAULT_ADMIN_API_BACKOFF_SEC,
                        TimeUnit.SECONDS);
            }
        }
    }

    private MessageId publishNewAssignment(Assignment assignment, boolean deleted) {
        try {
            String fullyQualifiedInstanceId = FunctionCommon.getFullyQualifiedInstanceId(assignment.getInstance());
            // publish empty message with instance-id key so, compactor can delete and skip delivery of this instance-id
            // message
            return producer.newMessage().key(fullyQualifiedInstanceId)
                    .value(deleted ? "".getBytes() : assignment.toByteArray()).send();
>>>>>>> f773c602c... Test pr 10 (#27)
        } catch (Exception e) {
            log.error("Failed to {} assignment update {}", assignment, deleted ? "send" : "deleted", e);
            throw new RuntimeException(e);
        }
    }

<<<<<<< HEAD
    public static Map<String, Function.Instance> computeAllInstances(List<FunctionMetaData> allFunctions,
=======
    private static Map<String, Function.Instance> computeAllInstances(List<FunctionMetaData> allFunctions,
>>>>>>> f773c602c... Test pr 10 (#27)
                                                                     boolean externallyManagedRuntime) {
        Map<String, Function.Instance> functionInstances = new HashMap<>();
        for (FunctionMetaData functionMetaData : allFunctions) {
            for (Function.Instance instance : computeInstances(functionMetaData, externallyManagedRuntime)) {
<<<<<<< HEAD
                functionInstances.put(Utils.getFullyQualifiedInstanceId(instance), instance);
=======
                functionInstances.put(FunctionCommon.getFullyQualifiedInstanceId(instance), instance);
>>>>>>> f773c602c... Test pr 10 (#27)
            }
        }
        return functionInstances;
    }

<<<<<<< HEAD
    public static List<Function.Instance> computeInstances(FunctionMetaData functionMetaData,
=======
    static List<Function.Instance> computeInstances(FunctionMetaData functionMetaData,
>>>>>>> f773c602c... Test pr 10 (#27)
                                                           boolean externallyManagedRuntime) {
        List<Function.Instance> functionInstances = new LinkedList<>();
        if (!externallyManagedRuntime) {
            int instances = functionMetaData.getFunctionDetails().getParallelism();
            for (int i = 0; i < instances; i++) {
                functionInstances.add(Function.Instance.newBuilder()
                        .setFunctionMetaData(functionMetaData)
                        .setInstanceId(i)
                        .build());
            }
        } else {
            functionInstances.add(Function.Instance.newBuilder()
                    .setFunctionMetaData(functionMetaData)
                    .setInstanceId(-1)
                    .build());
        }
        return functionInstances;
    }

    private Pair<List<Function.Instance>, List<Assignment>> getUnassignedFunctionInstances(
            Map<String, Map<String, Assignment>> currentAssignments, Map<String, Function.Instance> functionInstances) {

        List<Function.Instance> unassignedFunctionInstances = new LinkedList<>();
        List<Assignment> heartBeatAssignments = Lists.newArrayList();
        Map<String, Assignment> assignmentMap = new HashMap<>();
        for (Map<String, Assignment> entry : currentAssignments.values()) {
            assignmentMap.putAll(entry);
        }

        for (Map.Entry<String, Function.Instance> instanceEntry : functionInstances.entrySet()) {
            String fullyQualifiedInstanceId = instanceEntry.getKey();
            Function.Instance instance = instanceEntry.getValue();
            String heartBeatWorkerId = checkHeartBeatFunction(instance);
            if (heartBeatWorkerId != null) {
                heartBeatAssignments
                        .add(Assignment.newBuilder().setInstance(instance).setWorkerId(heartBeatWorkerId).build());
                continue;
            }
            if (!assignmentMap.containsKey(fullyQualifiedInstanceId)) {
                unassignedFunctionInstances.add(instance);
            }
        }
        return ImmutablePair.of(unassignedFunctionInstances, heartBeatAssignments);
    }

    @Override
<<<<<<< HEAD
    public void close() {
        try {
            this.producer.close();
        } catch (PulsarClientException e) {
            log.warn("Failed to shutdown scheduler manager assignment producer", e);
        }
    }
    
    public static String checkHeartBeatFunction(Instance funInstance) {
=======
    public synchronized void close() {
        log.info("Closing scheduler manager");
        try {
            // make sure we are not closing while a scheduling is being calculated
            schedulerLock.lock();

            isRunning = false;

            if (scheduledExecutorService != null) {
                scheduledExecutorService.shutdown();
            }

            if (executorService != null) {
                executorService.shutdown();
            }

            if (producer != null) {
                try {
                    producer.close();
                } catch (PulsarClientException e) {
                    log.warn("Failed to shutdown scheduler manager assignment producer", e);
                }
            }
        } finally {
            schedulerLock.unlock();
        }
    }
    
    static String checkHeartBeatFunction(Instance funInstance) {
>>>>>>> f773c602c... Test pr 10 (#27)
        if (funInstance.getFunctionMetaData() != null
                && funInstance.getFunctionMetaData().getFunctionDetails() != null) {
            FunctionDetails funDetails = funInstance.getFunctionMetaData().getFunctionDetails();
            return HEARTBEAT_TENANT.equals(funDetails.getTenant())
                    && HEARTBEAT_NAMESPACE.equals(funDetails.getNamespace()) ? funDetails.getName() : null;
        }
        return null;
    }
<<<<<<< HEAD
=======

    public static class RebalanceInProgressException extends RuntimeException {
    }

    private static class SchedulerStats {

        @Builder
        @Data
        private static class WorkerStats {
            private int originalNumAssignments;
            private int finalNumAssignments;
            private int instancesAdded;
            private int instancesRemoved;
            private int instancesUpdated;
            private boolean alive;
        }

        private Map<String, WorkerStats> workerStatsMap = new HashMap<>();

        private Map<String, String> instanceToWorkerId = new HashMap<>();

        public SchedulerStats(Map<String, Map<String, Assignment>> workerIdToAssignments, Set<String> workers) {

            for(String workerId : workers) {
                WorkerStats.WorkerStatsBuilder workerStats = WorkerStats.builder().alive(true);
                Map<String, Assignment> assignmentMap = workerIdToAssignments.get(workerId);
                if (assignmentMap != null) {
                    workerStats.originalNumAssignments(assignmentMap.size());
                    workerStats.finalNumAssignments(assignmentMap.size());

                    for (String fullyQualifiedInstanceId : assignmentMap.keySet()) {
                        instanceToWorkerId.put(fullyQualifiedInstanceId, workerId);
                    }
                } else {
                    workerStats.originalNumAssignments(0);
                    workerStats.finalNumAssignments(0);
                }

                workerStatsMap.put(workerId, workerStats.build());
            }

            // workers with assignments that are dead
            for (Map.Entry<String, Map<String, Assignment>> entry : workerIdToAssignments.entrySet()) {
                String workerId = entry.getKey();
                Map<String, Assignment> assignmentMap = entry.getValue();
                if (!workers.contains(workerId)) {
                    WorkerStats workerStats = WorkerStats.builder()
                            .alive(false)
                            .originalNumAssignments(assignmentMap.size())
                            .finalNumAssignments(assignmentMap.size())
                            .build();
                    workerStatsMap.put(workerId, workerStats);
                }
            }
        }

        public void removedAssignment(Assignment assignment) {
            String workerId = assignment.getWorkerId();
            WorkerStats stats = workerStatsMap.get(workerId);
            Preconditions.checkNotNull(stats);

            stats.instancesRemoved++;
            stats.finalNumAssignments--;
        }

        public void newAssignment(Assignment assignment) {
            String fullyQualifiedInstanceId = FunctionCommon.getFullyQualifiedInstanceId(assignment.getInstance());
            String newWorkerId = assignment.getWorkerId();
            String oldWorkerId = instanceToWorkerId.get(fullyQualifiedInstanceId);
            if (oldWorkerId != null) {
                WorkerStats oldWorkerStats = workerStatsMap.get(oldWorkerId);
                Preconditions.checkNotNull(oldWorkerStats);

                oldWorkerStats.instancesRemoved++;
                oldWorkerStats.finalNumAssignments--;
            }

            WorkerStats newWorkerStats = workerStatsMap.get(newWorkerId);
            Preconditions.checkNotNull(newWorkerStats);

            newWorkerStats.instancesAdded++;
            newWorkerStats.finalNumAssignments++;
        }

        public void updatedAssignment(Assignment assignment) {
            String workerId = assignment.getWorkerId();
            WorkerStats stats = workerStatsMap.get(workerId);
            Preconditions.checkNotNull(stats);

            stats.instancesUpdated++;
        }

        public String getSummary() {
            int totalAdded = 0;
            int totalUpdated = 0;
            int totalRemoved = 0;

            for (Map.Entry<String, WorkerStats> entry : workerStatsMap.entrySet()) {
                WorkerStats workerStats = entry.getValue();
                totalAdded += workerStats.instancesAdded;
                totalUpdated += workerStats.instancesUpdated;
                totalRemoved += workerStats.instancesRemoved;
            }

            return String.format("{\"Added\": %d, \"Updated\": %d, \"removed\": %d}", totalAdded, totalUpdated, totalRemoved);
        }

        @Override
        public String toString() {
            try {
                return ObjectMapperFactory.getThreadLocal().writerWithDefaultPrettyPrinter().writeValueAsString(workerStatsMap);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
    }
>>>>>>> f773c602c... Test pr 10 (#27)
}
