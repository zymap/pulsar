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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
=======
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.argThat;
>>>>>>> f773c602c... Test pr 10 (#27)
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
<<<<<<< HEAD
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
=======
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
>>>>>>> f773c602c... Test pr 10 (#27)

import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ConsumerEventListener;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.functions.WorkerInfo;
<<<<<<< HEAD
import org.apache.pulsar.functions.proto.Function;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.testng.Assert;
=======
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactoryConfig;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactory;
import org.mockito.Mockito;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.testng.annotations.Test;

public class MembershipManagerTest {

    private final WorkerConfig workerConfig;

    public MembershipManagerTest() {
        this.workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
<<<<<<< HEAD
        workerConfig.setThreadContainerFactory(new WorkerConfig.ThreadContainerFactory().setThreadGroupName("test"));
=======
        workerConfig.setFunctionRuntimeFactoryClassName(ThreadRuntimeFactory.class.getName());
        workerConfig.setFunctionRuntimeFactoryConfigs(
                ObjectMapperFactory.getThreadLocal().convertValue(
                        new ThreadRuntimeFactoryConfig().setThreadGroupName("test"), Map.class));
>>>>>>> f773c602c... Test pr 10 (#27)
        workerConfig.setPulsarServiceUrl("pulsar://localhost:6650");
        workerConfig.setStateStorageServiceUrl("foo");
    }

<<<<<<< HEAD
    @Test
    public void testConsumerEventListener() throws Exception {
        PulsarClientImpl mockClient = mock(PulsarClientImpl.class);

        ConsumerImpl<byte[]> mockConsumer = mock(ConsumerImpl.class);
        ConsumerBuilder<byte[]> mockConsumerBuilder = mock(ConsumerBuilder.class);

        when(mockConsumerBuilder.topic(anyString())).thenReturn(mockConsumerBuilder);
        when(mockConsumerBuilder.subscriptionName(anyString())).thenReturn(mockConsumerBuilder);
        when(mockConsumerBuilder.subscriptionType(any(SubscriptionType.class))).thenReturn(mockConsumerBuilder);
        when(mockConsumerBuilder.property(anyString(), anyString())).thenReturn(mockConsumerBuilder);

        when(mockConsumerBuilder.subscribe()).thenReturn(mockConsumer);
        WorkerService workerService = mock(WorkerService.class);
        doReturn(workerConfig).when(workerService).getWorkerConfig();

        AtomicReference<ConsumerEventListener> listenerHolder = new AtomicReference<>();
        when(mockConsumerBuilder.consumerEventListener(any(ConsumerEventListener.class))).thenAnswer(invocationOnMock -> {

            ConsumerEventListener listener = invocationOnMock.getArgumentAt(0, ConsumerEventListener.class);
            listenerHolder.set(listener);

            return mockConsumerBuilder;
        });

        when(mockClient.newConsumer()).thenReturn(mockConsumerBuilder);

        MembershipManager membershipManager = spy(new MembershipManager(workerService, mockClient));
        assertFalse(membershipManager.isLeader());
        verify(mockClient, times(1))
            .newConsumer();

        listenerHolder.get().becameActive(mockConsumer, 0);
        assertTrue(membershipManager.isLeader());

        listenerHolder.get().becameInactive(mockConsumer, 0);
        assertFalse(membershipManager.isLeader());
    }

=======
>>>>>>> f773c602c... Test pr 10 (#27)
    private static PulsarClient mockPulsarClient() throws PulsarClientException {
        PulsarClientImpl mockClient = mock(PulsarClientImpl.class);

        ConsumerImpl<byte[]> mockConsumer = mock(ConsumerImpl.class);
        ConsumerBuilder<byte[]> mockConsumerBuilder = mock(ConsumerBuilder.class);

        when(mockConsumerBuilder.topic(anyString())).thenReturn(mockConsumerBuilder);
        when(mockConsumerBuilder.subscriptionName(anyString())).thenReturn(mockConsumerBuilder);
        when(mockConsumerBuilder.subscriptionType(any(SubscriptionType.class))).thenReturn(mockConsumerBuilder);
        when(mockConsumerBuilder.property(anyString(), anyString())).thenReturn(mockConsumerBuilder);

        when(mockConsumerBuilder.subscribe()).thenReturn(mockConsumer);

        when(mockConsumerBuilder.consumerEventListener(any(ConsumerEventListener.class))).thenReturn(mockConsumerBuilder);

        when(mockClient.newConsumer()).thenReturn(mockConsumerBuilder);

        return mockClient;
    }

    @Test
    public void testCheckFailuresNoFailures() throws Exception {
        SchedulerManager schedulerManager = mock(SchedulerManager.class);
        PulsarClient pulsarClient = mockPulsarClient();
        ReaderBuilder<byte[]> readerBuilder = mock(ReaderBuilder.class);
        doReturn(readerBuilder).when(pulsarClient).newReader();
        doReturn(readerBuilder).when(readerBuilder).topic(anyString());
        doReturn(readerBuilder).when(readerBuilder).readCompacted(true);
        doReturn(readerBuilder).when(readerBuilder).startMessageId(any());
        doReturn(mock(Reader.class)).when(readerBuilder).create();
        WorkerService workerService = mock(WorkerService.class);
        doReturn(pulsarClient).when(workerService).getClient();
        doReturn(workerConfig).when(workerService).getWorkerConfig();
<<<<<<< HEAD
        doReturn(mock(PulsarAdmin.class)).when(workerService).getFunctionAdmin();
=======
        PulsarAdmin pulsarAdmin = mock(PulsarAdmin.class);
        doReturn(pulsarAdmin).when(workerService).getFunctionAdmin();
>>>>>>> f773c602c... Test pr 10 (#27)

        FunctionMetaDataManager functionMetaDataManager = mock(FunctionMetaDataManager.class);
        FunctionRuntimeManager functionRuntimeManager = spy(new FunctionRuntimeManager(
                workerConfig,
                workerService,
                mock(Namespace.class),
                mock(MembershipManager.class),
                mock(ConnectorsManager.class),
<<<<<<< HEAD
                functionMetaDataManager));
        MembershipManager membershipManager = spy(new MembershipManager(workerService, pulsarClient));
=======
                mock(FunctionsManager.class),
                functionMetaDataManager,
                mock(WorkerStatsManager.class),
                mock(ErrorNotifier.class)));
        MembershipManager membershipManager = spy(new MembershipManager(workerService, pulsarClient, pulsarAdmin));
>>>>>>> f773c602c... Test pr 10 (#27)

        List<WorkerInfo> workerInfoList = new LinkedList<>();
        workerInfoList.add(WorkerInfo.of("worker-1", "host-1", 8000));
        workerInfoList.add(WorkerInfo.of("worker-2", "host-2", 8001));

        Mockito.doReturn(workerInfoList).when(membershipManager).getCurrentMembership();

        Function.FunctionMetaData function1 = Function.FunctionMetaData.newBuilder().setFunctionDetails(
                Function.FunctionDetails.newBuilder()
                        .setTenant("test-tenant").setNamespace("test-namespace").setName("func-1")).build();

        Function.FunctionMetaData function2 = Function.FunctionMetaData.newBuilder().setFunctionDetails(
                Function.FunctionDetails.newBuilder()
                        .setTenant("test-tenant").setNamespace("test-namespace").setName("func-2")).build();

        List<Function.FunctionMetaData> metaDataList = new LinkedList<>();
        metaDataList.add(function1);
        metaDataList.add(function2);

        Mockito.doReturn(metaDataList).when(functionMetaDataManager).getAllFunctionMetaData();
        Function.Assignment assignment1 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function1)
                        .setInstanceId(0)
                        .build())
                .build();
        Function.Assignment assignment2 = Function.Assignment.newBuilder()
                .setWorkerId("worker-2")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function2)
                        .setInstanceId(0)
                        .build())
                .build();

        // add existing assignments
        functionRuntimeManager.setAssignment(assignment1);
        functionRuntimeManager.setAssignment(assignment2);

        membershipManager.checkFailures(functionMetaDataManager, functionRuntimeManager, schedulerManager);

        verify(schedulerManager, times(0)).schedule();
        verify(functionRuntimeManager, times(0)).removeAssignments(any());
<<<<<<< HEAD
        Assert.assertEquals(membershipManager.unsignedFunctionDurations.size(), 0);
=======
        assertEquals(membershipManager.unsignedFunctionDurations.size(), 0);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Test
    public void testCheckFailuresSomeFailures() throws Exception {
        workerConfig.setRescheduleTimeoutMs(30000);
        SchedulerManager schedulerManager = mock(SchedulerManager.class);
        PulsarClient pulsarClient = mockPulsarClient();
        ReaderBuilder<byte[]> readerBuilder = mock(ReaderBuilder.class);
        doReturn(readerBuilder).when(pulsarClient).newReader();
        doReturn(readerBuilder).when(readerBuilder).topic(anyString());
        doReturn(readerBuilder).when(readerBuilder).startMessageId(any());
        doReturn(readerBuilder).when(readerBuilder).readCompacted(true);
        doReturn(mock(Reader.class)).when(readerBuilder).create();
        WorkerService workerService = mock(WorkerService.class);
        doReturn(pulsarClient).when(workerService).getClient();
        doReturn(workerConfig).when(workerService).getWorkerConfig();
<<<<<<< HEAD
        doReturn(mock(PulsarAdmin.class)).when(workerService).getFunctionAdmin();
=======
        PulsarAdmin pulsarAdmin = mock(PulsarAdmin.class);
        doReturn(pulsarAdmin).when(workerService).getFunctionAdmin();
>>>>>>> f773c602c... Test pr 10 (#27)

        FunctionMetaDataManager functionMetaDataManager = mock(FunctionMetaDataManager.class);
        FunctionRuntimeManager functionRuntimeManager = spy(new FunctionRuntimeManager(
                workerConfig,
                workerService,
                mock(Namespace.class),
                mock(MembershipManager.class),
                mock(ConnectorsManager.class),
<<<<<<< HEAD
                functionMetaDataManager));

        MembershipManager membershipManager = spy(new MembershipManager(workerService, mockPulsarClient()));
=======
                mock(FunctionsManager.class),
                functionMetaDataManager,
                mock(WorkerStatsManager.class),
                mock(ErrorNotifier.class)));

        MembershipManager membershipManager = spy(new MembershipManager(workerService, mockPulsarClient(), pulsarAdmin));
>>>>>>> f773c602c... Test pr 10 (#27)

        List<WorkerInfo> workerInfoList = new LinkedList<>();
        workerInfoList.add(WorkerInfo.of("worker-1", "host-1", 8000));

        Mockito.doReturn(workerInfoList).when(membershipManager).getCurrentMembership();

        Function.FunctionMetaData function1 = Function.FunctionMetaData.newBuilder().setFunctionDetails(
                Function.FunctionDetails.newBuilder()
                        .setTenant("test-tenant").setNamespace("test-namespace").setName("func-1")).build();

        Function.FunctionMetaData function2 = Function.FunctionMetaData.newBuilder().setFunctionDetails(
                Function.FunctionDetails.newBuilder()
                        .setTenant("test-tenant").setNamespace("test-namespace").setName("func-2")).build();

        List<Function.FunctionMetaData> metaDataList = new LinkedList<>();
        metaDataList.add(function1);
        metaDataList.add(function2);

        Mockito.doReturn(metaDataList).when(functionMetaDataManager).getAllFunctionMetaData();
        Function.Assignment assignment1 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1").setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function1).setInstanceId(0).build())
                .build();
        Function.Assignment assignment2 = Function.Assignment.newBuilder()
                .setWorkerId("worker-2")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function2).setInstanceId(0).build())
                .build();

        // add existing assignments
        functionRuntimeManager.setAssignment(assignment1);
        functionRuntimeManager.setAssignment(assignment2);

        membershipManager.checkFailures(functionMetaDataManager, functionRuntimeManager, schedulerManager);

        verify(schedulerManager, times(0)).schedule();
        verify(functionRuntimeManager, times(0)).removeAssignments(any());
<<<<<<< HEAD
        Assert.assertEquals(membershipManager.unsignedFunctionDurations.size(), 1);
        Function.Instance instance = Function.Instance.newBuilder().setFunctionMetaData(function2).setInstanceId(0).build();
        Assert.assertTrue(membershipManager.unsignedFunctionDurations.get(instance) != null);
=======
        assertEquals(membershipManager.unsignedFunctionDurations.size(), 1);
        Function.Instance instance = Function.Instance.newBuilder().setFunctionMetaData(function2).setInstanceId(0).build();
        assertNotNull(membershipManager.unsignedFunctionDurations.get(instance));
>>>>>>> f773c602c... Test pr 10 (#27)

        membershipManager.unsignedFunctionDurations.put(instance,
                membershipManager.unsignedFunctionDurations.get(instance) - 30001);

        membershipManager.checkFailures(functionMetaDataManager, functionRuntimeManager, schedulerManager);

        verify(functionRuntimeManager, times(1)).removeAssignments(
<<<<<<< HEAD
                argThat(new ArgumentMatcher<Collection<Function.Assignment>>() {
            @Override
            public boolean matches(Object o) {
                if (o instanceof Collection) {
                    Collection<Function.Assignment> assignments = (Collection) o;

                    if (!assignments.contains(assignment2)) {
                        return false;
                    }
                    return true;
                }
                return false;
            }
        }));
=======
                argThat(assignments -> assignments.contains(assignment2)));
>>>>>>> f773c602c... Test pr 10 (#27)

        verify(schedulerManager, times(1)).schedule();
    }

    @Test
    public void testCheckFailuresSomeUnassigned() throws Exception {
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
<<<<<<< HEAD
        workerConfig.setThreadContainerFactory(new WorkerConfig.ThreadContainerFactory().setThreadGroupName("test"));
=======
        workerConfig.setFunctionRuntimeFactoryClassName(ThreadRuntimeFactory.class.getName());
        workerConfig.setFunctionRuntimeFactoryConfigs(
                ObjectMapperFactory.getThreadLocal().convertValue(
                        new ThreadRuntimeFactoryConfig().setThreadGroupName("test"), Map.class));
>>>>>>> f773c602c... Test pr 10 (#27)
        workerConfig.setPulsarServiceUrl("pulsar://localhost:6650");
        workerConfig.setStateStorageServiceUrl("foo");
        workerConfig.setRescheduleTimeoutMs(30000);
        SchedulerManager schedulerManager = mock(SchedulerManager.class);
        PulsarClient pulsarClient = mockPulsarClient();
        ReaderBuilder<byte[]> readerBuilder = mock(ReaderBuilder.class);
        doReturn(readerBuilder).when(pulsarClient).newReader();
        doReturn(readerBuilder).when(readerBuilder).topic(anyString());
        doReturn(readerBuilder).when(readerBuilder).readCompacted(true);
        doReturn(readerBuilder).when(readerBuilder).startMessageId(any());
        doReturn(mock(Reader.class)).when(readerBuilder).create();
        WorkerService workerService = mock(WorkerService.class);
        doReturn(pulsarClient).when(workerService).getClient();
        doReturn(workerConfig).when(workerService).getWorkerConfig();
<<<<<<< HEAD
        doReturn(mock(PulsarAdmin.class)).when(workerService).getFunctionAdmin();
=======
        PulsarAdmin pulsarAdmin = mock(PulsarAdmin.class);
        doReturn(pulsarAdmin).when(workerService).getFunctionAdmin();
>>>>>>> f773c602c... Test pr 10 (#27)

        FunctionMetaDataManager functionMetaDataManager = mock(FunctionMetaDataManager.class);
        FunctionRuntimeManager functionRuntimeManager = spy(new FunctionRuntimeManager(
                workerConfig,
                workerService,
                mock(Namespace.class),
                mock(MembershipManager.class),
                mock(ConnectorsManager.class),
<<<<<<< HEAD
                functionMetaDataManager));
        MembershipManager membershipManager = spy(new MembershipManager(workerService, mockPulsarClient()));
=======
                mock(FunctionsManager.class),
                functionMetaDataManager,
                mock(WorkerStatsManager.class),
                mock(ErrorNotifier.class)));
        MembershipManager membershipManager = spy(new MembershipManager(workerService, mockPulsarClient(), pulsarAdmin));
>>>>>>> f773c602c... Test pr 10 (#27)

        List<WorkerInfo> workerInfoList = new LinkedList<>();
        workerInfoList.add(WorkerInfo.of("worker-1", "host-1", 8000));
        workerInfoList.add(WorkerInfo.of("worker-2", "host-2", 8001));

        Mockito.doReturn(workerInfoList).when(membershipManager).getCurrentMembership();

        Function.FunctionMetaData function1 = Function.FunctionMetaData.newBuilder().setFunctionDetails(
                Function.FunctionDetails.newBuilder().setParallelism(1)
                        .setTenant("test-tenant").setNamespace("test-namespace").setName("func-1")).build();

        Function.FunctionMetaData function2 = Function.FunctionMetaData.newBuilder().setFunctionDetails(
                Function.FunctionDetails.newBuilder().setParallelism(1)
                        .setTenant("test-tenant").setNamespace("test-namespace").setName("func-2")).build();

        List<Function.FunctionMetaData> metaDataList = new LinkedList<>();
        metaDataList.add(function1);
        metaDataList.add(function2);

        Mockito.doReturn(metaDataList).when(functionMetaDataManager).getAllFunctionMetaData();
        Function.Assignment assignment1 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1").setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function1).setInstanceId(0).build())
                .build();

        // add existing assignments
        functionRuntimeManager.setAssignment(assignment1);

        membershipManager.checkFailures(functionMetaDataManager, functionRuntimeManager, schedulerManager);

        verify(schedulerManager, times(0)).schedule();
        verify(functionRuntimeManager, times(0)).removeAssignments(any());
<<<<<<< HEAD
        Assert.assertEquals(membershipManager.unsignedFunctionDurations.size(), 1);
        Function.Instance instance = Function.Instance.newBuilder().setFunctionMetaData(function2).setInstanceId(0).build();
        Assert.assertTrue(membershipManager.unsignedFunctionDurations.get(instance) != null);
=======
        assertEquals(membershipManager.unsignedFunctionDurations.size(), 1);
        Function.Instance instance = Function.Instance.newBuilder().setFunctionMetaData(function2).setInstanceId(0).build();
        assertNotNull(membershipManager.unsignedFunctionDurations.get(instance));
>>>>>>> f773c602c... Test pr 10 (#27)

        membershipManager.unsignedFunctionDurations.put(instance,
                membershipManager.unsignedFunctionDurations.get(instance) - 30001);

        membershipManager.checkFailures(functionMetaDataManager, functionRuntimeManager, schedulerManager);

        verify(schedulerManager, times(1)).schedule();
        verify(functionRuntimeManager, times(0)).removeAssignments(any());
    }

    @Test
    public void testHeartBeatFunctionWorkerDown() throws Exception {
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
<<<<<<< HEAD
        workerConfig.setThreadContainerFactory(new WorkerConfig.ThreadContainerFactory().setThreadGroupName("test"));
=======
        workerConfig.setFunctionRuntimeFactoryClassName(ThreadRuntimeFactory.class.getName());
        workerConfig.setFunctionRuntimeFactoryConfigs(
                ObjectMapperFactory.getThreadLocal().convertValue(
                        new ThreadRuntimeFactoryConfig().setThreadGroupName("test"), Map.class));
>>>>>>> f773c602c... Test pr 10 (#27)
        workerConfig.setPulsarServiceUrl("pulsar://localhost:6650");
        workerConfig.setStateStorageServiceUrl("foo");
        workerConfig.setRescheduleTimeoutMs(30000);
        SchedulerManager schedulerManager = mock(SchedulerManager.class);
        PulsarClient pulsarClient = mockPulsarClient();
        ReaderBuilder<byte[]> readerBuilder = mock(ReaderBuilder.class);
        doReturn(readerBuilder).when(pulsarClient).newReader();
        doReturn(readerBuilder).when(readerBuilder).topic(anyString());
        doReturn(readerBuilder).when(readerBuilder).readCompacted(true);
        doReturn(readerBuilder).when(readerBuilder).startMessageId(any());
        doReturn(mock(Reader.class)).when(readerBuilder).create();
        WorkerService workerService = mock(WorkerService.class);
        doReturn(pulsarClient).when(workerService).getClient();
        doReturn(workerConfig).when(workerService).getWorkerConfig();
<<<<<<< HEAD
=======
        PulsarAdmin pulsarAdmin = mock(PulsarAdmin.class);
>>>>>>> f773c602c... Test pr 10 (#27)
        doReturn(mock(PulsarAdmin.class)).when(workerService).getFunctionAdmin();

        FunctionMetaDataManager functionMetaDataManager = mock(FunctionMetaDataManager.class);
        FunctionRuntimeManager functionRuntimeManager = spy(new FunctionRuntimeManager(
                workerConfig,
                workerService,
                mock(Namespace.class),
                mock(MembershipManager.class),
                mock(ConnectorsManager.class),
<<<<<<< HEAD
                functionMetaDataManager));
        MembershipManager membershipManager = spy(new MembershipManager(workerService, mockPulsarClient()));
=======
                mock(FunctionsManager.class),
                functionMetaDataManager,
                mock(WorkerStatsManager.class),
                mock(ErrorNotifier.class)));
        MembershipManager membershipManager = spy(new MembershipManager(workerService, mockPulsarClient(), pulsarAdmin));
>>>>>>> f773c602c... Test pr 10 (#27)

        List<WorkerInfo> workerInfoList = new LinkedList<>();
        workerInfoList.add(WorkerInfo.of("worker-1", "host-1", 8000));
        // make worker-2 unavailable
        //workerInfoList.add(WorkerInfo.of("worker-2", "host-2", 8001));

        Mockito.doReturn(workerInfoList).when(membershipManager).getCurrentMembership();

        Function.FunctionMetaData function1 = Function.FunctionMetaData.newBuilder().setFunctionDetails(
                Function.FunctionDetails.newBuilder().setParallelism(1)
                        .setTenant("test-tenant").setNamespace("test-namespace").setName("func-1")).build();

        Function.FunctionMetaData function2 = Function.FunctionMetaData.newBuilder()
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setParallelism(1)
                        .setTenant(SchedulerManager.HEARTBEAT_TENANT)
                        .setNamespace(SchedulerManager.HEARTBEAT_NAMESPACE).setName("worker-2"))
                .build();

        List<Function.FunctionMetaData> metaDataList = new LinkedList<>();
        metaDataList.add(function1);
        metaDataList.add(function2);

        Mockito.doReturn(metaDataList).when(functionMetaDataManager).getAllFunctionMetaData();
        Function.Assignment assignment1 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1").setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function1).setInstanceId(0).build())
                .build();
        Function.Assignment assignment2 = Function.Assignment.newBuilder()
                .setWorkerId("worker-2").setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function2).setInstanceId(0).build())
                .build();

        // add existing assignments
        functionRuntimeManager.setAssignment(assignment1);
        functionRuntimeManager.setAssignment(assignment2);

        membershipManager.checkFailures(functionMetaDataManager, functionRuntimeManager, schedulerManager);

        verify(schedulerManager, times(0)).schedule();
        verify(functionRuntimeManager, times(0)).removeAssignments(any());
<<<<<<< HEAD
        Assert.assertEquals(membershipManager.unsignedFunctionDurations.size(), 0);
    }
    
=======
        assertEquals(membershipManager.unsignedFunctionDurations.size(), 0);
    }
>>>>>>> f773c602c... Test pr 10 (#27)
}
