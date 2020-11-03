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
=======
import static org.mockito.Mockito.any;
>>>>>>> f773c602c... Test pr 10 (#27)
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
<<<<<<< HEAD
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.net.UnknownHostException;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.pulsar.client.admin.PulsarAdmin;
=======
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.auth.FunctionAuthProvider;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.PackageLocationMetaData;
import org.apache.pulsar.functions.runtime.Runtime;
import org.apache.pulsar.functions.runtime.RuntimeFactory;
<<<<<<< HEAD
import org.apache.pulsar.functions.runtime.ThreadRuntimeFactory;
import org.testng.annotations.Test;
import static org.apache.pulsar.common.functions.Utils.FILE;
=======
import org.apache.pulsar.functions.runtime.RuntimeSpawner;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactoryConfig;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactory;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static org.apache.pulsar.common.functions.Utils.FILE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.AssertJUnit.fail;
>>>>>>> f773c602c... Test pr 10 (#27)

/**
 * Unit test of {@link FunctionActioner}.
 */
public class FunctionActionerTest {

    /**
     * Validates FunctionActioner tries to download file from bk.
     *
     * @throws Exception
     */
    @Test
    public void testStartFunctionWithDLNamespace() throws Exception {

        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
<<<<<<< HEAD
        workerConfig.setThreadContainerFactory(new WorkerConfig.ThreadContainerFactory().setThreadGroupName("test"));
        workerConfig.setPulsarServiceUrl("pulsar://localhost:6650");
=======
        workerConfig.setFunctionRuntimeFactoryClassName(ThreadRuntimeFactory.class.getName());
        workerConfig.setFunctionRuntimeFactoryConfigs(
                ObjectMapperFactory.getThreadLocal().convertValue(
                        new ThreadRuntimeFactoryConfig().setThreadGroupName("test"), Map.class));        workerConfig.setPulsarServiceUrl("pulsar://localhost:6650");
>>>>>>> f773c602c... Test pr 10 (#27)
        workerConfig.setStateStorageServiceUrl("foo");
        workerConfig.setFunctionAssignmentTopicName("assignments");

        ThreadRuntimeFactory factory = mock(ThreadRuntimeFactory.class);
        Namespace dlogNamespace = mock(Namespace.class);
        // throw exception when dlogNamespace is accessed by actioner and verify it
        final String exceptionMsg = "dl namespace not-found";
        doThrow(new IllegalArgumentException(exceptionMsg)).when(dlogNamespace).openLog(any());
<<<<<<< HEAD
        LinkedBlockingQueue<FunctionAction> queue = new LinkedBlockingQueue<>();

        @SuppressWarnings("resource")
        FunctionActioner actioner = new FunctionActioner(workerConfig, factory, dlogNamespace, queue,
                new ConnectorsManager(workerConfig), mock(PulsarAdmin.class));
=======

        @SuppressWarnings("resource")
        FunctionActioner actioner = new FunctionActioner(workerConfig, factory, dlogNamespace,
                new ConnectorsManager(workerConfig), new FunctionsManager(workerConfig), mock(PulsarAdmin.class));
>>>>>>> f773c602c... Test pr 10 (#27)
        Runtime runtime = mock(Runtime.class);
        Function.FunctionMetaData function1 = Function.FunctionMetaData.newBuilder()
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setTenant("test-tenant")
                        .setNamespace("test-namespace").setName("func-1"))
                .build();
        Function.Instance instance = Function.Instance.newBuilder().setFunctionMetaData(function1).setInstanceId(0)
                .build();
        FunctionRuntimeInfo functionRuntimeInfo = mock(FunctionRuntimeInfo.class);
        doReturn(instance).when(functionRuntimeInfo).getFunctionInstance();
<<<<<<< HEAD
=======
        doThrow(new IllegalStateException("StartupException")).when(functionRuntimeInfo).setStartupException(any());
>>>>>>> f773c602c... Test pr 10 (#27)

        // actioner should try to download file from bk-dlogNamespace and fails with exception
        try {
            actioner.startFunction(functionRuntimeInfo);
<<<<<<< HEAD
            fail("should have failed with dlogNamespace open");
        } catch (IllegalArgumentException ie) {
            assertEquals(ie.getMessage(), exceptionMsg);
=======
            fail();
        } catch (IllegalStateException ex) {
            assertEquals(ex.getMessage(), "StartupException");
>>>>>>> f773c602c... Test pr 10 (#27)
        }
    }

    @Test
    public void testStartFunctionWithPkgUrl() throws Exception {

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
        workerConfig.setFunctionAssignmentTopicName("assignments");
        String downloadDir = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
        workerConfig.setDownloadDirectory(downloadDir);

        RuntimeFactory factory = mock(RuntimeFactory.class);
        Runtime runtime = mock(Runtime.class);
        doReturn(runtime).when(factory).createContainer(any(), any(), any(), any());
        doNothing().when(runtime).start();
        Namespace dlogNamespace = mock(Namespace.class);
        final String exceptionMsg = "dl namespace not-found";
        doThrow(new IllegalArgumentException(exceptionMsg)).when(dlogNamespace).openLog(any());
<<<<<<< HEAD
        LinkedBlockingQueue<FunctionAction> queue = new LinkedBlockingQueue<>();

        @SuppressWarnings("resource")
        FunctionActioner actioner = new FunctionActioner(workerConfig, factory, dlogNamespace, queue,
                new ConnectorsManager(workerConfig), mock(PulsarAdmin.class));
=======

        @SuppressWarnings("resource")
        FunctionActioner actioner = new FunctionActioner(workerConfig, factory, dlogNamespace,
                new ConnectorsManager(workerConfig), new FunctionsManager(workerConfig), mock(PulsarAdmin.class));
>>>>>>> f773c602c... Test pr 10 (#27)

        // (1) test with file url. functionActioner should be able to consider file-url and it should be able to call
        // RuntimeSpawner
        String pkgPathLocation = FILE + ":/user/my-file.jar";
        Function.FunctionMetaData function1 = Function.FunctionMetaData.newBuilder()
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setTenant("test-tenant")
                        .setNamespace("test-namespace").setName("func-1"))
                .setPackageLocation(PackageLocationMetaData.newBuilder().setPackagePath(pkgPathLocation).build())
                .build();
        Function.Instance instance = Function.Instance.newBuilder().setFunctionMetaData(function1).setInstanceId(0)
                .build();
        FunctionRuntimeInfo functionRuntimeInfo = mock(FunctionRuntimeInfo.class);
        doReturn(instance).when(functionRuntimeInfo).getFunctionInstance();

        actioner.startFunction(functionRuntimeInfo);
        verify(runtime, times(1)).start();

        // (2) test with http-url, downloading file from http should fail with UnknownHostException due to invalid url
        pkgPathLocation = "http://invalid/my-file.jar";
        function1 = Function.FunctionMetaData.newBuilder()
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setTenant("test-tenant")
                        .setNamespace("test-namespace").setName("func-1"))
                .setPackageLocation(PackageLocationMetaData.newBuilder().setPackagePath(pkgPathLocation).build())
                .build();
        instance = Function.Instance.newBuilder().setFunctionMetaData(function1).setInstanceId(0).build();
        functionRuntimeInfo = mock(FunctionRuntimeInfo.class);
        doReturn(instance).when(functionRuntimeInfo).getFunctionInstance();
<<<<<<< HEAD

        try {
            actioner.startFunction(functionRuntimeInfo);
            fail("Function-Actioner should have tried to donwload file from http-location");
        } catch (UnknownHostException ue) {
            // ok
        }
    }

=======
        doThrow(new IllegalStateException("StartupException")).when(functionRuntimeInfo).setStartupException(any());

        try {
            actioner.startFunction(functionRuntimeInfo);
            fail();
        } catch (IllegalStateException ex) {
            assertEquals(ex.getMessage(), "StartupException");
        }
    }

    @Test
    public void testFunctionAuthDisabled() throws Exception {
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        workerConfig.setFunctionRuntimeFactoryClassName(ThreadRuntimeFactory.class.getName());
        workerConfig.setFunctionRuntimeFactoryConfigs(
                ObjectMapperFactory.getThreadLocal().convertValue(
                        new ThreadRuntimeFactoryConfig().setThreadGroupName("test"), Map.class));
        workerConfig.setPulsarServiceUrl("pulsar://localhost:6650");
        workerConfig.setStateStorageServiceUrl("foo");
        workerConfig.setFunctionAssignmentTopicName("assignments");
        String downloadDir = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
        workerConfig.setDownloadDirectory(downloadDir);

        RuntimeFactory factory = mock(RuntimeFactory.class);
        Runtime runtime = mock(Runtime.class);
        doReturn(runtime).when(factory).createContainer(any(), any(), any(), any());
        doNothing().when(runtime).start();
        Namespace dlogNamespace = mock(Namespace.class);
        final String exceptionMsg = "dl namespace not-found";
        doThrow(new IllegalArgumentException(exceptionMsg)).when(dlogNamespace).openLog(any());

        @SuppressWarnings("resource")
        FunctionActioner actioner = new FunctionActioner(workerConfig, factory, dlogNamespace,
                new ConnectorsManager(workerConfig), new FunctionsManager(workerConfig), mock(PulsarAdmin.class));


        String pkgPathLocation = "http://invalid/my-file.jar";
        Function.FunctionMetaData functionMeta = Function.FunctionMetaData.newBuilder()
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setTenant("test-tenant")
                        .setNamespace("test-namespace").setName("func-1"))
                .setPackageLocation(PackageLocationMetaData.newBuilder().setPackagePath(pkgPathLocation).build())
                .build();

        Function.Instance instance = Function.Instance.newBuilder()
                .setFunctionMetaData(functionMeta).build();

        RuntimeSpawner runtimeSpawner = spy(actioner.getRuntimeSpawner(instance, "foo"));

        assertNull(runtimeSpawner.getInstanceConfig().getFunctionAuthenticationSpec());

        FunctionRuntimeInfo functionRuntimeInfo = mock(FunctionRuntimeInfo.class);

        RuntimeFactory runtimeFactory = mock(RuntimeFactory.class);

        Optional<FunctionAuthProvider> functionAuthProvider = Optional.of(mock(FunctionAuthProvider.class));
        doReturn(functionAuthProvider).when(runtimeFactory).getAuthProvider();

        doReturn(runtimeFactory).when(runtimeSpawner).getRuntimeFactory();
        doReturn(instance).when(functionRuntimeInfo).getFunctionInstance();
        doReturn(runtimeSpawner).when(functionRuntimeInfo).getRuntimeSpawner();

        actioner.terminateFunction(functionRuntimeInfo);

        // make sure cache
        verify(functionAuthProvider.get(), times(0)).cleanUpAuthData(any(), any());
    }

>>>>>>> f773c602c... Test pr 10 (#27)
}
