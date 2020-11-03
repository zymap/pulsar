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
package org.apache.pulsar.functions.worker.rest.api.v3;

<<<<<<< HEAD
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
=======
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.doThrow;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.testng.Assert.assertEquals;

import com.google.common.collect.Lists;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.Functions;
import org.apache.pulsar.client.admin.Namespaces;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.FutureUtil;
<<<<<<< HEAD
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
=======
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.instance.InstanceUtils;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.Function.FunctionMetaData;
import org.apache.pulsar.functions.proto.Function.PackageLocationMetaData;
import org.apache.pulsar.functions.proto.Function.ProcessingGuarantees;
import org.apache.pulsar.functions.proto.Function.SinkSpec;
import org.apache.pulsar.functions.proto.Function.SourceSpec;
import org.apache.pulsar.functions.proto.Function.SubscriptionType;
import org.apache.pulsar.functions.runtime.RuntimeFactory;
import org.apache.pulsar.functions.source.TopicSchema;
import org.apache.pulsar.functions.utils.FunctionConfigUtils;
<<<<<<< HEAD
import org.apache.pulsar.functions.worker.FunctionMetaDataManager;
import org.apache.pulsar.functions.worker.FunctionRuntimeManager;
import org.apache.pulsar.functions.worker.Utils;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.request.RequestResult;
import org.apache.pulsar.functions.worker.rest.RestException;
import org.apache.pulsar.functions.worker.rest.api.ComponentImpl;
import org.apache.pulsar.functions.worker.rest.api.FunctionsImpl;
import org.apache.pulsar.functions.worker.rest.api.v2.FunctionApiV2Resource;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
=======
import org.apache.pulsar.functions.worker.*;
import org.apache.pulsar.functions.worker.rest.api.FunctionsImpl;
import org.apache.pulsar.functions.worker.rest.api.v2.FunctionsApiV2Resource;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.Assert;
import org.testng.IObjectFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

<<<<<<< HEAD
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.apache.pulsar.functions.utils.Utils.ComponentType.FUNCTION;
import static org.apache.pulsar.functions.utils.Utils.ComponentType.SINK;
import static org.apache.pulsar.functions.utils.Utils.ComponentType.SOURCE;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.doReturn;
import static org.powermock.api.mockito.PowerMockito.doThrow;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.testng.Assert.assertEquals;

/**
 * Unit test of {@link FunctionApiV2Resource}.
 */
@PrepareForTest(Utils.class)
@PowerMockIgnore({ "javax.management.*", "javax.ws.*", "org.apache.logging.log4j.*" })
@Slf4j
=======
/**
 * Unit test of {@link FunctionsApiV2Resource}.
 */
@PrepareForTest({WorkerUtils.class, InstanceUtils.class})
@PowerMockIgnore({ "javax.management.*", "javax.ws.*", "org.apache.logging.log4j.*", "org.apache.pulsar.functions.api.*" })
>>>>>>> f773c602c... Test pr 10 (#27)
public class FunctionApiV3ResourceTest {

    @ObjectFactory
    public IObjectFactory getObjectFactory() {
        return new org.powermock.modules.testng.PowerMockObjectFactory();
    }

    private static final class TestFunction implements Function<String, String> {

        @Override
        public String process(String input, Context context) {
            return input;
        }
    }

<<<<<<< HEAD
=======
    private static final class WrongFunction implements Consumer<String> {
        @Override
        public void accept(String s) {

        }
    }

>>>>>>> f773c602c... Test pr 10 (#27)
    private static final String tenant = "test-tenant";
    private static final String namespace = "test-namespace";
    private static final String function = "test-function";
    private static final String outputTopic = "test-output-topic";
    private static final String outputSerdeClassName = TopicSchema.DEFAULT_SERDE;
    private static final String className = TestFunction.class.getName();
    private SubscriptionType subscriptionType = SubscriptionType.FAILOVER;
    private static final Map<String, String> topicsToSerDeClassName = new HashMap<>();
    static {
        topicsToSerDeClassName.put("persistent://sample/standalone/ns1/test_src", TopicSchema.DEFAULT_SERDE);
    }
    private static final int parallelism = 1;

    private WorkerService mockedWorkerService;
    private PulsarAdmin mockedPulsarAdmin;
    private Tenants mockedTenants;
    private Namespaces mockedNamespaces;
<<<<<<< HEAD
=======
    private Functions mockedFunctions;
>>>>>>> f773c602c... Test pr 10 (#27)
    private TenantInfo mockedTenantInfo;
    private List<String> namespaceList = new LinkedList<>();
    private FunctionMetaDataManager mockedManager;
    private FunctionRuntimeManager mockedFunctionRunTimeManager;
    private RuntimeFactory mockedRuntimeFactory;
    private Namespace mockedNamespace;
    private FunctionsImpl resource;
    private InputStream mockedInputStream;
    private FormDataContentDisposition mockedFormData;
    private FunctionMetaData mockedFunctionMetadata;
<<<<<<< HEAD
=======
    private LeaderService mockedLeaderService;
>>>>>>> f773c602c... Test pr 10 (#27)

    @BeforeMethod
    public void setup() throws Exception {
        this.mockedManager = mock(FunctionMetaDataManager.class);
        this.mockedFunctionRunTimeManager = mock(FunctionRuntimeManager.class);
        this.mockedTenantInfo = mock(TenantInfo.class);
        this.mockedRuntimeFactory = mock(RuntimeFactory.class);
        this.mockedInputStream = mock(InputStream.class);
        this.mockedNamespace = mock(Namespace.class);
        this.mockedFormData = mock(FormDataContentDisposition.class);
        when(mockedFormData.getFileName()).thenReturn("test");
        this.mockedPulsarAdmin = mock(PulsarAdmin.class);
        this.mockedTenants = mock(Tenants.class);
        this.mockedNamespaces = mock(Namespaces.class);
<<<<<<< HEAD
=======
        this.mockedFunctions = mock(Functions.class);
        this.mockedLeaderService = mock(LeaderService.class);
>>>>>>> f773c602c... Test pr 10 (#27)
        this.mockedFunctionMetadata = FunctionMetaData.newBuilder().setFunctionDetails(createDefaultFunctionDetails()).build();
        namespaceList.add(tenant + "/" + namespace);

        this.mockedWorkerService = mock(WorkerService.class);
        when(mockedWorkerService.getFunctionMetaDataManager()).thenReturn(mockedManager);
        when(mockedWorkerService.getFunctionRuntimeManager()).thenReturn(mockedFunctionRunTimeManager);
<<<<<<< HEAD
=======
        when(mockedWorkerService.getLeaderService()).thenReturn(mockedLeaderService);
>>>>>>> f773c602c... Test pr 10 (#27)
        when(mockedFunctionRunTimeManager.getRuntimeFactory()).thenReturn(mockedRuntimeFactory);
        when(mockedWorkerService.getDlogNamespace()).thenReturn(mockedNamespace);
        when(mockedWorkerService.isInitialized()).thenReturn(true);
        when(mockedWorkerService.getBrokerAdmin()).thenReturn(mockedPulsarAdmin);
<<<<<<< HEAD
        when(mockedPulsarAdmin.tenants()).thenReturn(mockedTenants);
        when(mockedPulsarAdmin.namespaces()).thenReturn(mockedNamespaces);
        when(mockedTenants.getTenantInfo(any())).thenReturn(mockedTenantInfo);
        when(mockedNamespaces.getNamespaces(any())).thenReturn(namespaceList);
=======
        when(mockedWorkerService.getFunctionAdmin()).thenReturn(mockedPulsarAdmin);
        when(mockedPulsarAdmin.tenants()).thenReturn(mockedTenants);
        when(mockedPulsarAdmin.namespaces()).thenReturn(mockedNamespaces);
        when(mockedPulsarAdmin.functions()).thenReturn(mockedFunctions);
        when(mockedTenants.getTenantInfo(any())).thenReturn(mockedTenantInfo);
        when(mockedNamespaces.getNamespaces(any())).thenReturn(namespaceList);
        when(mockedLeaderService.isLeader()).thenReturn(true);
>>>>>>> f773c602c... Test pr 10 (#27)
        when(mockedManager.getFunctionMetaData(any(), any(), any())).thenReturn(mockedFunctionMetadata);

        // worker config
        WorkerConfig workerConfig = new WorkerConfig()
            .setWorkerId("test")
            .setWorkerPort(8080)
            .setDownloadDirectory("/tmp/pulsar/functions")
            .setFunctionMetadataTopicName("pulsar/functions")
            .setNumFunctionPackageReplicas(3)
            .setPulsarServiceUrl("pulsar://localhost:6650/");
        when(mockedWorkerService.getWorkerConfig()).thenReturn(workerConfig);

        this.resource = spy(new FunctionsImpl(() -> mockedWorkerService));
<<<<<<< HEAD
        doReturn(FUNCTION).when(this.resource).calculateSubjectType(any());
=======
        mockStatic(InstanceUtils.class);
        PowerMockito.when(InstanceUtils.calculateSubjectType(any())).thenReturn(FunctionDetails.ComponentType.FUNCTION);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    //
    // Register Functions
    //

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
    public void testRegisterFunctionMissingTenant() {
        try {
            testRegisterFunctionMissingArguments(
                    null,
                    namespace,
                    function,
                    mockedInputStream,
                    topicsToSerDeClassName,
                    mockedFormData,
                    outputTopic,
                    outputSerdeClassName,
                    className,
                    parallelism,
                    null);
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace is not provided")
    public void testRegisterFunctionMissingNamespace() {
        try {
            testRegisterFunctionMissingArguments(
                tenant,
                null,
                function,
                mockedInputStream,
                topicsToSerDeClassName,
                mockedFormData,
                outputTopic,
                    outputSerdeClassName,
                className,
                parallelism,
                    null);
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

<<<<<<< HEAD
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function Name is not provided")
=======
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function name is not provided")
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testRegisterFunctionMissingFunctionName() {
        try {
        testRegisterFunctionMissingArguments(
            tenant,
            namespace,
            null,
            mockedInputStream,
            topicsToSerDeClassName,
            mockedFormData,
            outputTopic,
                outputSerdeClassName,
            className,
            parallelism,
                null);
    } catch (RestException re){
        assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
        throw re;
    }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function Package is not provided")
    public void testRegisterFunctionMissingPackage() {
        try {
            testRegisterFunctionMissingArguments(
                    tenant,
                    namespace,
                    function,
                    null,
                    topicsToSerDeClassName,
                    mockedFormData,
                    outputTopic,
                    outputSerdeClassName,
                    className,
                    parallelism,
                    null);
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "No input topic\\(s\\) specified for the function")
    public void testRegisterFunctionMissingInputTopics() {
        try {
            testRegisterFunctionMissingArguments(
                    tenant,
                    namespace,
                    function,
                    null,
                    null,
                    mockedFormData,
                    outputTopic,
                    outputSerdeClassName,
                    className,
                    parallelism,
                    null);
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function Package is not provided")
    public void testRegisterFunctionMissingPackageDetails() {
        try {
            testRegisterFunctionMissingArguments(
                tenant,
                namespace,
                function,
                mockedInputStream,
                topicsToSerDeClassName,
                null,
                outputTopic,
                    outputSerdeClassName,
                className,
                parallelism,
                    null);
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function classname cannot be null")
    public void testRegisterFunctionMissingClassName() {
        try {
            testRegisterFunctionMissingArguments(
                    tenant,
                    namespace,
                    function,
                    mockedInputStream,
                    topicsToSerDeClassName,
                    mockedFormData,
                    outputTopic,
                    outputSerdeClassName,
                    null,
                    parallelism,
                    null);
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

<<<<<<< HEAD
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "User class must be in class path")
=======
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function class UnknownClass must be in class path")
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testRegisterFunctionWrongClassName() {
        try {
            testRegisterFunctionMissingArguments(
                    tenant,
                    namespace,
                    function,
                    mockedInputStream,
                    topicsToSerDeClassName,
                    mockedFormData,
                    outputTopic,
                    outputSerdeClassName,
                    "UnknownClass",
                    parallelism,
                    null);
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

<<<<<<< HEAD
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function parallelism should positive number")
=======
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function parallelism must be a positive number")
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testRegisterFunctionWrongParallelism() {
        try {
            testRegisterFunctionMissingArguments(
                tenant,
                namespace,
                function,
                mockedInputStream,
                topicsToSerDeClassName,
                mockedFormData,
                outputTopic,
                outputSerdeClassName,
                className,
                -2,
                null);
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class,
            expectedExceptionsMessageRegExp = "Output topic persistent://sample/standalone/ns1/test_src is also being used as an input topic \\(topics must be one or the other\\)")
    public void testRegisterFunctionSameInputOutput() {
        try {
            testRegisterFunctionMissingArguments(
                    tenant,
                    namespace,
                    function,
                    mockedInputStream,
                    topicsToSerDeClassName,
                    mockedFormData,
                    topicsToSerDeClassName.keySet().iterator().next(),
                    outputSerdeClassName,
                    className,
                    parallelism,
                    null);
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Output topic " + function + "-output-topic/test:" + " is invalid")
    public void testRegisterFunctionWrongOutputTopic() {
        try {
            testRegisterFunctionMissingArguments(
                tenant,
                namespace,
                function,
                mockedInputStream,
                topicsToSerDeClassName,
                mockedFormData,
                function + "-output-topic/test:",
                outputSerdeClassName,
                className,
                parallelism,
                null);
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

<<<<<<< HEAD
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Corrupted Jar File")
=======
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Encountered error .*. when getting Function package from .*")
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testRegisterFunctionHttpUrl() {
        try {
            testRegisterFunctionMissingArguments(
                tenant,
                namespace,
                function,
                null,
                topicsToSerDeClassName,
                null,
                outputTopic,
                outputSerdeClassName,
                className,
                parallelism,
                "http://localhost:1234/test");
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

<<<<<<< HEAD
=======
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function class .*. does not implement the correct interface")
    public void testRegisterFunctionImplementWrongInterface() {
        try {
            testRegisterFunctionMissingArguments(
                    tenant,
                    namespace,
                    function,
                    mockedInputStream,
                    topicsToSerDeClassName,
                    mockedFormData,
                    outputTopic,
                    outputSerdeClassName,
                    WrongFunction.class.getName(),
                    parallelism,
                    null);
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

>>>>>>> f773c602c... Test pr 10 (#27)
    private void testRegisterFunctionMissingArguments(
            String tenant,
            String namespace,
            String function,
            InputStream inputStream,
            Map<String, String> topicsToSerDeClassName,
            FormDataContentDisposition details,
            String outputTopic,
            String outputSerdeClassName,
            String className,
            Integer parallelism,
            String functionPkgUrl) {
        FunctionConfig functionConfig = new FunctionConfig();
        if (tenant != null) {
            functionConfig.setTenant(tenant);
        }
        if (namespace != null) {
            functionConfig.setNamespace(namespace);
        }
        if (function != null) {
            functionConfig.setName(function);
        }
        if (topicsToSerDeClassName != null) {
            functionConfig.setCustomSerdeInputs(topicsToSerDeClassName);
        }
        if (outputTopic != null) {
            functionConfig.setOutput(outputTopic);
        }
        if (outputSerdeClassName != null) {
            functionConfig.setOutputSerdeClassName(outputSerdeClassName);
        }
        if (className != null) {
            functionConfig.setClassName(className);
        }
        if (parallelism != null) {
            functionConfig.setParallelism(parallelism);
        }
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);

        resource.registerFunction(
                tenant,
                namespace,
                function,
                inputStream,
                details,
                functionPkgUrl,
<<<<<<< HEAD
                null,
                new Gson().toJson(functionConfig),
                null);

=======
                functionConfig,
                null, null);

    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function config is not provided")
    public void testMissingFunctionConfig() {
        resource.registerFunction(
                tenant,
                namespace,
                function,
                mockedInputStream,
                mockedFormData,
                null,
                null,
                null, null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function config is not provided")
    public void testUpdateMissingFunctionConfig() {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

        resource.updateFunction(
                tenant,
                namespace,
                function,
                mockedInputStream,
                mockedFormData,
                null,
                null,
                null, null, null);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    private void registerDefaultFunction() {
        FunctionConfig functionConfig = createDefaultFunctionConfig();
        resource.registerFunction(
            tenant,
            namespace,
            function,
            mockedInputStream,
            mockedFormData,
            null,
<<<<<<< HEAD
            null,
            new Gson().toJson(functionConfig),
                null);
=======
            functionConfig,
                null, null);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function test-function already exists")
    public void testRegisterExistedFunction() {
        try {
            Configurator.setRootLevel(Level.DEBUG);
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);
            registerDefaultFunction();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }


    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "upload failure")
    public void testRegisterFunctionUploadFailure() throws Exception {
        try {
<<<<<<< HEAD
            mockStatic(Utils.class);
            doThrow(new IOException("upload failure")).when(Utils.class);

            Utils.uploadFileToBookkeeper(
                    anyString(),
                any(File.class),
                any(Namespace.class));
=======
            mockStatic(WorkerUtils.class);
            doThrow(new IOException("upload failure")).when(WorkerUtils.class);

            WorkerUtils.uploadFileToBookkeeper(
                    anyString(),
                any(File.class),
                any(Namespace.class));
            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

>>>>>>> f773c602c... Test pr 10 (#27)
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);

            registerDefaultFunction();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    @Test
    public void testRegisterFunctionSuccess() throws Exception {
        try {
<<<<<<< HEAD
            mockStatic(Utils.class);
            doNothing().when(Utils.class);
            Utils.uploadToBookeeper(
=======
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.uploadToBookeeper(
>>>>>>> f773c602c... Test pr 10 (#27)
                any(Namespace.class),
                any(InputStream.class),
                anyString());

<<<<<<< HEAD
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);

            RequestResult rr = new RequestResult()
                .setSuccess(true)
                .setMessage("function registered");
            CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
            when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);
=======
            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);
>>>>>>> f773c602c... Test pr 10 (#27)

            registerDefaultFunction();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace does not exist")
    public void testRegisterFunctionNonExistingNamespace() {
        try {
            this.namespaceList.clear();
            registerDefaultFunction();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant does not exist")
    public void testRegisterFunctionNonexistantTenant() throws Exception {
        try {
            when(mockedTenants.getTenantInfo(any())).thenThrow(PulsarAdminException.NotFoundException.class);
            registerDefaultFunction();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "function failed to register")
    public void testRegisterFunctionFailure() throws Exception {
        try {
<<<<<<< HEAD
            mockStatic(Utils.class);
            doNothing().when(Utils.class);
            Utils.uploadToBookeeper(
=======
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.uploadToBookeeper(
>>>>>>> f773c602c... Test pr 10 (#27)
                any(Namespace.class),
                any(InputStream.class),
                anyString());

<<<<<<< HEAD
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);

            RequestResult rr = new RequestResult()
                .setSuccess(false)
                .setMessage("function failed to register");
            CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
            when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);
=======
            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);

            doThrow(new IllegalArgumentException("function failed to register"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());
>>>>>>> f773c602c... Test pr 10 (#27)

            registerDefaultFunction();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

<<<<<<< HEAD
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "java.io.IOException: Function registeration interrupted")
    public void testRegisterFunctionInterrupted() throws Exception {
        try {
            mockStatic(Utils.class);
            doNothing().when(Utils.class);
            Utils.uploadToBookeeper(
=======
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function registration interrupted")
    public void testRegisterFunctionInterrupted() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.uploadToBookeeper(
>>>>>>> f773c602c... Test pr 10 (#27)
                any(Namespace.class),
                any(InputStream.class),
                anyString());

<<<<<<< HEAD
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);

            CompletableFuture<RequestResult> requestResult = FutureUtil.failedFuture(
                new IOException("Function registeration interrupted"));
            when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);
=======
            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);

            doThrow(new IllegalStateException("Function registration interrupted"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());
>>>>>>> f773c602c... Test pr 10 (#27)

            registerDefaultFunction();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    //
    // Update Functions
    //

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
<<<<<<< HEAD
    public void testUpdateFunctionMissingTenant() {
=======
    public void testUpdateFunctionMissingTenant() throws Exception {
>>>>>>> f773c602c... Test pr 10 (#27)
        try {
            testUpdateFunctionMissingArguments(
                null,
                namespace,
                function,
                mockedInputStream,
                topicsToSerDeClassName,
                mockedFormData,
                outputTopic,
                    outputSerdeClassName,
                className,
                parallelism,
                    "Tenant is not provided");
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace is not provided")
<<<<<<< HEAD
    public void testUpdateFunctionMissingNamespace() {
=======
    public void testUpdateFunctionMissingNamespace() throws Exception {
>>>>>>> f773c602c... Test pr 10 (#27)
        try {
            testUpdateFunctionMissingArguments(
                tenant,
                null,
                function,
                mockedInputStream,
                topicsToSerDeClassName,
                mockedFormData,
                outputTopic,
                    outputSerdeClassName,
                className,
                parallelism,
                    "Namespace is not provided");
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

<<<<<<< HEAD
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function Name is not provided")
    public void testUpdateFunctionMissingFunctionName() {
=======
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function name is not provided")
    public void testUpdateFunctionMissingFunctionName() throws Exception {
>>>>>>> f773c602c... Test pr 10 (#27)
        try {
            testUpdateFunctionMissingArguments(
                tenant,
                namespace,
                null,
                mockedInputStream,
                topicsToSerDeClassName,
                mockedFormData,
                outputTopic,
                    outputSerdeClassName,
                className,
                parallelism,
<<<<<<< HEAD
                    "Function Name is not provided");
=======
                    "Function name is not provided");
>>>>>>> f773c602c... Test pr 10 (#27)
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Update contains no change")
<<<<<<< HEAD
    public void testUpdateFunctionMissingPackage() throws IOException {
        try {
            mockStatic(Utils.class);
            doNothing().when(Utils.class);
            Utils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());
=======
    public void testUpdateFunctionMissingPackage() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());
            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();
>>>>>>> f773c602c... Test pr 10 (#27)
            testUpdateFunctionMissingArguments(
                tenant,
                namespace,
                function,
                null,
                topicsToSerDeClassName,
                mockedFormData,
                outputTopic,
                    outputSerdeClassName,
                className,
                parallelism,
                    "Update contains no change");
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Update contains no change")
<<<<<<< HEAD
    public void testUpdateFunctionMissingInputTopic() throws IOException {
        try {
            mockStatic(Utils.class);
            doNothing().when(Utils.class);
            Utils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());
=======
    public void testUpdateFunctionMissingInputTopic() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());
            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

>>>>>>> f773c602c... Test pr 10 (#27)
            testUpdateFunctionMissingArguments(
                    tenant,
                    namespace,
                    function,
                    null,
                    null,
                    mockedFormData,
                    outputTopic,
                    outputSerdeClassName,
                    className,
                    parallelism,
                    "Update contains no change");
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Update contains no change")
<<<<<<< HEAD
    public void testUpdateFunctionMissingClassName() throws IOException {
        try {
            mockStatic(Utils.class);
            doNothing().when(Utils.class);
            Utils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());
=======
    public void testUpdateFunctionMissingClassName() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());
            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

>>>>>>> f773c602c... Test pr 10 (#27)
            testUpdateFunctionMissingArguments(
                tenant,
                namespace,
                function,
                null,
                topicsToSerDeClassName,
                mockedFormData,
                outputTopic,
                    outputSerdeClassName,
                null,
                parallelism,
                    "Update contains no change");
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test
<<<<<<< HEAD
    public void testUpdateFunctionChangedParallelism() throws IOException {
        try {
            mockStatic(Utils.class);
            doNothing().when(Utils.class);
            Utils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());
=======
    public void testUpdateFunctionChangedParallelism() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());
            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

>>>>>>> f773c602c... Test pr 10 (#27)
            testUpdateFunctionMissingArguments(
                tenant,
                namespace,
                function,
                null,
                topicsToSerDeClassName,
                mockedFormData,
                outputTopic,
                outputSerdeClassName,
                null,
                parallelism + 1,
                null);
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

<<<<<<< HEAD
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Output topics differ")
    public void testUpdateFunctionChangedInputs() throws IOException {
        try {
            mockStatic(Utils.class);
            doNothing().when(Utils.class);
            Utils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());
            testUpdateFunctionMissingArguments(
                tenant,
                namespace,
                function,
                null,
                topicsToSerDeClassName,
                mockedFormData,
                "DifferentOutput",
                outputSerdeClassName,
                null,
                parallelism,
                "Output topics differ");
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Input Topics cannot be altered")
    public void testUpdateFunctionChangedOutput() throws IOException {
        try {
            mockStatic(Utils.class);
            doNothing().when(Utils.class);
            Utils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());
=======
    @Test
    public void testUpdateFunctionChangedInputs() throws Exception {
        mockStatic(WorkerUtils.class);
        doNothing().when(WorkerUtils.class);
        WorkerUtils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());
        PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

        testUpdateFunctionMissingArguments(
            tenant,
            namespace,
            function,
            null,
            topicsToSerDeClassName,
            mockedFormData,
            "DifferentOutput",
            outputSerdeClassName,
            null,
            parallelism,
            null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Input Topics cannot be altered")
    public void testUpdateFunctionChangedOutput() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());
            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

>>>>>>> f773c602c... Test pr 10 (#27)
            Map<String, String> someOtherInput = new HashMap<>();
            someOtherInput.put("DifferentTopic", TopicSchema.DEFAULT_SERDE);
            testUpdateFunctionMissingArguments(
                tenant,
                namespace,
                function,
                null,
                someOtherInput,
                mockedFormData,
                outputTopic,
                outputSerdeClassName,
                null,
                parallelism,
                "Input Topics cannot be altered");
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    private void testUpdateFunctionMissingArguments(
            String tenant,
            String namespace,
            String function,
            InputStream inputStream,
            Map<String, String> topicsToSerDeClassName,
            FormDataContentDisposition details,
            String outputTopic,
            String outputSerdeClassName,
            String className,
            Integer parallelism,
<<<<<<< HEAD
            String expectedError) {
=======
            String expectedError) throws Exception {
>>>>>>> f773c602c... Test pr 10 (#27)
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

        FunctionConfig functionConfig = new FunctionConfig();
        if (tenant != null) {
            functionConfig.setTenant(tenant);
        }
        if (namespace != null) {
            functionConfig.setNamespace(namespace);
        }
        if (function != null) {
            functionConfig.setName(function);
        }
        if (topicsToSerDeClassName != null) {
            functionConfig.setCustomSerdeInputs(topicsToSerDeClassName);
        }
        if (outputTopic != null) {
            functionConfig.setOutput(outputTopic);
        }
        if (outputSerdeClassName != null) {
            functionConfig.setOutputSerdeClassName(outputSerdeClassName);
        }
        if (className != null) {
            functionConfig.setClassName(className);
        }
        if (parallelism != null) {
            functionConfig.setParallelism(parallelism);
        }
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);

<<<<<<< HEAD
        if (expectedError == null) {
            RequestResult rr = new RequestResult()
                    .setSuccess(true)
                    .setMessage("function registered");
            CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
            when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);
=======
        if (expectedError != null) {
            doThrow(new IllegalArgumentException(expectedError))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());
>>>>>>> f773c602c... Test pr 10 (#27)
        }

        resource.updateFunction(
            tenant,
            namespace,
            function,
            inputStream,
            details,
            null,
<<<<<<< HEAD
            null,
            new Gson().toJson(functionConfig),
                null);
=======
            functionConfig,
                null, null, null);
>>>>>>> f773c602c... Test pr 10 (#27)

    }

    private void updateDefaultFunction() {
        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setTenant(tenant);
        functionConfig.setNamespace(namespace);
        functionConfig.setName(function);
        functionConfig.setClassName(className);
        functionConfig.setParallelism(parallelism);
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
        functionConfig.setCustomSerdeInputs(topicsToSerDeClassName);
        functionConfig.setOutput(outputTopic);
        functionConfig.setOutputSerdeClassName(outputSerdeClassName);

        resource.updateFunction(
            tenant,
            namespace,
            function,
            mockedInputStream,
            mockedFormData,
            null,
<<<<<<< HEAD
            null,
            new Gson().toJson(functionConfig),
                null);
=======
            functionConfig,
                null, null, null);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function test-function doesn't exist")
    public void testUpdateNotExistedFunction() {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);
            updateDefaultFunction();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "upload failure")
    public void testUpdateFunctionUploadFailure() throws Exception {
        try {
<<<<<<< HEAD
            mockStatic(Utils.class);
            doThrow(new IOException("upload failure")).when(Utils.class);
            Utils.uploadFileToBookkeeper(
                    anyString(),
                any(File.class),
                any(Namespace.class));
=======
            mockStatic(WorkerUtils.class);
            doThrow(new IOException("upload failure")).when(WorkerUtils.class);
            WorkerUtils.uploadFileToBookkeeper(
                    anyString(),
                any(File.class),
                any(Namespace.class));
            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();
>>>>>>> f773c602c... Test pr 10 (#27)

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

            updateDefaultFunction();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    @Test
    public void testUpdateFunctionSuccess() throws Exception {
<<<<<<< HEAD
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.uploadToBookeeper(
            any(Namespace.class),
            any(InputStream.class),
            anyString());

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

        RequestResult rr = new RequestResult()
            .setSuccess(true)
            .setMessage("function registered");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

=======
        mockStatic(WorkerUtils.class);
        doNothing().when(WorkerUtils.class);
        WorkerUtils.uploadToBookeeper(
            any(Namespace.class),
            any(InputStream.class),
            anyString());
        PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

>>>>>>> f773c602c... Test pr 10 (#27)
        updateDefaultFunction();
    }

    @Test
    public void testUpdateFunctionWithUrl() {
        Configurator.setRootLevel(Level.DEBUG);

        String fileLocation = FutureUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        String filePackageUrl = "file://" + fileLocation;

        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setOutput(outputTopic);
        functionConfig.setOutputSerdeClassName(outputSerdeClassName);
        functionConfig.setTenant(tenant);
        functionConfig.setNamespace(namespace);
        functionConfig.setName(function);
        functionConfig.setClassName(className);
        functionConfig.setParallelism(parallelism);
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
        functionConfig.setCustomSerdeInputs(topicsToSerDeClassName);

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);
<<<<<<< HEAD
        RequestResult rr = new RequestResult()
                .setSuccess(true)
                .setMessage("function registered");
            CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
            when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);
=======
>>>>>>> f773c602c... Test pr 10 (#27)

        resource.updateFunction(
            tenant,
            namespace,
            function,
            null,
            null,
            filePackageUrl,
<<<<<<< HEAD
            null,
            new Gson().toJson(functionConfig),
                null);
=======
            functionConfig,
                null, null, null);
>>>>>>> f773c602c... Test pr 10 (#27)

    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "function failed to register")
    public void testUpdateFunctionFailure() throws Exception {
        try {
<<<<<<< HEAD
            mockStatic(Utils.class);
            doNothing().when(Utils.class);
            Utils.uploadToBookeeper(
                any(Namespace.class),
                any(InputStream.class),
                anyString());

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

            RequestResult rr = new RequestResult()
                .setSuccess(false)
                .setMessage("function failed to register");
            CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
            when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);
=======
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.uploadToBookeeper(
                any(Namespace.class),
                any(InputStream.class),
                anyString());
            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

            doThrow(new IllegalArgumentException("function failed to register"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());
>>>>>>> f773c602c... Test pr 10 (#27)

            updateDefaultFunction();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

<<<<<<< HEAD
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "java.io.IOException: Function registeration interrupted")
    public void testUpdateFunctionInterrupted() throws Exception {
        try {
            mockStatic(Utils.class);
            doNothing().when(Utils.class);
            Utils.uploadToBookeeper(
                any(Namespace.class),
                any(InputStream.class),
                anyString());

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

            CompletableFuture<RequestResult> requestResult = FutureUtil.failedFuture(
                new IOException("Function registeration interrupted"));
            when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);
=======
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function registeration interrupted")
    public void testUpdateFunctionInterrupted() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.uploadToBookeeper(
                any(Namespace.class),
                any(InputStream.class),
                anyString());
            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

            doThrow(new IllegalStateException("Function registeration interrupted"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());
>>>>>>> f773c602c... Test pr 10 (#27)

            updateDefaultFunction();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    //
    // deregister function
    //

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
    public void testDeregisterFunctionMissingTenant() {
        try {

            testDeregisterFunctionMissingArguments(
                null,
                namespace,
                function
            );
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace is not provided")
    public void testDeregisterFunctionMissingNamespace() {
        try {
            testDeregisterFunctionMissingArguments(
                tenant,
                null,
                function
            );
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

<<<<<<< HEAD
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function Name is not provided")
=======
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function name is not provided")
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testDeregisterFunctionMissingFunctionName() {
        try {
             testDeregisterFunctionMissingArguments(
                tenant,
                namespace,
                null
            );
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    private void testDeregisterFunctionMissingArguments(
            String tenant,
            String namespace,
            String function
    ) {
        resource.deregisterFunction(
            tenant,
            namespace,
            function,
<<<<<<< HEAD
                null);
=======
                null, null);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    private void deregisterDefaultFunction() {
        resource.deregisterFunction(
            tenant,
            namespace,
            function,
<<<<<<< HEAD
                null);
=======
                null, null);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function test-function doesn't exist")
    public void testDeregisterNotExistedFunction() {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);
            deregisterDefaultFunction();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.NOT_FOUND);
            throw re;
        }
    }

    @Test
    public void testDeregisterFunctionSuccess() {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

<<<<<<< HEAD
        RequestResult rr = new RequestResult()
            .setSuccess(true)
            .setMessage("function deregistered");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.deregisterFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(requestResult);

=======
>>>>>>> f773c602c... Test pr 10 (#27)
        deregisterDefaultFunction();
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "function failed to deregister")
<<<<<<< HEAD
    public void testDeregisterFunctionFailure() {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

            RequestResult rr = new RequestResult()
                .setSuccess(false)
                .setMessage("function failed to deregister");
            CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
            when(mockedManager.deregisterFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(requestResult);
=======
    public void testDeregisterFunctionFailure() throws Exception {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

            doThrow(new IllegalArgumentException("function failed to deregister"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());
>>>>>>> f773c602c... Test pr 10 (#27)

            deregisterDefaultFunction();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function deregisteration interrupted")
<<<<<<< HEAD
    public void testDeregisterFunctionInterrupted() {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

            CompletableFuture<RequestResult> requestResult = FutureUtil.failedFuture(
                    new IOException("Function deregisteration interrupted"));
            when(mockedManager.deregisterFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(requestResult);
=======
    public void testDeregisterFunctionInterrupted() throws Exception {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

            doThrow(new IllegalStateException("Function deregisteration interrupted"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());
>>>>>>> f773c602c... Test pr 10 (#27)

            deregisterDefaultFunction();
        }
        catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    //
    // Get Function Info
    //

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
    public void testGetFunctionMissingTenant() {
        try {
            testGetFunctionMissingArguments(
                null,
                namespace,
                function
            );
        }
        catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace is not provided")
    public void testGetFunctionMissingNamespace() {
        try {
            testGetFunctionMissingArguments(
                tenant,
                null,
                function
            );
        }
        catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

<<<<<<< HEAD
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function Name is not provided")
=======
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function name is not provided")
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testGetFunctionMissingFunctionName() {
        try {
            testGetFunctionMissingArguments(
                tenant,
                namespace,
                null
            );
        }
        catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    private void testGetFunctionMissingArguments(
            String tenant,
            String namespace,
            String function
    ) {
        resource.getFunctionInfo(
            tenant,
            namespace,
<<<<<<< HEAD
            function
=======
            function,null,null
>>>>>>> f773c602c... Test pr 10 (#27)
        );

    }

    private FunctionConfig getDefaultFunctionInfo() {
        return resource.getFunctionInfo(
            tenant,
            namespace,
<<<<<<< HEAD
            function
=======
            function,
                null,
                null
>>>>>>> f773c602c... Test pr 10 (#27)
        );
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function test-function doesn't exist")
    public void testGetNotExistedFunction() {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);
            getDefaultFunctionInfo();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.NOT_FOUND);
            throw re;
        }
    }

    @Test
    public void testGetFunctionSuccess() {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

        SinkSpec sinkSpec = SinkSpec.newBuilder()
                .setTopic(outputTopic)
                .setSerDeClassName(outputSerdeClassName).build();
        FunctionDetails functionDetails = FunctionDetails.newBuilder()
                .setClassName(className)
                .setSink(sinkSpec)
                .setName(function)
                .setNamespace(namespace)
                .setProcessingGuarantees(ProcessingGuarantees.ATMOST_ONCE)
                .setTenant(tenant)
                .setParallelism(parallelism)
                .setSource(SourceSpec.newBuilder().setSubscriptionType(subscriptionType)
                        .putAllTopicsToSerDeClassName(topicsToSerDeClassName)).build();
        FunctionMetaData metaData = FunctionMetaData.newBuilder()
            .setCreateTime(System.currentTimeMillis())
            .setFunctionDetails(functionDetails)
            .setPackageLocation(PackageLocationMetaData.newBuilder().setPackagePath("/path/to/package"))
            .setVersion(1234)
            .build();
        when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(function))).thenReturn(metaData);

        FunctionConfig functionConfig = getDefaultFunctionInfo();
        assertEquals(
                FunctionConfigUtils.convertFromDetails(functionDetails),
                functionConfig);
    }

    //
    // List Functions
    //

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
    public void testListFunctionsMissingTenant() {
        try {
            testListFunctionsMissingArguments(
                null,
                namespace
            );
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace is not provided")
    public void testListFunctionsMissingNamespace() {
        try {
            testListFunctionsMissingArguments(
                tenant,
                null
            );
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    private void testListFunctionsMissingArguments(
            String tenant,
            String namespace
    ) {
        resource.listFunctions(
            tenant,
<<<<<<< HEAD
            namespace
=======
            namespace,null,null
>>>>>>> f773c602c... Test pr 10 (#27)
        );

    }

    private List<String> listDefaultFunctions() {
        return resource.listFunctions(
            tenant,
<<<<<<< HEAD
            namespace
=======
            namespace,null,null
>>>>>>> f773c602c... Test pr 10 (#27)
        );
    }

    @Test
    public void testListFunctionsSuccess() {
        final List<String> functions = Lists.newArrayList("test-1", "test-2");
        final List<FunctionMetaData> metaDataList = new LinkedList<>();
        FunctionMetaData functionMetaData1 = FunctionMetaData.newBuilder().setFunctionDetails(
                FunctionDetails.newBuilder().setName("test-1").build()
        ).build();
        FunctionMetaData functionMetaData2 = FunctionMetaData.newBuilder().setFunctionDetails(
                FunctionDetails.newBuilder().setName("test-2").build()
        ).build();
        metaDataList.add(functionMetaData1);
        metaDataList.add(functionMetaData2);
        when(mockedManager.listFunctions(eq(tenant), eq(namespace))).thenReturn(metaDataList);

        List<String> functionList = listDefaultFunctions();
        assertEquals(functions, functionList);
    }

    @Test
    public void testOnlyGetSources() {
        List<String> functions = Lists.newArrayList("test-2");
        List<FunctionMetaData> functionMetaDataList = new LinkedList<>();
        FunctionMetaData f1 = FunctionMetaData.newBuilder().setFunctionDetails(
                FunctionDetails.newBuilder().setName("test-1").build()).build();
        functionMetaDataList.add(f1);
        FunctionMetaData f2 = FunctionMetaData.newBuilder().setFunctionDetails(
                FunctionDetails.newBuilder().setName("test-2").build()).build();
        functionMetaDataList.add(f2);
        FunctionMetaData f3 = FunctionMetaData.newBuilder().setFunctionDetails(
                FunctionDetails.newBuilder().setName("test-3").build()).build();
        functionMetaDataList.add(f3);
        when(mockedManager.listFunctions(eq(tenant), eq(namespace))).thenReturn(functionMetaDataList);
<<<<<<< HEAD
        doReturn(SOURCE).when(this.resource).calculateSubjectType(f1);
        doReturn(FUNCTION).when(this.resource).calculateSubjectType(f2);
        doReturn(SINK).when(this.resource).calculateSubjectType(f3);
=======

        mockStatic(InstanceUtils.class);
        PowerMockito.when(InstanceUtils.calculateSubjectType(f1.getFunctionDetails())).thenReturn(FunctionDetails.ComponentType.SOURCE);
        PowerMockito.when(InstanceUtils.calculateSubjectType(f2.getFunctionDetails())).thenReturn(FunctionDetails.ComponentType.FUNCTION);
        PowerMockito.when(InstanceUtils.calculateSubjectType(f3.getFunctionDetails())).thenReturn(FunctionDetails.ComponentType.SINK);
>>>>>>> f773c602c... Test pr 10 (#27)

        List<String> functionList = listDefaultFunctions();
        assertEquals(functions, functionList);
    }

    @Test
    public void testDownloadFunctionHttpUrl() throws Exception {
<<<<<<< HEAD
        String jarHttpUrl = "http://central.maven.org/maven2/org/apache/pulsar/pulsar-common/1.22.0-incubating/pulsar-common-1.22.0-incubating.jar";
        String testDir = FunctionApiV3ResourceTest.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        FunctionsImpl function = new FunctionsImpl(null);
        StreamingOutput streamOutput = function.downloadFunction(jarHttpUrl);
=======
        String jarHttpUrl = "https://repo1.maven.org/maven2/org/apache/pulsar/pulsar-common/2.4.2/pulsar-common-2.4.2.jar";
        String testDir = FunctionApiV3ResourceTest.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        WorkerService worker = mock(WorkerService.class);
        doReturn(true).when(worker).isInitialized();
        WorkerConfig config = mock(WorkerConfig.class);
        when(config.isAuthorizationEnabled()).thenReturn(false);
        when(worker.getWorkerConfig()).thenReturn(config);
        FunctionsImpl function = new FunctionsImpl(()-> worker);
        StreamingOutput streamOutput = function.downloadFunction(jarHttpUrl, null, null);
>>>>>>> f773c602c... Test pr 10 (#27)
        File pkgFile = new File(testDir, UUID.randomUUID().toString());
        OutputStream output = new FileOutputStream(pkgFile);
        streamOutput.write(output);
        Assert.assertTrue(pkgFile.exists());
        if (pkgFile.exists()) {
            pkgFile.delete();
        }
    }

    @Test
    public void testDownloadFunctionFile() throws Exception {
<<<<<<< HEAD
        String fileLocation = FutureUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        String testDir = FunctionApiV3ResourceTest.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        FunctionsImpl function = new FunctionsImpl(null);
        StreamingOutput streamOutput = function.downloadFunction("file://" + fileLocation);
=======
        URL fileUrl = getClass().getClassLoader().getResource("test_worker_config.yml");
        File file = Paths.get(fileUrl.toURI()).toFile();
        String fileLocation = file.getAbsolutePath();
        String testDir = FunctionApiV3ResourceTest.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        WorkerService worker = mock(WorkerService.class);
        doReturn(true).when(worker).isInitialized();
        WorkerConfig config = mock(WorkerConfig.class);
        when(config.isAuthorizationEnabled()).thenReturn(false);
        when(worker.getWorkerConfig()).thenReturn(config);
        FunctionsImpl function = new FunctionsImpl(() -> worker);
        StreamingOutput streamOutput = function.downloadFunction("file://" + fileLocation, null, null);
>>>>>>> f773c602c... Test pr 10 (#27)
        File pkgFile = new File(testDir, UUID.randomUUID().toString());
        OutputStream output = new FileOutputStream(pkgFile);
        streamOutput.write(output);
        Assert.assertTrue(pkgFile.exists());
        if (pkgFile.exists()) {
            pkgFile.delete();
        }
    }

    @Test
<<<<<<< HEAD
    public void testRegisterFunctionFileUrlWithValidSinkClass() {
        Configurator.setRootLevel(Level.DEBUG);

        String fileLocation = FutureUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        String filePackageUrl = "file://" + fileLocation;
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);

        RequestResult rr = new RequestResult().setSuccess(true).setMessage("function registered");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

=======
    public void testRegisterFunctionFileUrlWithValidSinkClass() throws Exception {
        Configurator.setRootLevel(Level.DEBUG);

        URL fileUrl = getClass().getClassLoader().getResource("test_worker_config.yml");
        File file = Paths.get(fileUrl.toURI()).toFile();
        String fileLocation = file.getAbsolutePath();
        String filePackageUrl = "file://" + fileLocation;
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);

>>>>>>> f773c602c... Test pr 10 (#27)
        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setTenant(tenant);
        functionConfig.setNamespace(namespace);
        functionConfig.setName(function);
        functionConfig.setClassName(className);
        functionConfig.setParallelism(parallelism);
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
        functionConfig.setCustomSerdeInputs(topicsToSerDeClassName);
        functionConfig.setOutput(outputTopic);
        functionConfig.setOutputSerdeClassName(outputSerdeClassName);
<<<<<<< HEAD
        resource.registerFunction(tenant, namespace, function, null, null, filePackageUrl,
                null, new Gson().toJson(functionConfig), null);
=======
        resource.registerFunction(tenant, namespace, function, null, null, filePackageUrl, functionConfig, null, null);
>>>>>>> f773c602c... Test pr 10 (#27)

    }

    @Test
<<<<<<< HEAD
    public void testRegisterFunctionWithConflictingFields() {
=======
    public void testRegisterFunctionWithConflictingFields() throws Exception {
>>>>>>> f773c602c... Test pr 10 (#27)
        Configurator.setRootLevel(Level.DEBUG);
        String actualTenant = "DIFFERENT_TENANT";
        String actualNamespace = "DIFFERENT_NAMESPACE";
        String actualName = "DIFFERENT_NAME";
        this.namespaceList.add(actualTenant + "/" + actualNamespace);

<<<<<<< HEAD
        String fileLocation = FutureUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath();
=======
        URL fileUrl = getClass().getClassLoader().getResource("test_worker_config.yml");
        File file = Paths.get(fileUrl.toURI()).toFile();
        String fileLocation = file.getAbsolutePath();
>>>>>>> f773c602c... Test pr 10 (#27)
        String filePackageUrl = "file://" + fileLocation;
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);
        when(mockedManager.containsFunction(eq(actualTenant), eq(actualNamespace), eq(actualName))).thenReturn(false);

<<<<<<< HEAD
        RequestResult rr = new RequestResult().setSuccess(true).setMessage("function registered");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

=======
>>>>>>> f773c602c... Test pr 10 (#27)
        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setTenant(tenant);
        functionConfig.setNamespace(namespace);
        functionConfig.setName(function);
        functionConfig.setClassName(className);
        functionConfig.setParallelism(parallelism);
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
        functionConfig.setCustomSerdeInputs(topicsToSerDeClassName);
        functionConfig.setOutput(outputTopic);
        functionConfig.setOutputSerdeClassName(outputSerdeClassName);
<<<<<<< HEAD
        resource.registerFunction(actualTenant, actualNamespace, actualName, null, null, filePackageUrl,
                null, new Gson().toJson(functionConfig), null);
=======
        resource.registerFunction(actualTenant, actualNamespace, actualName, null, null, filePackageUrl, functionConfig, null, null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function language runtime is either not set or cannot be determined")
    public void testCreateFunctionWithoutSettingRuntime() throws Exception {
        Configurator.setRootLevel(Level.DEBUG);

        URL fileUrl = getClass().getClassLoader().getResource("test_worker_config.yml");
        File file = Paths.get(fileUrl.toURI()).toFile();
        String fileLocation = file.getAbsolutePath();
        String filePackageUrl = "file://" + fileLocation;
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);

        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setTenant(tenant);
        functionConfig.setNamespace(namespace);
        functionConfig.setName(function);
        functionConfig.setClassName(className);
        functionConfig.setParallelism(parallelism);
        functionConfig.setCustomSerdeInputs(topicsToSerDeClassName);
        functionConfig.setOutput(outputTopic);
        functionConfig.setOutputSerdeClassName(outputSerdeClassName);
        resource.registerFunction(tenant, namespace, function, null, null, filePackageUrl, functionConfig, null, null);

>>>>>>> f773c602c... Test pr 10 (#27)
    }

    public static FunctionConfig createDefaultFunctionConfig() {
        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setTenant(tenant);
        functionConfig.setNamespace(namespace);
        functionConfig.setName(function);
        functionConfig.setClassName(className);
        functionConfig.setParallelism(parallelism);
        functionConfig.setCustomSerdeInputs(topicsToSerDeClassName);
        functionConfig.setOutput(outputTopic);
        functionConfig.setOutputSerdeClassName(outputSerdeClassName);
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
        return functionConfig;
    }

    public static FunctionDetails createDefaultFunctionDetails() {
        FunctionConfig functionConfig = createDefaultFunctionConfig();
        return FunctionConfigUtils.convert(functionConfig, null);
    }
}
