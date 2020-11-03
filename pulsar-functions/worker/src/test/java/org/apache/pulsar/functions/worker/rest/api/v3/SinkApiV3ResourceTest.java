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

import com.google.common.collect.Lists;
<<<<<<< HEAD
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
=======
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.pulsar.client.admin.Functions;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.policies.data.TenantInfo;
<<<<<<< HEAD
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.functions.api.utils.IdentityFunction;
=======
import org.apache.pulsar.common.util.ClassLoaderUtils;
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.functions.api.utils.IdentityFunction;
import org.apache.pulsar.functions.instance.InstanceUtils;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.Function.FunctionMetaData;
import org.apache.pulsar.functions.runtime.RuntimeFactory;
<<<<<<< HEAD
import org.apache.pulsar.functions.utils.SinkConfigUtils;
import org.apache.pulsar.functions.utils.io.ConnectorUtils;
import org.apache.pulsar.functions.worker.FunctionMetaDataManager;
import org.apache.pulsar.functions.worker.FunctionRuntimeManager;
import org.apache.pulsar.functions.worker.Utils;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.request.RequestResult;
import org.apache.pulsar.functions.worker.rest.RestException;
import org.apache.pulsar.functions.worker.rest.api.SinkImpl;
import org.apache.pulsar.io.cassandra.CassandraStringSink;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.mockito.Mockito;
=======
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.utils.SinkConfigUtils;
import org.apache.pulsar.functions.utils.io.ConnectorUtils;
import org.apache.pulsar.functions.worker.*;
import org.apache.pulsar.functions.worker.rest.api.SinksImpl;
import org.apache.pulsar.io.cassandra.CassandraStringSink;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.IObjectFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
<<<<<<< HEAD
import java.nio.file.Path;
=======
>>>>>>> f773c602c... Test pr 10 (#27)
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
<<<<<<< HEAD
import java.util.concurrent.CompletableFuture;

import static org.apache.pulsar.functions.proto.Function.ProcessingGuarantees.ATLEAST_ONCE;
import static org.apache.pulsar.functions.source.TopicSchema.DEFAULT_SERDE;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
=======

import static org.apache.pulsar.functions.proto.Function.ProcessingGuarantees.ATLEAST_ONCE;
import static org.apache.pulsar.functions.source.TopicSchema.DEFAULT_SERDE;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
>>>>>>> f773c602c... Test pr 10 (#27)
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.doReturn;
import static org.powermock.api.mockito.PowerMockito.doThrow;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.testng.Assert.assertEquals;

/**
<<<<<<< HEAD
 * Unit test of {@link SinkApiV3Resource}.
 */
@PrepareForTest({Utils.class, SinkConfigUtils.class, ConnectorUtils.class, org.apache.pulsar.functions.utils.Utils.class})
@PowerMockIgnore({ "javax.management.*", "javax.ws.*", "org.apache.logging.log4j.*", "org.apache.pulsar.io.*" })
@Slf4j
=======
 * Unit test of {@link SinksApiV3Resource}.
 */
@PrepareForTest({WorkerUtils.class, SinkConfigUtils.class, ConnectorUtils.class, FunctionCommon.class, ClassLoaderUtils.class, InstanceUtils.class})
@PowerMockIgnore({ "javax.management.*", "javax.ws.*", "org.apache.logging.log4j.*", "org.apache.pulsar.io.*", "java.io.*" })

>>>>>>> f773c602c... Test pr 10 (#27)
public class SinkApiV3ResourceTest {

    @ObjectFactory
    public IObjectFactory getObjectFactory() {
        return new org.powermock.modules.testng.PowerMockObjectFactory();
    }

    private static final String tenant = "test-tenant";
    private static final String namespace = "test-namespace";
    private static final String sink = "test-sink";
    private static final Map<String, String> topicsToSerDeClassName = new HashMap<>();
    static {
        topicsToSerDeClassName.put("persistent://sample/standalone/ns1/test_src", DEFAULT_SERDE);
    }
    private static final String subscriptionName = "test-subscription";
    private static final String className = CassandraStringSink.class.getName();
    private static final int parallelism = 1;
    private static final String JAR_FILE_NAME = "pulsar-io-cassandra.nar";
    private static final String INVALID_JAR_FILE_NAME = "pulsar-io-twitter.nar";
    private String JAR_FILE_PATH;
    private String INVALID_JAR_FILE_PATH;

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
<<<<<<< HEAD
    private SinkImpl resource;
    private InputStream mockedInputStream;
    private FormDataContentDisposition mockedFormData;
    private FunctionMetaData mockedFunctionMetaData;
=======
    private SinksImpl resource;
    private InputStream mockedInputStream;
    private FormDataContentDisposition mockedFormData;
    private FunctionMetaData mockedFunctionMetaData;
    private LeaderService mockedLeaderService;
>>>>>>> f773c602c... Test pr 10 (#27)

    @BeforeMethod
    public void setup() throws Exception {
        this.mockedManager = mock(FunctionMetaDataManager.class);
        this.mockedFunctionRunTimeManager = mock(FunctionRuntimeManager.class);
        this.mockedRuntimeFactory = mock(RuntimeFactory.class);
        this.mockedInputStream = mock(InputStream.class);
        this.mockedNamespace = mock(Namespace.class);
        this.mockedFormData = mock(FormDataContentDisposition.class);
        when(mockedFormData.getFileName()).thenReturn("test");
        this.mockedTenantInfo = mock(TenantInfo.class);
        this.mockedPulsarAdmin = mock(PulsarAdmin.class);
        this.mockedTenants = mock(Tenants.class);
        this.mockedNamespaces = mock(Namespaces.class);
<<<<<<< HEAD
=======
        this.mockedFunctions = mock(Functions.class);
        this.mockedLeaderService = mock(LeaderService.class);
>>>>>>> f773c602c... Test pr 10 (#27)
        namespaceList.add(tenant + "/" + namespace);

        this.mockedWorkerService = mock(WorkerService.class);
        when(mockedWorkerService.getFunctionMetaDataManager()).thenReturn(mockedManager);
<<<<<<< HEAD
=======
        when(mockedWorkerService.getLeaderService()).thenReturn(mockedLeaderService);
>>>>>>> f773c602c... Test pr 10 (#27)
        when(mockedWorkerService.getFunctionRuntimeManager()).thenReturn(mockedFunctionRunTimeManager);
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

        URL file = Thread.currentThread().getContextClassLoader().getResource(JAR_FILE_NAME);
        if (file == null)  {
            throw new RuntimeException("Failed to file required test archive: " + JAR_FILE_NAME);
        }
        JAR_FILE_PATH = file.getFile();
        INVALID_JAR_FILE_PATH = Thread.currentThread().getContextClassLoader().getResource(INVALID_JAR_FILE_NAME).getFile();

<<<<<<< HEAD

=======
>>>>>>> f773c602c... Test pr 10 (#27)
        // worker config
        WorkerConfig workerConfig = new WorkerConfig()
            .setWorkerId("test")
            .setWorkerPort(8080)
            .setDownloadDirectory("/tmp/pulsar/functions")
            .setFunctionMetadataTopicName("pulsar/functions")
            .setNumFunctionPackageReplicas(3)
            .setPulsarServiceUrl("pulsar://localhost:6650/");
        when(mockedWorkerService.getWorkerConfig()).thenReturn(workerConfig);

<<<<<<< HEAD
        this.resource = spy(new SinkImpl(() -> mockedWorkerService));
        Mockito.doReturn(org.apache.pulsar.functions.utils.Utils.ComponentType.SINK).when(this.resource).calculateSubjectType(any());
=======
        this.resource = spy(new SinksImpl(() -> mockedWorkerService));
        mockStatic(InstanceUtils.class);
        PowerMockito.when(InstanceUtils.calculateSubjectType(any())).thenReturn(FunctionDetails.ComponentType.SINK);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    //
    // Register Functions
    //

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
    public void testRegisterSinkMissingTenant() {
        try {
            testRegisterSinkMissingArguments(
                null,
                namespace,
                    sink,
                mockedInputStream,
                mockedFormData,
                topicsToSerDeClassName,
                className,
                parallelism,
                    null
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace is not provided")
    public void testRegisterSinkMissingNamespace() {
        try {
            testRegisterSinkMissingArguments(
                tenant,
                null,
                    sink,
                mockedInputStream,
                mockedFormData,
                topicsToSerDeClassName,
                className,
                parallelism,
                    null
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

<<<<<<< HEAD
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Sink Name is not provided")
    public void testRegisterSinkMissingFunctionName() {
=======
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Sink name is not provided")
    public void testRegisterSinkMissingSinkName() {
>>>>>>> f773c602c... Test pr 10 (#27)
        try {
            testRegisterSinkMissingArguments(
                tenant,
                namespace,
                null,
                mockedInputStream,
                mockedFormData,
                topicsToSerDeClassName,
                className,
                parallelism,
                    null);
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

<<<<<<< HEAD
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Sink Package is not provided")
=======
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Sink package is not provided")
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testRegisterSinkMissingPackage() {
        try {
            testRegisterSinkMissingArguments(
            tenant,
            namespace,
                sink,
            null,
            mockedFormData,
            topicsToSerDeClassName,
            null,
            parallelism,
                null
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

<<<<<<< HEAD
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "zip file is empty")
=======
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Sink class UnknownClass must be in class path")
    public void testRegisterSinkWrongClassName() {
            try {
                testRegisterSinkMissingArguments(
                        tenant,
                        namespace,
                        sink,
                        mockedInputStream,
                        mockedFormData,
                        topicsToSerDeClassName,
                        "UnknownClass",
                        parallelism,
                        null
                );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Sink package does not have the" +
            " correct format. Pulsar cannot determine if the package is a NAR package" +
            " or JAR package.Sink classname is not provided and attempts to load it as a NAR package produced error: " +
            "zip file is empty")
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testRegisterSinkMissingPackageDetails() {
        try {
            testRegisterSinkMissingArguments(
            tenant,
            namespace,
                sink,
            mockedInputStream,
            null,
            topicsToSerDeClassName,
            null,
            parallelism,
                null
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

<<<<<<< HEAD
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Failed to extract sink class from archive")
=======
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Failed to extract Sink class from archive")
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testRegisterSinkInvalidJarNoSink() throws IOException {
        try {
            FileInputStream inputStream = new FileInputStream(INVALID_JAR_FILE_PATH);
            testRegisterSinkMissingArguments(
                tenant,
                namespace,
                sink,
                inputStream,
                mockedFormData,
                topicsToSerDeClassName,
                null,
                parallelism,
                null
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Must specify at least one topic of input via topicToSerdeClassName, topicsPattern, topicToSchemaType or inputSpecs")
    public void testRegisterSinkNoInput() {
        try {
            testRegisterSinkMissingArguments(
                tenant,
                namespace,
                sink,
                mockedInputStream,
                mockedFormData,
                null,
                className,
                parallelism,
                null
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

<<<<<<< HEAD
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Sink parallelism should positive number")
=======
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Sink parallelism must be a positive number")
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testRegisterSinkNegativeParallelism() {
        try {
            testRegisterSinkMissingArguments(
                tenant,
                namespace,
                sink,
                mockedInputStream,
                mockedFormData,
                topicsToSerDeClassName,
                className,
                -2,
                null
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

<<<<<<< HEAD
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Sink parallelism should positive number")
=======
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Sink parallelism must be a positive number")
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testRegisterSinkZeroParallelism() {
        try {
            testRegisterSinkMissingArguments(
                tenant,
                namespace,
                sink,
                mockedInputStream,
                mockedFormData,
                topicsToSerDeClassName,
                className,
                0,
                null
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

<<<<<<< HEAD
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Invalid Sink Jar")
=======
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Encountered error .*. when getting Sink package from .*")
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testRegisterSinkHttpUrl() {
        try {
            testRegisterSinkMissingArguments(
                tenant,
                namespace,
                sink,
                null,
                null,
                topicsToSerDeClassName,
                className,
                parallelism,
                "http://localhost:1234/test"
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    private void testRegisterSinkMissingArguments(
            String tenant,
            String namespace,
            String sink,
            InputStream inputStream,
            FormDataContentDisposition details,
            Map<String, String> inputTopicMap,
            String className,
            Integer parallelism,
            String pkgUrl) {
        SinkConfig sinkConfig = new SinkConfig();
        if (tenant != null) {
            sinkConfig.setTenant(tenant);
        }
        if (namespace != null) {
            sinkConfig.setNamespace(namespace);
        }
        if (sink != null) {
            sinkConfig.setName(sink);
        }
        if (inputTopicMap != null) {
            sinkConfig.setTopicToSerdeClassName(inputTopicMap);
        }
        if (className != null) {
            sinkConfig.setClassName(className);
        }
        if (parallelism != null) {
            sinkConfig.setParallelism(parallelism);
        }

<<<<<<< HEAD
        resource.registerFunction(
=======
        resource.registerSink(
>>>>>>> f773c602c... Test pr 10 (#27)
                tenant,
                namespace,
                sink,
                inputStream,
                details,
                pkgUrl,
<<<<<<< HEAD
                null,
                new Gson().toJson(sinkConfig),
                null);

=======
                sinkConfig,
                null, null);

    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Sink config is not provided")
    public void testMissingSinkConfig() {
        resource.registerSink(
                tenant,
                namespace,
                sink,
                mockedInputStream,
                mockedFormData,
                null,
                null,
                null, null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Sink config is not provided")
    public void testUpdateMissingSinkConfig() {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);
        resource.updateSink(
                tenant,
                namespace,
                sink,
                mockedInputStream,
                mockedFormData,
                null,
                null,
                null, null, null);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    private void registerDefaultSink() throws IOException {
        SinkConfig sinkConfig = createDefaultSinkConfig();
<<<<<<< HEAD
        resource.registerFunction(
=======
        resource.registerSink(
>>>>>>> f773c602c... Test pr 10 (#27)
            tenant,
            namespace,
                sink,
                new FileInputStream(JAR_FILE_PATH),
            mockedFormData,
            null,
<<<<<<< HEAD
            null,
            new Gson().toJson(sinkConfig),
                null);
=======
            sinkConfig,
                null, null);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Sink test-sink already exists")
    public void testRegisterExistedSink() throws IOException {
        try {
            Configurator.setRootLevel(Level.DEBUG);

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);

            registerDefaultSink();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "upload failure")
    public void testRegisterSinkUploadFailure() throws Exception {
        try {
<<<<<<< HEAD
            mockStatic(Utils.class);
            doThrow(new IOException("upload failure")).when(Utils.class);
            Utils.uploadFileToBookkeeper(
=======
            mockStatic(WorkerUtils.class);
            doThrow(new IOException("upload failure")).when(WorkerUtils.class);
            WorkerUtils.uploadFileToBookkeeper(
>>>>>>> f773c602c... Test pr 10 (#27)
                    anyString(),
                any(File.class),
                any(Namespace.class));

<<<<<<< HEAD
=======
            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

>>>>>>> f773c602c... Test pr 10 (#27)
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(false);

            registerDefaultSink();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    @Test
    public void testRegisterSinkSuccess() throws Exception {
<<<<<<< HEAD
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.uploadFileToBookkeeper(
=======
        mockStatic(WorkerUtils.class);
        doNothing().when(WorkerUtils.class);
        WorkerUtils.uploadFileToBookkeeper(
>>>>>>> f773c602c... Test pr 10 (#27)
                anyString(),
                any(File.class),
                any(Namespace.class));

<<<<<<< HEAD
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(false);

        RequestResult rr = new RequestResult()
            .setSuccess(true)
            .setMessage("source registered");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);
=======
        PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(false);
>>>>>>> f773c602c... Test pr 10 (#27)

        registerDefaultSink();
    }

    @Test
    public void testRegisterSinkConflictingFields() throws Exception {
<<<<<<< HEAD
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.uploadFileToBookkeeper(
                anyString(),
                any(File.class),
                any(Namespace.class));
=======
        mockStatic(WorkerUtils.class);
        doNothing().when(WorkerUtils.class);
        WorkerUtils.uploadFileToBookkeeper(
                anyString(),
                any(File.class),
                any(Namespace.class));

        PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

>>>>>>> f773c602c... Test pr 10 (#27)
        String actualTenant = "DIFFERENT_TENANT";
        String actualNamespace = "DIFFERENT_NAMESPACE";
        String actualName = "DIFFERENT_NAME";
        this.namespaceList.add(actualTenant + "/" + actualNamespace);

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);
        when(mockedManager.containsFunction(eq(actualTenant), eq(actualNamespace), eq(actualName))).thenReturn(false);

<<<<<<< HEAD
        RequestResult rr = new RequestResult()
                .setSuccess(true)
                .setMessage("source registered");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

=======
>>>>>>> f773c602c... Test pr 10 (#27)
        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setTenant(tenant);
        sinkConfig.setNamespace(namespace);
        sinkConfig.setName(sink);
        sinkConfig.setClassName(className);
        sinkConfig.setParallelism(parallelism);
        sinkConfig.setTopicToSerdeClassName(topicsToSerDeClassName);
<<<<<<< HEAD
        resource.registerFunction(
=======
        resource.registerSink(
>>>>>>> f773c602c... Test pr 10 (#27)
                actualTenant,
                actualNamespace,
                actualName,
                new FileInputStream(JAR_FILE_PATH),
                mockedFormData,
                null,
<<<<<<< HEAD
                null,
                new Gson().toJson(sinkConfig),
                null);
=======
                sinkConfig,
                null, null);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "sink failed to register")
    public void testRegisterSinkFailure() throws Exception {
        try {
<<<<<<< HEAD
            mockStatic(Utils.class);
            doNothing().when(Utils.class);
            Utils.uploadFileToBookkeeper(
=======
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.uploadFileToBookkeeper(
>>>>>>> f773c602c... Test pr 10 (#27)
                    anyString(),
                    any(File.class),
                    any(Namespace.class));

<<<<<<< HEAD
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(false);

            RequestResult rr = new RequestResult()
                .setSuccess(false)
                .setMessage("sink failed to register");
            CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
            when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);
=======
            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(false);

            doThrow(new IllegalArgumentException("sink failed to register"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());
>>>>>>> f773c602c... Test pr 10 (#27)

            registerDefaultSink();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

<<<<<<< HEAD
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "java.io.IOException: Function registeration interrupted")
    public void testRegisterSinkInterrupted() throws Exception {
        try {
            mockStatic(Utils.class);
            doNothing().when(Utils.class);
            Utils.uploadFileToBookkeeper(
=======
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function registration interrupted")
    public void testRegisterSinkInterrupted() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.uploadFileToBookkeeper(
>>>>>>> f773c602c... Test pr 10 (#27)
                    anyString(),
                    any(File.class),
                    any(Namespace.class));

<<<<<<< HEAD
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(false);

            CompletableFuture<RequestResult> requestResult = FutureUtil.failedFuture(
                new IOException("Function registeration interrupted"));
            when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);
=======
            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(false);

            doThrow(new IllegalStateException("Function registration interrupted"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());
>>>>>>> f773c602c... Test pr 10 (#27)

            registerDefaultSink();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    //
    // Update Functions
    //

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
<<<<<<< HEAD
    public void testUpdateSinkMissingTenant() throws IOException {
=======
    public void testUpdateSinkMissingTenant() throws Exception {
>>>>>>> f773c602c... Test pr 10 (#27)
        try {
            testUpdateSinkMissingArguments(
            null,
            namespace,
                sink,
            mockedInputStream,
            mockedFormData,
            topicsToSerDeClassName,
            className,
            parallelism,
                "Tenant is not provided");
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace is not provided")
<<<<<<< HEAD
    public void testUpdateSinkMissingNamespace() throws IOException {
=======
    public void testUpdateSinkMissingNamespace() throws Exception {
>>>>>>> f773c602c... Test pr 10 (#27)
        try {
            testUpdateSinkMissingArguments(
            tenant,
            null,
                sink,
            mockedInputStream,
            mockedFormData,
            topicsToSerDeClassName,
            className,
            parallelism,
                "Namespace is not provided");
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

<<<<<<< HEAD
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Sink Name is not provided")
    public void testUpdateSinkMissingFunctionName() throws IOException {
=======
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Sink name is not provided")
    public void testUpdateSinkMissingFunctionName() throws Exception {
>>>>>>> f773c602c... Test pr 10 (#27)
        try {
            testUpdateSinkMissingArguments(
            tenant,
            namespace,
            null,
            mockedInputStream,
            mockedFormData,
            topicsToSerDeClassName,
            className,
            parallelism,
<<<<<<< HEAD
                "Sink Name is not provided");
=======
                "Sink name is not provided");
>>>>>>> f773c602c... Test pr 10 (#27)
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Update contains no change")
<<<<<<< HEAD
    public void testUpdateSinkMissingPackage() throws IOException {
        try {
            mockStatic(Utils.class);
            doNothing().when(Utils.class);
            Utils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());
=======
    public void testUpdateSinkMissingPackage() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());

            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();
>>>>>>> f773c602c... Test pr 10 (#27)

            testUpdateSinkMissingArguments(
                tenant,
                namespace,
                    sink,
                null,
                mockedFormData,
                topicsToSerDeClassName,
                null,
                parallelism,
                    "Update contains no change");
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Update contains no change")
<<<<<<< HEAD
    public void testUpdateSinkMissingInputs() throws IOException {
        try {
            mockStatic(Utils.class);
            doNothing().when(Utils.class);
            Utils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());
=======
    public void testUpdateSinkMissingInputs() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());

            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();
>>>>>>> f773c602c... Test pr 10 (#27)

            testUpdateSinkMissingArguments(
                tenant,
                namespace,
                sink,
                null,
                mockedFormData,
                null,
                null,
                parallelism,
                "Update contains no change");
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Input Topics cannot be altered")
<<<<<<< HEAD
    public void testUpdateSinkDifferentInputs() throws IOException {
        try {
            mockStatic(Utils.class);
            doNothing().when(Utils.class);
            Utils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());
=======
    public void testUpdateSinkDifferentInputs() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());

            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();
>>>>>>> f773c602c... Test pr 10 (#27)

            Map<String, String> inputTopics = new HashMap<>();
            inputTopics.put("DifferntTopic", DEFAULT_SERDE);
            testUpdateSinkMissingArguments(
                tenant,
                namespace,
                sink,
                null,
                mockedFormData,
                inputTopics,
                className,
                parallelism,
                "Input Topics cannot be altered");
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test
<<<<<<< HEAD
    public void testUpdateSinkDifferentParallelism() throws IOException {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());
=======
    public void testUpdateSinkDifferentParallelism() throws Exception {
        mockStatic(WorkerUtils.class);
        doNothing().when(WorkerUtils.class);
        WorkerUtils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());

        PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();
>>>>>>> f773c602c... Test pr 10 (#27)

        testUpdateSinkMissingArguments(
                tenant,
                namespace,
                sink,
                null,
                mockedFormData,
                topicsToSerDeClassName,
                className,
                parallelism + 1,
                null);
    }

    private void testUpdateSinkMissingArguments(
            String tenant,
            String namespace,
            String sink,
            InputStream inputStream,
            FormDataContentDisposition details,
            Map<String, String> inputTopicsMap,
            String className,
            Integer parallelism,
<<<<<<< HEAD
            String expectedError) throws IOException {
=======
            String expectedError) throws Exception {
>>>>>>> f773c602c... Test pr 10 (#27)
        mockStatic(ConnectorUtils.class);
        doReturn(CassandraStringSink.class.getName()).when(ConnectorUtils.class);
        ConnectorUtils.getIOSinkClass(any(NarClassLoader.class));

<<<<<<< HEAD
        mockStatic(org.apache.pulsar.functions.utils.Utils.class);
        doReturn(String.class).when(org.apache.pulsar.functions.utils.Utils.class);
        org.apache.pulsar.functions.utils.Utils.getSinkType(anyString(), any(NarClassLoader.class));

        doReturn(mock(NarClassLoader.class)).when(org.apache.pulsar.functions.utils.Utils.class);
        org.apache.pulsar.functions.utils.Utils.extractNarClassLoader(any(Path.class), anyString(), any(File.class));

        doReturn(ATLEAST_ONCE).when(org.apache.pulsar.functions.utils.Utils.class);
        org.apache.pulsar.functions.utils.Utils.convertProcessingGuarantee(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
=======
        mockStatic(ClassLoaderUtils.class);

        mockStatic(FunctionCommon.class);
        PowerMockito.when(FunctionCommon.class, "createPkgTempFile").thenCallRealMethod();

        doReturn(String.class).when(FunctionCommon.class);
        FunctionCommon.getSinkType(anyString(), any(NarClassLoader.class));

        doReturn(mock(NarClassLoader.class)).when(FunctionCommon.class);
        FunctionCommon.extractNarClassLoader(any(), any(), any());

        doReturn(ATLEAST_ONCE).when(FunctionCommon.class);
        FunctionCommon.convertProcessingGuarantee(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
>>>>>>> f773c602c... Test pr 10 (#27)


        this.mockedFunctionMetaData = FunctionMetaData.newBuilder().setFunctionDetails(createDefaultFunctionDetails()).build();
        when(mockedManager.getFunctionMetaData(any(), any(), any())).thenReturn(mockedFunctionMetaData);

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);

        SinkConfig sinkConfig = new SinkConfig();
        if (tenant != null) {
            sinkConfig.setTenant(tenant);
        }
        if (namespace != null) {
            sinkConfig.setNamespace(namespace);
        }
        if (sink != null) {
            sinkConfig.setName(sink);
        }
        if (inputTopicsMap != null) {
            sinkConfig.setTopicToSerdeClassName(inputTopicsMap);
        }
        if (className != null) {
            sinkConfig.setClassName(className);
        }
        if (parallelism != null) {
            sinkConfig.setParallelism(parallelism);
        }

<<<<<<< HEAD
        if (expectedError == null) {
            RequestResult rr = new RequestResult()
                    .setSuccess(true)
                    .setMessage("source registered");
            CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
            when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);
        }

        resource.updateFunction(
=======
        if (expectedError != null) {
            doThrow(new IllegalArgumentException(expectedError))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());
        }

        resource.updateSink(
>>>>>>> f773c602c... Test pr 10 (#27)
            tenant,
            namespace,
            sink,
            inputStream,
            details,
            null,
<<<<<<< HEAD
            null,
            new Gson().toJson(sinkConfig),
                null);

    }

    private void updateDefaultSink() throws IOException {
=======
            sinkConfig,
                null, null, null);

    }

    private void updateDefaultSink() throws Exception {
>>>>>>> f773c602c... Test pr 10 (#27)
        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setTenant(tenant);
        sinkConfig.setNamespace(namespace);
        sinkConfig.setName(sink);
        sinkConfig.setClassName(className);
        sinkConfig.setParallelism(parallelism);
        sinkConfig.setTopicToSerdeClassName(topicsToSerDeClassName);

        mockStatic(ConnectorUtils.class);
        doReturn(CassandraStringSink.class.getName()).when(ConnectorUtils.class);
        ConnectorUtils.getIOSinkClass(any(NarClassLoader.class));

<<<<<<< HEAD
        mockStatic(org.apache.pulsar.functions.utils.Utils.class);
        doReturn(String.class).when(org.apache.pulsar.functions.utils.Utils.class);
        org.apache.pulsar.functions.utils.Utils.getSinkType(anyString(), any(NarClassLoader.class));

        doReturn(mock(NarClassLoader.class)).when(org.apache.pulsar.functions.utils.Utils.class);
        org.apache.pulsar.functions.utils.Utils.extractNarClassLoader(any(Path.class), anyString(), any(File.class));

        doReturn(ATLEAST_ONCE).when(org.apache.pulsar.functions.utils.Utils.class);
        org.apache.pulsar.functions.utils.Utils.convertProcessingGuarantee(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
=======
        mockStatic(ClassLoaderUtils.class);

        mockStatic(FunctionCommon.class);
        PowerMockito.when(FunctionCommon.class, "createPkgTempFile").thenCallRealMethod();

        doReturn(String.class).when(FunctionCommon.class);
        FunctionCommon.getSinkType(anyString(), any(NarClassLoader.class));

        doReturn(mock(NarClassLoader.class)).when(FunctionCommon.class);
        FunctionCommon.extractNarClassLoader(any(), any(), any());

        doReturn(ATLEAST_ONCE).when(FunctionCommon.class);
        FunctionCommon.convertProcessingGuarantee(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
>>>>>>> f773c602c... Test pr 10 (#27)


        this.mockedFunctionMetaData = FunctionMetaData.newBuilder().setFunctionDetails(createDefaultFunctionDetails()).build();
        when(mockedManager.getFunctionMetaData(any(), any(), any())).thenReturn(mockedFunctionMetaData);

<<<<<<< HEAD
        resource.updateFunction(
=======
        resource.updateSink(
>>>>>>> f773c602c... Test pr 10 (#27)
            tenant,
            namespace,
                sink,
                new FileInputStream(JAR_FILE_PATH),
            mockedFormData,
            null,
<<<<<<< HEAD
            null,
            new Gson().toJson(sinkConfig),
                null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Sink test-sink doesn't exist")
    public void testUpdateNotExistedSink() throws IOException {
=======
            sinkConfig,
                null, null, null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Sink test-sink doesn't exist")
    public void testUpdateNotExistedSink() throws Exception {
>>>>>>> f773c602c... Test pr 10 (#27)
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(false);
            updateDefaultSink();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "upload failure")
    public void testUpdateSinkUploadFailure() throws Exception {
        try {
<<<<<<< HEAD
            mockStatic(Utils.class);
            doThrow(new IOException("upload failure")).when(Utils.class);
            Utils.uploadFileToBookkeeper(
=======
            mockStatic(WorkerUtils.class);
            doThrow(new IOException("upload failure")).when(WorkerUtils.class);
            WorkerUtils.uploadFileToBookkeeper(
>>>>>>> f773c602c... Test pr 10 (#27)
                    anyString(),
                    any(File.class),
                    any(Namespace.class));

<<<<<<< HEAD
=======
            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

>>>>>>> f773c602c... Test pr 10 (#27)
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);

            updateDefaultSink();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    @Test
    public void testUpdateSinkSuccess() throws Exception {
<<<<<<< HEAD
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.uploadFileToBookkeeper(
                anyString(),
                any(File.class),
                any(Namespace.class));

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);

        RequestResult rr = new RequestResult()
            .setSuccess(true)
            .setMessage("source registered");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

=======
        mockStatic(WorkerUtils.class);
        doNothing().when(WorkerUtils.class);
        WorkerUtils.uploadFileToBookkeeper(
                anyString(),
                any(File.class),
                any(Namespace.class));
        PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);

>>>>>>> f773c602c... Test pr 10 (#27)
        updateDefaultSink();
    }

    @Test
<<<<<<< HEAD
    public void testUpdateSinkWithUrl() throws IOException {
=======
    public void testUpdateSinkWithUrl() throws Exception {
>>>>>>> f773c602c... Test pr 10 (#27)
        Configurator.setRootLevel(Level.DEBUG);

        String filePackageUrl = "file://" + JAR_FILE_PATH;

        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setTopicToSerdeClassName(topicsToSerDeClassName);
        sinkConfig.setTenant(tenant);
        sinkConfig.setNamespace(namespace);
        sinkConfig.setName(sink);
        sinkConfig.setClassName(className);
        sinkConfig.setParallelism(parallelism);

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);
        mockStatic(ConnectorUtils.class);
        doReturn(CassandraStringSink.class.getName()).when(ConnectorUtils.class);
        ConnectorUtils.getIOSinkClass(any(NarClassLoader.class));

<<<<<<< HEAD
        mockStatic(org.apache.pulsar.functions.utils.Utils.class);
        doReturn(String.class).when(org.apache.pulsar.functions.utils.Utils.class);
        org.apache.pulsar.functions.utils.Utils.getSinkType(anyString(), any(NarClassLoader.class));

        doReturn(mock(NarClassLoader.class)).when(org.apache.pulsar.functions.utils.Utils.class);
        org.apache.pulsar.functions.utils.Utils.extractNarClassLoader(any(Path.class), anyString(), any(File.class));

        doReturn(ATLEAST_ONCE).when(org.apache.pulsar.functions.utils.Utils.class);
        org.apache.pulsar.functions.utils.Utils.convertProcessingGuarantee(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);

=======
        mockStatic(ClassLoaderUtils.class);

        mockStatic(FunctionCommon.class);
        doReturn(String.class).when(FunctionCommon.class);
        FunctionCommon.getSinkType(anyString(), any(NarClassLoader.class));
        PowerMockito.when(FunctionCommon.class, "extractFileFromPkgURL", any()).thenCallRealMethod();

        doReturn(mock(NarClassLoader.class)).when(FunctionCommon.class);
        FunctionCommon.extractNarClassLoader(any(), any(), any());

        doReturn(ATLEAST_ONCE).when(FunctionCommon.class);
        FunctionCommon.convertProcessingGuarantee(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
>>>>>>> f773c602c... Test pr 10 (#27)

        this.mockedFunctionMetaData = FunctionMetaData.newBuilder().setFunctionDetails(createDefaultFunctionDetails()).build();
        when(mockedManager.getFunctionMetaData(any(), any(), any())).thenReturn(mockedFunctionMetaData);

<<<<<<< HEAD
        RequestResult rr = new RequestResult()
                .setSuccess(true)
                .setMessage("source registered");
            CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
            when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

        resource.updateFunction(
=======
        resource.updateSink(
>>>>>>> f773c602c... Test pr 10 (#27)
            tenant,
            namespace,
                sink,
            null,
            null,
            filePackageUrl,
<<<<<<< HEAD
            null,
            new Gson().toJson(sinkConfig),
                null);
=======
            sinkConfig,
                null, null, null);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "sink failed to register")
    public void testUpdateSinkFailure() throws Exception {
        try {
<<<<<<< HEAD
            mockStatic(Utils.class);
            doNothing().when(Utils.class);
            Utils.uploadFileToBookkeeper(
                    anyString(),
                    any(File.class),
                    any(Namespace.class));

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);

            RequestResult rr = new RequestResult()
                    .setSuccess(false)
                    .setMessage("sink failed to register");
            CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
            when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);
=======
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.uploadFileToBookkeeper(
                    anyString(),
                    any(File.class),
                    any(Namespace.class));
            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);

            doThrow(new IllegalArgumentException("sink failed to register"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());
>>>>>>> f773c602c... Test pr 10 (#27)

            updateDefaultSink();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

<<<<<<< HEAD
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "java.io.IOException: Function registeration interrupted")
    public void testUpdateSinkInterrupted() throws Exception {
        try {
            mockStatic(Utils.class);
            doNothing().when(Utils.class);
            Utils.uploadFileToBookkeeper(
                    anyString(),
                    any(File.class),
                    any(Namespace.class));

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);

            CompletableFuture<RequestResult> requestResult = FutureUtil.failedFuture(
                    new IOException("Function registeration interrupted"));
            when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);
=======
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function registration interrupted")
    public void testUpdateSinkInterrupted() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.uploadFileToBookkeeper(
                    anyString(),
                    any(File.class),
                    any(Namespace.class));
            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);

            doThrow(new IllegalStateException("Function registration interrupted"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());
>>>>>>> f773c602c... Test pr 10 (#27)

            updateDefaultSink();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    //
    // deregister sink
    //

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
    public void testDeregisterSinkMissingTenant() {
        try {
            testDeregisterSinkMissingArguments(
                null,
                namespace,
                sink
            );
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace is not provided")
    public void testDeregisterSinkMissingNamespace() {
        try {
            testDeregisterSinkMissingArguments(
                tenant,
                null,
                    sink
            );
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

<<<<<<< HEAD
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Sink Name is not provided")
=======
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Sink name is not provided")
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testDeregisterSinkMissingFunctionName() {
        try {
            testDeregisterSinkMissingArguments(
                tenant,
                namespace,
                null
            );
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    private void testDeregisterSinkMissingArguments(
            String tenant,
            String namespace,
            String sink
    ) {
        resource.deregisterFunction(
            tenant,
            namespace,
            sink,
<<<<<<< HEAD
                null);
=======
                null, null);
>>>>>>> f773c602c... Test pr 10 (#27)

    }

    private void deregisterDefaultSink() {
        resource.deregisterFunction(
            tenant,
            namespace,
                sink,
<<<<<<< HEAD
                null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Sink test-sink doesn't exist")
    public void testDeregisterNotExistedSink() {
=======
                null, null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Sink test-sink doesn't exist")
    public void testDeregisterNotExistedSink()  {
>>>>>>> f773c602c... Test pr 10 (#27)
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(false);
            deregisterDefaultSink();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.NOT_FOUND);
            throw re;
        }
    }

    @Test
<<<<<<< HEAD
    public void testDeregisterSinkSuccess() {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);

        RequestResult rr = new RequestResult()
            .setSuccess(true)
            .setMessage("source deregistered");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.deregisterFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(requestResult);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "sink failed to deregister")
    public void testDeregisterSinkFailure() {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);

            RequestResult rr = new RequestResult()
                .setSuccess(false)
                .setMessage("sink failed to deregister");
            CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
            when(mockedManager.deregisterFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(requestResult);
=======
    public void testDeregisterSinkSuccess() throws Exception {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "sink failed to deregister")
    public void testDeregisterSinkFailure() throws Exception {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);

            when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(sink))).thenReturn(FunctionMetaData.newBuilder().build());

            doThrow(new IllegalArgumentException("sink failed to deregister"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());
>>>>>>> f773c602c... Test pr 10 (#27)

            deregisterDefaultSink();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function deregistration interrupted")
<<<<<<< HEAD
    public void testDeregisterSinkInterrupted() {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);

            CompletableFuture<RequestResult> requestResult = FutureUtil.failedFuture(
                new IOException("Function deregistration interrupted"));
            when(mockedManager.deregisterFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(requestResult);
=======
    public void testDeregisterSinkInterrupted() throws Exception {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);

            when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(sink))).thenReturn(FunctionMetaData.newBuilder().build());

            doThrow(new IllegalStateException("Function deregistration interrupted"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());
>>>>>>> f773c602c... Test pr 10 (#27)

            deregisterDefaultSink();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    //
    // Get Sink Info
    //

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
    public void testGetSinkMissingTenant() {
        try {
            testGetSinkMissingArguments(
                null,
                namespace,
                    sink
            );
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace is not provided")
    public void testGetSinkMissingNamespace() {
        try {
            testGetSinkMissingArguments(
                    tenant,
                    null,
                    sink
            );
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

<<<<<<< HEAD
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Sink Name is not provided")
=======
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Sink name is not provided")
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testGetSinkMissingFunctionName() {
        try {

            testGetSinkMissingArguments(
                tenant,
                namespace,
                null
            );
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    private void testGetSinkMissingArguments(
            String tenant,
            String namespace,
            String sink
    ) {
        resource.getFunctionInfo(
            tenant,
            namespace,
<<<<<<< HEAD
            sink
=======
            sink, null, null
>>>>>>> f773c602c... Test pr 10 (#27)
        );

    }

    private SinkConfig getDefaultSinkInfo() {
        return resource.getSinkInfo(
            tenant,
            namespace,
                sink
        );
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Sink test-sink doesn't exist")
    public void testGetNotExistedSink() {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(false);
            getDefaultSinkInfo();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.NOT_FOUND);
            throw re;
        }
    }

    @Test
    public void testGetSinkSuccess() {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);

        Function.SourceSpec sourceSpec = Function.SourceSpec.newBuilder()
                .setSubscriptionType(Function.SubscriptionType.SHARED)
                .setSubscriptionName(subscriptionName)
                .putInputSpecs("input", Function.ConsumerSpec.newBuilder()
                .setSerdeClassName(DEFAULT_SERDE)
                .setIsRegexPattern(false)
                .build()).build();
        Function.SinkSpec sinkSpec = Function.SinkSpec.newBuilder()
                .setBuiltin("jdbc")
                .build();
        FunctionDetails functionDetails = FunctionDetails.newBuilder()
                .setClassName(IdentityFunction.class.getName())
                .setSink(sinkSpec)
                .setName(sink)
                .setNamespace(namespace)
                .setProcessingGuarantees(ATLEAST_ONCE)
                .setTenant(tenant)
                .setParallelism(parallelism)
                .setRuntime(FunctionDetails.Runtime.JAVA)
                .setSource(sourceSpec).build();
        FunctionMetaData metaData = FunctionMetaData.newBuilder()
            .setCreateTime(System.currentTimeMillis())
            .setFunctionDetails(functionDetails)
            .setPackageLocation(Function.PackageLocationMetaData.newBuilder().setPackagePath("/path/to/package"))
            .setVersion(1234)
            .build();
        when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(sink))).thenReturn(metaData);

        getDefaultSinkInfo();
        assertEquals(
            SinkConfigUtils.convertFromDetails(functionDetails),
            getDefaultSinkInfo());
    }

    //
    // List Sinks
    //

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
    public void testListSinksMissingTenant() {
        try {
            testListSinksMissingArguments(
                null,
                namespace
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace is not provided")
    public void testListFunctionsMissingNamespace() {
        try {
            testListSinksMissingArguments(
            tenant,
            null
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    private void testListSinksMissingArguments(
            String tenant,
            String namespace
    ) {
        resource.listFunctions(
            tenant,
<<<<<<< HEAD
            namespace
=======
            namespace, null, null
>>>>>>> f773c602c... Test pr 10 (#27)
        );

    }

    private List<String> listDefaultSinks() {
        return resource.listFunctions(
            tenant,
<<<<<<< HEAD
            namespace
=======
            namespace, null, null
>>>>>>> f773c602c... Test pr 10 (#27)
        );
    }

    @Test
    public void testListSinksSuccess() {
        final List<String> functions = Lists.newArrayList("test-1", "test-2");
        final List<FunctionMetaData> functionMetaDataList = new LinkedList<>();
        functionMetaDataList.add(FunctionMetaData.newBuilder().setFunctionDetails(
                FunctionDetails.newBuilder().setName("test-1").build()
        ).build());
        functionMetaDataList.add(FunctionMetaData.newBuilder().setFunctionDetails(
                FunctionDetails.newBuilder().setName("test-2").build()
        ).build());
        when(mockedManager.listFunctions(eq(tenant), eq(namespace))).thenReturn(functionMetaDataList);

        List<String> sinkList = listDefaultSinks();
        assertEquals(functions, sinkList);
    }

    @Test
    public void testOnlyGetSinks() {
        final List<String> functions = Lists.newArrayList("test-3");
        final List<FunctionMetaData> functionMetaDataList = new LinkedList<>();
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
        doReturn(org.apache.pulsar.functions.utils.Utils.ComponentType.SOURCE).when(this.resource).calculateSubjectType(f1);
        doReturn(org.apache.pulsar.functions.utils.Utils.ComponentType.FUNCTION).when(this.resource).calculateSubjectType(f2);
        doReturn(org.apache.pulsar.functions.utils.Utils.ComponentType.SINK).when(this.resource).calculateSubjectType(f3);
=======

        mockStatic(InstanceUtils.class);
        PowerMockito.when(InstanceUtils.calculateSubjectType(f1.getFunctionDetails())).thenReturn(FunctionDetails.ComponentType.SOURCE);
        PowerMockito.when(InstanceUtils.calculateSubjectType(f2.getFunctionDetails())).thenReturn(FunctionDetails.ComponentType.FUNCTION);
        PowerMockito.when(InstanceUtils.calculateSubjectType(f3.getFunctionDetails())).thenReturn(FunctionDetails.ComponentType.SINK);
>>>>>>> f773c602c... Test pr 10 (#27)

        List<String> sinkList = listDefaultSinks();
        assertEquals(functions, sinkList);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace does not exist")
<<<<<<< HEAD
    public void testRegisterFunctionNonExistingNamespace() throws Exception {
=======
    public void testregisterSinkNonExistingNamespace() throws Exception {
>>>>>>> f773c602c... Test pr 10 (#27)
        try {
            this.namespaceList.clear();
            registerDefaultSink();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant does not exist")
<<<<<<< HEAD
    public void testRegisterFunctionNonExistingTenant() throws Exception {
=======
    public void testregisterSinkNonExistingTenant() throws Exception {
>>>>>>> f773c602c... Test pr 10 (#27)
        try {
            when(mockedTenants.getTenantInfo(any())).thenThrow(PulsarAdminException.NotFoundException.class);
            registerDefaultSink();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    private SinkConfig createDefaultSinkConfig() {
        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setTenant(tenant);
        sinkConfig.setNamespace(namespace);
        sinkConfig.setName(sink);
        sinkConfig.setClassName(className);
        sinkConfig.setParallelism(parallelism);
        sinkConfig.setTopicToSerdeClassName(topicsToSerDeClassName);
        return sinkConfig;
    }

    private FunctionDetails createDefaultFunctionDetails() throws IOException {
        return SinkConfigUtils.convert(createDefaultSinkConfig(),
                new SinkConfigUtils.ExtractedSinkDetails(null, null));
    }
}
