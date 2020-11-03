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
=======
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.doReturn;
import static org.powermock.api.mockito.PowerMockito.doThrow;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.testng.Assert.assertEquals;

import com.google.common.collect.Lists;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;

import javax.ws.rs.core.Response;

import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.pulsar.client.admin.Functions;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.common.io.SourceConfig;
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
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.Function.FunctionMetaData;
import org.apache.pulsar.functions.proto.Function.PackageLocationMetaData;
import org.apache.pulsar.functions.proto.Function.ProcessingGuarantees;
import org.apache.pulsar.functions.proto.Function.SinkSpec;
import org.apache.pulsar.functions.proto.Function.SourceSpec;
import org.apache.pulsar.functions.runtime.RuntimeFactory;
import org.apache.pulsar.functions.source.TopicSchema;
<<<<<<< HEAD
import org.apache.pulsar.functions.utils.SourceConfigUtils;
import org.apache.pulsar.functions.utils.io.ConnectorUtils;
import org.apache.pulsar.functions.worker.FunctionMetaDataManager;
import org.apache.pulsar.functions.worker.FunctionRuntimeManager;
import org.apache.pulsar.functions.worker.Utils;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.request.RequestResult;
import org.apache.pulsar.functions.worker.rest.RestException;
import org.apache.pulsar.functions.worker.rest.api.SourceImpl;
import org.apache.pulsar.io.twitter.TwitterFireHose;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.mockito.Mockito;
=======
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.utils.SourceConfigUtils;
import org.apache.pulsar.functions.utils.io.ConnectorUtils;
import org.apache.pulsar.functions.worker.*;
import org.apache.pulsar.functions.worker.rest.api.SourcesImpl;
import org.apache.pulsar.io.twitter.TwitterFireHose;
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

<<<<<<< HEAD
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

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
 * Unit test of {@link SourceApiV3Resource}.
 */
@PrepareForTest({Utils.class, ConnectorUtils.class, org.apache.pulsar.functions.utils.Utils.class})
@PowerMockIgnore({ "javax.management.*", "javax.ws.*", "org.apache.logging.log4j.*", "org.apache.pulsar.io.*" })
@Slf4j
=======
/**
 * Unit test of {@link SourcesApiV3Resource}.
 */
@PrepareForTest({WorkerUtils.class, ConnectorUtils.class, FunctionCommon.class, ClassLoaderUtils.class, InstanceUtils.class})
@PowerMockIgnore({ "javax.management.*", "javax.ws.*", "org.apache.logging.log4j.*", "org.apache.pulsar.io.*" })
>>>>>>> f773c602c... Test pr 10 (#27)
public class SourceApiV3ResourceTest {

    @ObjectFactory
    public IObjectFactory getObjectFactory() {
        return new org.powermock.modules.testng.PowerMockObjectFactory();
    }

    private static final String tenant = "test-tenant";
    private static final String namespace = "test-namespace";
    private static final String source = "test-source";
    private static final String outputTopic = "test-output-topic";
    private static final String outputSerdeClassName = TopicSchema.DEFAULT_SERDE;
    private static final String className = TwitterFireHose.class.getName();
    private static final int parallelism = 1;
    private static final String JAR_FILE_NAME = "pulsar-io-twitter.nar";
    private static final String INVALID_JAR_FILE_NAME = "pulsar-io-cassandra.nar";
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
    private SourceImpl resource;
    private InputStream mockedInputStream;
    private FormDataContentDisposition mockedFormData;
    private FunctionMetaData mockedFunctionMetaData;
=======
    private SourcesImpl resource;
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
        this.resource = spy(new SourceImpl(() -> mockedWorkerService));
        Mockito.doReturn(org.apache.pulsar.functions.utils.Utils.ComponentType.SOURCE).when(this.resource).calculateSubjectType(any());
=======
        this.resource = spy(new SourcesImpl(() -> mockedWorkerService));
        mockStatic(InstanceUtils.class);
        PowerMockito.when(InstanceUtils.calculateSubjectType(any())).thenReturn(FunctionDetails.ComponentType.SOURCE);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    //
    // Register Functions
    //

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
    public void testRegisterSourceMissingTenant() {
        try {
            testRegisterSourceMissingArguments(
                    null,
                    namespace,
                    source,
                    mockedInputStream,
                    mockedFormData,
                    outputTopic,
                    outputSerdeClassName,
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
    public void testRegisterSourceMissingNamespace() {
        try {
            testRegisterSourceMissingArguments(
                tenant,
                null,
                source,
                mockedInputStream,
                mockedFormData,
                outputTopic,
                outputSerdeClassName,
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
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source Name is not provided")
=======
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source name is not provided")
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testRegisterSourceMissingSourceName() {
        try {
            testRegisterSourceMissingArguments(
                    tenant,
                    namespace,
                    null,
                    mockedInputStream,
                    mockedFormData,
                    outputTopic,
                    outputSerdeClassName,
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
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source Package is not provided")
=======
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source class UnknownClass must be in class path")
    public void testRegisterSourceWrongClassName() {
        try {
            testRegisterSourceMissingArguments(
                    tenant,
                    namespace,
                    source,
                    mockedInputStream,
                    mockedFormData,
                    outputTopic,
                    outputSerdeClassName,
                    "UnknownClass",
                    parallelism,
                    null
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source package is not provided")
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testRegisterSourceMissingPackage() {
        try {
            testRegisterSourceMissingArguments(
                    tenant,
                    namespace,
                    source,
                    null,
                    mockedFormData,
                    outputTopic,
                    outputSerdeClassName,
                    null,
                    parallelism,
                    null
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source Package is not provided")
    public void testRegisterSourceMissingPackageDetails() {
        try {
            testRegisterSourceMissingArguments(
                tenant,
                namespace,
                    source,
                mockedInputStream,
                null,
                outputTopic,
                    outputSerdeClassName,
                className,
                parallelism,
                    null
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Failed to extract source class from archive")
    public void testRegisterSourceInvalidJarWithNoSource() throws IOException {
        try {
            FileInputStream inputStream = new FileInputStream(INVALID_JAR_FILE_PATH);
            testRegisterSourceMissingArguments(
                    tenant,
                    namespace,
                    source,
                    inputStream,
                    null,
                    outputTopic,
                    outputSerdeClassName,
                    null,
                    parallelism,
                    null
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Topic name cannot be null")
    public void testRegisterSourceNoOutputTopic() throws IOException {
        try {
            FileInputStream inputStream = new FileInputStream(JAR_FILE_PATH);
            testRegisterSourceMissingArguments(
                    tenant,
                    namespace,
                    source,
                    inputStream,
                    mockedFormData,
                    null,
                    outputSerdeClassName,
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
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Invalid Source Jar")
=======
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Encountered error .*. when getting Source package from .*")
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testRegisterSourceHttpUrl() {
        try {
            testRegisterSourceMissingArguments(
                    tenant,
                    namespace,
                    source,
                    null,
                    null,
                    outputTopic,
                    outputSerdeClassName,
                    className,
                    parallelism,
                    "http://localhost:1234/test"
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    private void testRegisterSourceMissingArguments(
            String tenant,
            String namespace,
            String function,
            InputStream inputStream,
            FormDataContentDisposition details,
            String outputTopic,
            String outputSerdeClassName,
            String className,
            Integer parallelism,
            String pkgUrl) {
        SourceConfig sourceConfig = new SourceConfig();
        if (tenant != null) {
            sourceConfig.setTenant(tenant);
        }
        if (namespace != null) {
            sourceConfig.setNamespace(namespace);
        }
        if (function != null) {
            sourceConfig.setName(function);
        }
        if (outputTopic != null) {
            sourceConfig.setTopicName(outputTopic);
        }
        if (outputSerdeClassName != null) {
            sourceConfig.setSerdeClassName(outputSerdeClassName);
        }
        if (className != null) {
            sourceConfig.setClassName(className);
        }
        if (parallelism != null) {
            sourceConfig.setParallelism(parallelism);
        }

<<<<<<< HEAD
        resource.registerFunction(
=======
        resource.registerSource(
>>>>>>> f773c602c... Test pr 10 (#27)
                tenant,
                namespace,
                function,
                inputStream,
                details,
                pkgUrl,
<<<<<<< HEAD
                null,
                new Gson().toJson(sourceConfig),
                null);

=======
                sourceConfig,
                null, null);

    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source config is not provided")
    public void testMissingSinkConfig() {
        resource.registerSource(
                tenant,
                namespace,
                source,
                mockedInputStream,
                mockedFormData,
                null,
                null,
                null, null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source config is not provided")
    public void testUpdateMissingSinkConfig() {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);
        resource.updateSource(
                tenant,
                namespace,
                source,
                mockedInputStream,
                mockedFormData,
                null,
                null,
                null, null, null);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    private void registerDefaultSource() throws IOException {
        SourceConfig sourceConfig = createDefaultSourceConfig();
<<<<<<< HEAD
        resource.registerFunction(
=======
        resource.registerSource(
>>>>>>> f773c602c... Test pr 10 (#27)
            tenant,
            namespace,
                source,
            new FileInputStream(JAR_FILE_PATH),
            mockedFormData,
            null,
<<<<<<< HEAD
            null,
            new Gson().toJson(sourceConfig),
                null);
=======
            sourceConfig,
                null, null);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source test-source already exists")
    public void testRegisterExistedSource() throws IOException {
        try {
            Configurator.setRootLevel(Level.DEBUG);

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);

            registerDefaultSource();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "upload failure")
    public void testRegisterSourceUploadFailure() throws Exception {
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
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(false);

            registerDefaultSource();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

<<<<<<< HEAD
    public void testRegisterSourceSuccess() throws Exception {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.uploadFileToBookkeeper(
=======
    @Test
    public void testRegisterSourceSuccess() throws Exception {
        mockStatic(WorkerUtils.class);
        doNothing().when(WorkerUtils.class);
        WorkerUtils.uploadFileToBookkeeper(
>>>>>>> f773c602c... Test pr 10 (#27)
                anyString(),
                any(File.class),
                any(Namespace.class));

<<<<<<< HEAD
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(false);

        RequestResult rr = new RequestResult()
            .setSuccess(true)
            .setMessage("source registered");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);
=======
        PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(false);
>>>>>>> f773c602c... Test pr 10 (#27)

        registerDefaultSource();
    }

    @Test
    public void testRegisterSourceConflictingFields() throws Exception {
<<<<<<< HEAD
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.uploadFileToBookkeeper(
                anyString(),
                any(File.class),
                any(Namespace.class));
=======

        mockStatic(WorkerUtils.class);
        PowerMockito.doNothing().when(WorkerUtils.class, "uploadFileToBookkeeper", anyString(),
                any(File.class),
                any(Namespace.class));

        PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

>>>>>>> f773c602c... Test pr 10 (#27)
        String actualTenant = "DIFFERENT_TENANT";
        String actualNamespace = "DIFFERENT_NAMESPACE";
        String actualName = "DIFFERENT_NAME";
        this.namespaceList.add(actualTenant + "/" + actualNamespace);

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);
        when(mockedManager.containsFunction(eq(actualTenant), eq(actualNamespace), eq(actualName))).thenReturn(false);

<<<<<<< HEAD
        RequestResult rr = new RequestResult()
                .setSuccess(true)
                .setMessage("source registered");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

=======
>>>>>>> f773c602c... Test pr 10 (#27)
        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setTenant(tenant);
        sourceConfig.setNamespace(namespace);
        sourceConfig.setName(source);
        sourceConfig.setClassName(className);
        sourceConfig.setParallelism(parallelism);
        sourceConfig.setTopicName(outputTopic);
        sourceConfig.setSerdeClassName(outputSerdeClassName);
<<<<<<< HEAD
        resource.registerFunction(
=======
        resource.registerSource(
>>>>>>> f773c602c... Test pr 10 (#27)
                actualTenant,
                actualNamespace,
                actualName,
                new FileInputStream(JAR_FILE_PATH),
                mockedFormData,
                null,
<<<<<<< HEAD
                null,
                new Gson().toJson(sourceConfig),
                null);
=======
                sourceConfig,
                null, null);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "source failed to register")
    public void testRegisterSourceFailure() throws Exception {
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
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(false);

            RequestResult rr = new RequestResult()
                .setSuccess(false)
                .setMessage("source failed to register");
            CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
            when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);
=======
            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(false);

            doThrow(new IllegalArgumentException("source failed to register"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());
>>>>>>> f773c602c... Test pr 10 (#27)

            registerDefaultSource();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

<<<<<<< HEAD
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "java.io.IOException: Function registration interrupted")
    public void testRegisterSourceInterrupted() throws Exception {
        try {
            mockStatic(Utils.class);
            doNothing().when(Utils.class);
            Utils.uploadFileToBookkeeper(
=======
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function registration interrupted")
    public void testRegisterSourceInterrupted() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.uploadFileToBookkeeper(
>>>>>>> f773c602c... Test pr 10 (#27)
                    anyString(),
                    any(File.class),
                    any(Namespace.class));

<<<<<<< HEAD
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(false);

            CompletableFuture<RequestResult> requestResult = FutureUtil.failedFuture(
                new IOException("Function registration interrupted"));
            when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);
=======
            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(false);

            doThrow(new IllegalStateException("Function registration interrupted"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());
>>>>>>> f773c602c... Test pr 10 (#27)

            registerDefaultSource();
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
    public void testUpdateSourceMissingTenant() throws IOException {
=======
    public void testUpdateSourceMissingTenant() throws Exception {
>>>>>>> f773c602c... Test pr 10 (#27)
        try {
            testUpdateSourceMissingArguments(
                null,
                namespace,
                    source,
                mockedInputStream,
                mockedFormData,
                outputTopic,
                    outputSerdeClassName,
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
    public void testUpdateSourceMissingNamespace() throws IOException {
=======
    public void testUpdateSourceMissingNamespace() throws Exception {
>>>>>>> f773c602c... Test pr 10 (#27)
        try {
            testUpdateSourceMissingArguments(
                tenant,
                null,
                    source,
                mockedInputStream,
                mockedFormData,
                outputTopic,
                    outputSerdeClassName,
                className,
                parallelism,
                    "Namespace is not provided");
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

<<<<<<< HEAD
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source Name is not provided")
    public void testUpdateSourceMissingFunctionName() throws IOException {
=======
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source name is not provided")
    public void testUpdateSourceMissingFunctionName() throws Exception {
>>>>>>> f773c602c... Test pr 10 (#27)
        try {
            testUpdateSourceMissingArguments(
                tenant,
                namespace,
                null,
                mockedInputStream,
                mockedFormData,
                outputTopic,
                    outputSerdeClassName,
                className,
                parallelism,
<<<<<<< HEAD
                    "Source Name is not provided");
=======
                    "Source name is not provided");
>>>>>>> f773c602c... Test pr 10 (#27)
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Update contains no change")
<<<<<<< HEAD
    public void testUpdateSourceMissingPackage() throws IOException {
        try {
            mockStatic(Utils.class);
            doNothing().when(Utils.class);
            Utils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());
=======
    public void testUpdateSourceMissingPackage() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());
>>>>>>> f773c602c... Test pr 10 (#27)

            testUpdateSourceMissingArguments(
                tenant,
                namespace,
                    source,
                    null,
                mockedFormData,
                outputTopic,
                    outputSerdeClassName,
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
    public void testUpdateSourceMissingTopicName() throws IOException {
        try {
            mockStatic(Utils.class);
            doNothing().when(Utils.class);
            Utils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());
=======
    public void testUpdateSourceMissingTopicName() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());
>>>>>>> f773c602c... Test pr 10 (#27)

            testUpdateSourceMissingArguments(
                    tenant,
                    namespace,
                    source,
                    null,
                    mockedFormData,
                    null,
                    outputSerdeClassName,
                    null,
                    parallelism,
                    "Update contains no change");
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

<<<<<<< HEAD
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source parallelism should positive number")
    public void testUpdateSourceNegativeParallelism() throws IOException {
        try {
            mockStatic(Utils.class);
            doNothing().when(Utils.class);
            Utils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());
=======
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source parallelism must be a positive number")
    public void testUpdateSourceNegativeParallelism() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());

            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();
>>>>>>> f773c602c... Test pr 10 (#27)

            testUpdateSourceMissingArguments(
                    tenant,
                    namespace,
                    source,
                    null,
                    mockedFormData,
                    outputTopic,
                    outputSerdeClassName,
                    className,
                    -2,
<<<<<<< HEAD
                    "Source parallelism should positive number");
=======
                    "Source parallelism must be a positive number");
>>>>>>> f773c602c... Test pr 10 (#27)
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test
<<<<<<< HEAD
    public void testUpdateSourceChangedParallelism() throws IOException {
        try {
            mockStatic(Utils.class);
            doNothing().when(Utils.class);
            Utils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());
=======
    public void testUpdateSourceChangedParallelism() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());

            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();
>>>>>>> f773c602c... Test pr 10 (#27)

            testUpdateSourceMissingArguments(
                tenant,
                namespace,
                source,
                null,
                mockedFormData,
                outputTopic,
                outputSerdeClassName,
                className,
                parallelism + 1,
                null);
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

<<<<<<< HEAD
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Destination topics differ")
    public void testUpdateSourceChangedTopic() throws IOException {
        try {
            mockStatic(Utils.class);
            doNothing().when(Utils.class);
            Utils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());

            testUpdateSourceMissingArguments(
                    tenant,
                    namespace,
                    source,
                    null,
                    mockedFormData,
                    "DifferentTopic",
                    outputSerdeClassName,
                    className,
                    parallelism,
                    "Destination topics differ");
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source parallelism should positive number")
    public void testUpdateSourceZeroParallelism() throws IOException {
        try {
            mockStatic(Utils.class);
            doNothing().when(Utils.class);
            Utils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());
=======
    @Test
    public void testUpdateSourceChangedTopic() throws Exception {
        mockStatic(WorkerUtils.class);
        doNothing().when(WorkerUtils.class);
        WorkerUtils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());

        testUpdateSourceMissingArguments(
                tenant,
                namespace,
                source,
                null,
                mockedFormData,
                "DifferentTopic",
                outputSerdeClassName,
                className,
                parallelism,
                null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source parallelism must be a positive number")
    public void testUpdateSourceZeroParallelism() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());

            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();
>>>>>>> f773c602c... Test pr 10 (#27)

            testUpdateSourceMissingArguments(
                    tenant,
                    namespace,
                    source,
                    mockedInputStream,
                    mockedFormData,
                    outputTopic,
                    outputSerdeClassName,
                    className,
                    0,
<<<<<<< HEAD
                    "Source parallelism should positive number");
=======
                    "Source parallelism must be a positive number");
>>>>>>> f773c602c... Test pr 10 (#27)
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    private void testUpdateSourceMissingArguments(
            String tenant,
            String namespace,
            String function,
            InputStream inputStream,
            FormDataContentDisposition details,
            String outputTopic,
            String outputSerdeClassName,
            String className,
            Integer parallelism,
<<<<<<< HEAD
            String expectedError) throws IOException {
=======
            String expectedError) throws Exception {

        NarClassLoader classLoader = mock(NarClassLoader.class);
        doReturn(TwitterFireHose.class).when(classLoader).loadClass(eq(TwitterFireHose.class.getName()));
>>>>>>> f773c602c... Test pr 10 (#27)

        mockStatic(ConnectorUtils.class);
        doReturn(TwitterFireHose.class.getName()).when(ConnectorUtils.class);
        ConnectorUtils.getIOSourceClass(any(NarClassLoader.class));

<<<<<<< HEAD
        mockStatic(org.apache.pulsar.functions.utils.Utils.class);
        doReturn(String.class).when(org.apache.pulsar.functions.utils.Utils.class);
        org.apache.pulsar.functions.utils.Utils.getSourceType(anyString(), any(NarClassLoader.class));

        doReturn(mock(NarClassLoader.class)).when(org.apache.pulsar.functions.utils.Utils.class);
        org.apache.pulsar.functions.utils.Utils.extractNarClassLoader(any(Path.class), anyString(), any(File.class));
=======
        mockStatic(ClassLoaderUtils.class);

        mockStatic(FunctionCommon.class);
        PowerMockito.when(FunctionCommon.class, "createPkgTempFile").thenCallRealMethod();
        doReturn(String.class).when(FunctionCommon.class);
        FunctionCommon.getSourceType(eq(TwitterFireHose.class));

        doReturn(classLoader).when(FunctionCommon.class);
        FunctionCommon.extractNarClassLoader(any(), any(), any());
>>>>>>> f773c602c... Test pr 10 (#27)

        this.mockedFunctionMetaData = FunctionMetaData.newBuilder().setFunctionDetails(createDefaultFunctionDetails()).build();
        when(mockedManager.getFunctionMetaData(any(), any(), any())).thenReturn(mockedFunctionMetaData);

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

        SourceConfig sourceConfig = new SourceConfig();
        if (tenant != null) {
            sourceConfig.setTenant(tenant);
        }
        if (namespace != null) {
            sourceConfig.setNamespace(namespace);
        }
        if (function != null) {
            sourceConfig.setName(function);
        }
        if (outputTopic != null) {
            sourceConfig.setTopicName(outputTopic);
        }
        if (outputSerdeClassName != null) {
            sourceConfig.setSerdeClassName(outputSerdeClassName);
        }
        if (className != null) {
            sourceConfig.setClassName(className);
        }
        if (parallelism != null) {
            sourceConfig.setParallelism(parallelism);
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

        resource.updateSource(
>>>>>>> f773c602c... Test pr 10 (#27)
            tenant,
            namespace,
            function,
            inputStream,
            details,
            null,
<<<<<<< HEAD
            null,
            new Gson().toJson(sourceConfig),
                null);

    }

    private void updateDefaultSource() throws IOException {
=======
            sourceConfig,
                null, null, null);

    }

    private void updateDefaultSource() throws Exception {
>>>>>>> f773c602c... Test pr 10 (#27)
        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setTenant(tenant);
        sourceConfig.setNamespace(namespace);
        sourceConfig.setName(source);
        sourceConfig.setClassName(className);
        sourceConfig.setParallelism(parallelism);
        sourceConfig.setTopicName(outputTopic);
        sourceConfig.setSerdeClassName(outputSerdeClassName);

<<<<<<< HEAD
=======
        NarClassLoader classLoader = mock(NarClassLoader.class);
        doReturn(TwitterFireHose.class).when(classLoader).loadClass(eq(TwitterFireHose.class.getName()));

>>>>>>> f773c602c... Test pr 10 (#27)
        mockStatic(ConnectorUtils.class);
        doReturn(TwitterFireHose.class.getName()).when(ConnectorUtils.class);
        ConnectorUtils.getIOSourceClass(any(NarClassLoader.class));

<<<<<<< HEAD
        mockStatic(org.apache.pulsar.functions.utils.Utils.class);
        doReturn(String.class).when(org.apache.pulsar.functions.utils.Utils.class);
        org.apache.pulsar.functions.utils.Utils.getSourceType(anyString(), any(NarClassLoader.class));

        doReturn(mock(NarClassLoader.class)).when(org.apache.pulsar.functions.utils.Utils.class);
        org.apache.pulsar.functions.utils.Utils.extractNarClassLoader(any(Path.class), anyString(), any(File.class));
=======
        mockStatic(ClassLoaderUtils.class);

        mockStatic(FunctionCommon.class);
        PowerMockito.when(FunctionCommon.class, "createPkgTempFile").thenCallRealMethod();
        doReturn(String.class).when(FunctionCommon.class);
        FunctionCommon.getSourceType(eq(TwitterFireHose.class));

        doReturn(classLoader).when(FunctionCommon.class);
        FunctionCommon.extractNarClassLoader(any(), any(File.class), any());
>>>>>>> f773c602c... Test pr 10 (#27)

        this.mockedFunctionMetaData = FunctionMetaData.newBuilder().setFunctionDetails(createDefaultFunctionDetails()).build();
        when(mockedManager.getFunctionMetaData(any(), any(), any())).thenReturn(mockedFunctionMetaData);

<<<<<<< HEAD
        resource.updateFunction(
=======
        resource.updateSource(
>>>>>>> f773c602c... Test pr 10 (#27)
            tenant,
            namespace,
                source,
                new FileInputStream(JAR_FILE_PATH),
            mockedFormData,
            null,
<<<<<<< HEAD
            null,
            new Gson().toJson(sourceConfig),
                null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source test-source doesn't exist")
    public void testUpdateNotExistedSource() throws IOException {
=======
            sourceConfig,
                null, null, null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source test-source doesn't exist")
    public void testUpdateNotExistedSource() throws Exception {
>>>>>>> f773c602c... Test pr 10 (#27)
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(false);
            updateDefaultSource();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "upload failure")
    public void testUpdateSourceUploadFailure() throws Exception {
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
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);
            updateDefaultSource();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    @Test
    public void testUpdateSourceSuccess() throws Exception {
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
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);

        RequestResult rr = new RequestResult()
            .setSuccess(true)
            .setMessage("source registered");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);
=======
        PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);
>>>>>>> f773c602c... Test pr 10 (#27)

        updateDefaultSource();
    }

    @Test
<<<<<<< HEAD
    public void testUpdateSourceWithUrl() throws IOException {
=======
    public void testUpdateSourceWithUrl() throws Exception {
>>>>>>> f773c602c... Test pr 10 (#27)
        Configurator.setRootLevel(Level.DEBUG);

        String filePackageUrl = "file://" + JAR_FILE_PATH;

        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setTopicName(outputTopic);
        sourceConfig.setSerdeClassName(outputSerdeClassName);
        sourceConfig.setTenant(tenant);
        sourceConfig.setNamespace(namespace);
        sourceConfig.setName(source);
        sourceConfig.setClassName(className);
        sourceConfig.setParallelism(parallelism);

<<<<<<< HEAD
=======
        NarClassLoader classLoader = mock(NarClassLoader.class);
        doReturn(TwitterFireHose.class).when(classLoader).loadClass(eq(TwitterFireHose.class.getName()));

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);
>>>>>>> f773c602c... Test pr 10 (#27)
        mockStatic(ConnectorUtils.class);
        doReturn(TwitterFireHose.class.getName()).when(ConnectorUtils.class);
        ConnectorUtils.getIOSourceClass(any(NarClassLoader.class));

<<<<<<< HEAD
        mockStatic(org.apache.pulsar.functions.utils.Utils.class);
        doReturn(String.class).when(org.apache.pulsar.functions.utils.Utils.class);
        org.apache.pulsar.functions.utils.Utils.getSourceType(anyString(), any(NarClassLoader.class));

        doReturn(mock(NarClassLoader.class)).when(org.apache.pulsar.functions.utils.Utils.class);
        org.apache.pulsar.functions.utils.Utils.extractNarClassLoader(any(Path.class), anyString(), any(File.class));
=======
        mockStatic(ClassLoaderUtils.class);

        mockStatic(FunctionCommon.class);
        doReturn(String.class).when(FunctionCommon.class);
        FunctionCommon.getSourceType(eq(TwitterFireHose.class));
        PowerMockito.when(FunctionCommon.class, "extractFileFromPkgURL", any()).thenCallRealMethod();

        doReturn(classLoader).when(FunctionCommon.class);
        FunctionCommon.extractNarClassLoader(any(), any(), any());
>>>>>>> f773c602c... Test pr 10 (#27)

        this.mockedFunctionMetaData = FunctionMetaData.newBuilder().setFunctionDetails(createDefaultFunctionDetails()).build();
        when(mockedManager.getFunctionMetaData(any(), any(), any())).thenReturn(mockedFunctionMetaData);

<<<<<<< HEAD

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);
        RequestResult rr = new RequestResult()
                .setSuccess(true)
                .setMessage("source registered");
            CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
            when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

        resource.updateFunction(
=======
        resource.updateSource(
>>>>>>> f773c602c... Test pr 10 (#27)
            tenant,
            namespace,
                source,
            null,
            null,
            filePackageUrl,
<<<<<<< HEAD
            null,
            new Gson().toJson(sourceConfig),
                null);
=======
            sourceConfig,
                null, null, null);
>>>>>>> f773c602c... Test pr 10 (#27)

    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "source failed to register")
    public void testUpdateSourceFailure() throws Exception {
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
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);

            RequestResult rr = new RequestResult()
                    .setSuccess(false)
                    .setMessage("source failed to register");
            CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
            when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);
=======
            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);

            doThrow(new IllegalArgumentException("source failed to register"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());
>>>>>>> f773c602c... Test pr 10 (#27)

            updateDefaultSource();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

<<<<<<< HEAD
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "java.io.IOException: Function registration interrupted")
    public void testUpdateSourceInterrupted() throws Exception {
        try {
            mockStatic(Utils.class);
            doNothing().when(Utils.class);
            Utils.uploadFileToBookkeeper(
=======
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function registration interrupted")
    public void testUpdateSourceInterrupted() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.uploadFileToBookkeeper(
>>>>>>> f773c602c... Test pr 10 (#27)
                    anyString(),
                    any(File.class),
                    any(Namespace.class));

<<<<<<< HEAD
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);

            CompletableFuture<RequestResult> requestResult = FutureUtil.failedFuture(
                new IOException("Function registration interrupted"));
            when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);
=======
            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);

            doThrow(new IllegalStateException("Function registration interrupted"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());
>>>>>>> f773c602c... Test pr 10 (#27)

            updateDefaultSource();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    //
    // deregister source
    //

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
    public void testDeregisterSourceMissingTenant() {
        try {
            testDeregisterSourceMissingArguments(
                null,
                namespace,
                    source
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace is not provided")
    public void testDeregisterSourceMissingNamespace() {
        try {
            testDeregisterSourceMissingArguments(
                tenant,
                null,
                source
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

<<<<<<< HEAD
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source Name is not provided")
=======
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source name is not provided")
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testDeregisterSourceMissingFunctionName() {
        try {
            testDeregisterSourceMissingArguments(
                tenant,
                namespace,
                null
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    private void testDeregisterSourceMissingArguments(
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

    private void deregisterDefaultSource() {
        resource.deregisterFunction(
            tenant,
            namespace,
                source,
<<<<<<< HEAD
                null);
=======
                null, null);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp= "Source test-source doesn't exist")
    public void testDeregisterNotExistedSource() {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(false);
            deregisterDefaultSource();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.NOT_FOUND);
            throw re;
        }
    }

    @Test
    public void testDeregisterSourceSuccess() {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);

<<<<<<< HEAD
        RequestResult rr = new RequestResult()
            .setSuccess(true)
            .setMessage("source deregistered");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.deregisterFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(requestResult);
=======
        when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(source))).thenReturn(FunctionMetaData.newBuilder().build());
>>>>>>> f773c602c... Test pr 10 (#27)

        deregisterDefaultSource();
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "source failed to deregister")
<<<<<<< HEAD
    public void testDeregisterSourceFailure() {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);

            RequestResult rr = new RequestResult()
                .setSuccess(false)
                .setMessage("source failed to deregister");
            CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
            when(mockedManager.deregisterFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(requestResult);
=======
    public void testDeregisterSourceFailure() throws Exception {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);

            when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(source))).thenReturn(FunctionMetaData.newBuilder().build());

            doThrow(new IllegalArgumentException("source failed to deregister"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());
>>>>>>> f773c602c... Test pr 10 (#27)

            deregisterDefaultSource();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function deregistration interrupted")
<<<<<<< HEAD
    public void testDeregisterSourceInterrupted() {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);

            CompletableFuture<RequestResult> requestResult = FutureUtil.failedFuture(
                new IOException("Function deregistration interrupted"));
            when(mockedManager.deregisterFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(requestResult);
=======
    public void testDeregisterSourceInterrupted() throws Exception {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);

            when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(source))).thenReturn(FunctionMetaData.newBuilder().build());

            doThrow(new IllegalStateException("Function deregistration interrupted"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());
>>>>>>> f773c602c... Test pr 10 (#27)

            deregisterDefaultSource();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    //
    // Get Source Info
    //

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
    public void testGetSourceMissingTenant() {
        try {
            testGetSourceMissingArguments(
                    null,
                    namespace,
                    source
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace is not provided")
    public void testGetSourceMissingNamespace() {
        try {
            testGetSourceMissingArguments(
                tenant,
                null,
                    source
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

<<<<<<< HEAD
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source Name is not provided")
=======
    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source name is not provided")
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testGetSourceMissingFunctionName() {
        try {
            testGetSourceMissingArguments(
                tenant,
                namespace,
                null
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    private void testGetSourceMissingArguments(
            String tenant,
            String namespace,
            String source
    ) {
        resource.getFunctionInfo(
            tenant,
            namespace,
<<<<<<< HEAD
            source
=======
            source, null, null
>>>>>>> f773c602c... Test pr 10 (#27)
        );
    }

    private SourceConfig getDefaultSourceInfo() {
        return resource.getSourceInfo(
            tenant,
            namespace,
                source
        );
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source test-source doesn't exist")
    public void testGetNotExistedSource() {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(false);
            getDefaultSourceInfo();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.NOT_FOUND);
            throw re;
        }
    }

    @Test
    public void testGetSourceSuccess() {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);

        SourceSpec sourceSpec = SourceSpec.newBuilder().setBuiltin("jdbc").build();
        SinkSpec sinkSpec = SinkSpec.newBuilder()
                .setTopic(outputTopic)
                .setSerDeClassName(outputSerdeClassName).build();
        FunctionDetails functionDetails = FunctionDetails.newBuilder()
                .setClassName(IdentityFunction.class.getName())
                .setSink(sinkSpec)
                .setName(source)
                .setNamespace(namespace)
                .setProcessingGuarantees(ProcessingGuarantees.ATLEAST_ONCE)
                .setRuntime(FunctionDetails.Runtime.JAVA)
                .setAutoAck(true)
                .setTenant(tenant)
                .setParallelism(parallelism)
                .setSource(sourceSpec).build();
        FunctionMetaData metaData = FunctionMetaData.newBuilder()
            .setCreateTime(System.currentTimeMillis())
            .setFunctionDetails(functionDetails)
            .setPackageLocation(PackageLocationMetaData.newBuilder().setPackagePath("/path/to/package"))
            .setVersion(1234)
            .build();
        when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(source))).thenReturn(metaData);

        SourceConfig config = getDefaultSourceInfo();
        assertEquals(SourceConfigUtils.convertFromDetails(functionDetails), config);
    }

    //
    // List Sources
    //

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
    public void testListSourcesMissingTenant() {
        try {
            testListSourcesMissingArguments(
                null,
                namespace
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace is not provided")
    public void testListSourcesMissingNamespace() {
        try {
            testListSourcesMissingArguments(
                tenant,
                null
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    private void testListSourcesMissingArguments(
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

    private List<String> listDefaultSources() {
        return resource.listFunctions(
            tenant,
<<<<<<< HEAD
            namespace);
=======
            namespace, null, null);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Test
    public void testListSourcesSuccess() {
        final List<String> functions = Lists.newArrayList("test-1", "test-2");
        final List<FunctionMetaData> functionMetaDataList = new LinkedList<>();
        functionMetaDataList.add(FunctionMetaData.newBuilder().setFunctionDetails(
                FunctionDetails.newBuilder().setName("test-1").build()
        ).build());
        functionMetaDataList.add(FunctionMetaData.newBuilder().setFunctionDetails(
                FunctionDetails.newBuilder().setName("test-2").build()
        ).build());
        when(mockedManager.listFunctions(eq(tenant), eq(namespace))).thenReturn(functionMetaDataList);

        List<String> sourceList = listDefaultSources();
        assertEquals(functions, sourceList);
    }

    @Test
    public void testOnlyGetSources() {
        final List<String> functions = Lists.newArrayList("test-1");
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

        List<String> sourceList = listDefaultSources();
        assertEquals(functions, sourceList);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace does not exist")
    public void testRegisterFunctionNonExistingNamespace() throws Exception {
        try {
            this.namespaceList.clear();
            registerDefaultSource();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant does not exist")
    public void testRegisterFunctionNonExistingTenant() throws Exception {
        try {
            when(mockedTenants.getTenantInfo(any())).thenThrow(PulsarAdminException.NotFoundException.class);
            registerDefaultSource();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    private SourceConfig createDefaultSourceConfig() {
        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setTenant(tenant);
        sourceConfig.setNamespace(namespace);
        sourceConfig.setName(source);
        sourceConfig.setClassName(className);
        sourceConfig.setParallelism(parallelism);
        sourceConfig.setTopicName(outputTopic);
        sourceConfig.setSerdeClassName(outputSerdeClassName);
        return sourceConfig;
    }

    private FunctionDetails createDefaultFunctionDetails() {
        return SourceConfigUtils.convert(createDefaultSourceConfig(),
                new SourceConfigUtils.ExtractedSourceDetails(null, null));
    }
}
