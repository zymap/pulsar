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
package org.apache.pulsar.functions.worker.rest.api;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
<<<<<<< HEAD
import org.apache.pulsar.common.functions.FunctionState;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.policies.data.FunctionStatus;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.InstanceCommunication;
=======
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.FunctionState;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.policies.data.FunctionStatus;
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.utils.FunctionConfigUtils;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.functions.worker.FunctionMetaDataManager;
import org.apache.pulsar.functions.worker.WorkerService;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Slf4j
public class FunctionsImplV2 {

    private FunctionsImpl delegate;
    public FunctionsImplV2(Supplier<WorkerService> workerServiceSupplier) {
        this.delegate = new FunctionsImpl(workerServiceSupplier);
    }

    // For test purposes
    public FunctionsImplV2(FunctionsImpl delegate) {
        this.delegate = delegate;
    }

<<<<<<< HEAD
    public Response getFunctionInfo(final String tenant, final String namespace, final String functionName)
            throws IOException {

        // run just for parameter checks
        delegate.getFunctionInfo(tenant, namespace, functionName);
=======
    public Response getFunctionInfo(final String tenant, final String namespace, final String functionName, String clientRole)
            throws IOException {

        // run just for parameter checks
        delegate.getFunctionInfo(tenant, namespace, functionName, clientRole, null);
>>>>>>> f773c602c... Test pr 10 (#27)

        FunctionMetaDataManager functionMetaDataManager = delegate.worker().getFunctionMetaDataManager();

        Function.FunctionMetaData functionMetaData = functionMetaDataManager.getFunctionMetaData(tenant, namespace,
                functionName);
<<<<<<< HEAD
        String functionDetailsJson = org.apache.pulsar.functions.utils.Utils.printJson(functionMetaData.getFunctionDetails());
=======
        String functionDetailsJson = FunctionCommon.printJson(functionMetaData.getFunctionDetails());
>>>>>>> f773c602c... Test pr 10 (#27)
        return Response.status(Response.Status.OK).entity(functionDetailsJson).build();
    }

    public Response getFunctionInstanceStatus(final String tenant, final String namespace, final String functionName,
<<<<<<< HEAD
                                              final String instanceId, URI uri) throws IOException {

        org.apache.pulsar.common.policies.data.FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData
                functionInstanceStatus = delegate.getFunctionInstanceStatus(tenant, namespace, functionName, instanceId, uri);

        String jsonResponse = org.apache.pulsar.functions.utils.Utils.printJson(toProto(functionInstanceStatus, instanceId));
        return Response.status(Response.Status.OK).entity(jsonResponse).build();
    }

    public Response getFunctionStatusV2(String tenant, String namespace, String functionName, URI requestUri) throws
            IOException {
        FunctionStatus functionStatus = delegate.getFunctionStatus(tenant, namespace, functionName, requestUri);
=======
                                              final String instanceId, URI uri, String clientRole) throws IOException {

        org.apache.pulsar.common.policies.data.FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData
                functionInstanceStatus = delegate.getFunctionInstanceStatus(tenant, namespace, functionName, instanceId, uri, clientRole, null);

        String jsonResponse = FunctionCommon.printJson(toProto(functionInstanceStatus, instanceId));
        return Response.status(Response.Status.OK).entity(jsonResponse).build();
    }

    public Response getFunctionStatusV2(String tenant, String namespace, String functionName, URI requestUri, String clientRole) throws
            IOException {
        FunctionStatus functionStatus = delegate.getFunctionStatus(tenant, namespace, functionName, requestUri, clientRole, null);
>>>>>>> f773c602c... Test pr 10 (#27)
        InstanceCommunication.FunctionStatusList.Builder functionStatusList = InstanceCommunication.FunctionStatusList.newBuilder();
        functionStatus.instances.forEach(functionInstanceStatus -> functionStatusList.addFunctionStatusList(
                toProto(functionInstanceStatus.getStatus(),
                        String.valueOf(functionInstanceStatus.getInstanceId()))));
<<<<<<< HEAD
        String jsonResponse = org.apache.pulsar.functions.utils.Utils.printJson(functionStatusList);
=======
        String jsonResponse = FunctionCommon.printJson(functionStatusList);
>>>>>>> f773c602c... Test pr 10 (#27)
        return Response.status(Response.Status.OK).entity(jsonResponse).build();
    }

    public Response registerFunction(String tenant, String namespace, String functionName, InputStream
            uploadedInputStream, FormDataContentDisposition fileDetail, String functionPkgUrl, String
<<<<<<< HEAD
                                             functionDetailsJson, String functionConfigJson, String clientAppId) {
        delegate.registerFunction(tenant, namespace, functionName, uploadedInputStream, fileDetail,
                functionPkgUrl, functionDetailsJson, functionConfigJson, clientAppId);
=======
                                             functionDetailsJson, String clientRole) {

        Function.FunctionDetails.Builder functionDetailsBuilder = Function.FunctionDetails.newBuilder();
        try {
            FunctionCommon.mergeJson(functionDetailsJson, functionDetailsBuilder);
        } catch (IOException e) {
            throw new RestException(Response.Status.BAD_REQUEST, e.getMessage());
        }
        FunctionConfig functionConfig = FunctionConfigUtils.convertFromDetails(functionDetailsBuilder.build());

        delegate.registerFunction(tenant, namespace, functionName, uploadedInputStream, fileDetail,
                functionPkgUrl, functionConfig, clientRole, null);
>>>>>>> f773c602c... Test pr 10 (#27)
        return Response.ok().build();
    }

    public Response updateFunction(String tenant, String namespace, String functionName, InputStream uploadedInputStream,
                                   FormDataContentDisposition fileDetail, String functionPkgUrl, String
<<<<<<< HEAD
                                           functionDetailsJson, String functionConfigJson, String clientAppId) {
        delegate.updateFunction(tenant, namespace, functionName, uploadedInputStream, fileDetail,
                functionPkgUrl, functionDetailsJson, functionConfigJson, clientAppId);
=======
                                           functionDetailsJson, String clientRole) {

        Function.FunctionDetails.Builder functionDetailsBuilder = Function.FunctionDetails.newBuilder();
        try {
            FunctionCommon.mergeJson(functionDetailsJson, functionDetailsBuilder);
        } catch (IOException e) {
            throw new RestException(Response.Status.BAD_REQUEST, e.getMessage());
        }
        FunctionConfig functionConfig = FunctionConfigUtils.convertFromDetails(functionDetailsBuilder.build());

        delegate.updateFunction(tenant, namespace, functionName, uploadedInputStream, fileDetail,
                functionPkgUrl, functionConfig, clientRole, null, null);
>>>>>>> f773c602c... Test pr 10 (#27)
        return Response.ok().build();
    }

    public Response deregisterFunction(String tenant, String namespace, String functionName, String clientAppId) {
<<<<<<< HEAD
        delegate.deregisterFunction(tenant, namespace, functionName, clientAppId);
        return Response.ok().build();
    }

    public Response listFunctions(String tenant, String namespace) {
        Collection<String> functionStateList = delegate.listFunctions( tenant, namespace);
=======
        delegate.deregisterFunction(tenant, namespace, functionName, clientAppId, null);
        return Response.ok().build();
    }

    public Response listFunctions(String tenant, String namespace, String clientRole) {
        Collection<String> functionStateList = delegate.listFunctions( tenant, namespace, clientRole, null);
>>>>>>> f773c602c... Test pr 10 (#27)
        return Response.status(Response.Status.OK).entity(new Gson().toJson(functionStateList.toArray())).build();
    }

    public Response triggerFunction(String tenant, String namespace, String functionName, String triggerValue,
<<<<<<< HEAD
                                    InputStream triggerStream, String topic) {
        String result = delegate.triggerFunction(tenant, namespace, functionName, triggerValue, triggerStream, topic);
        return Response.status(Response.Status.OK).entity(result).build();
    }

    public Response getFunctionState(String tenant, String namespace, String functionName, String key) {
        FunctionState functionState = delegate.getFunctionState(
                tenant, namespace, functionName, key);
=======
                                    InputStream triggerStream, String topic, String clientRole) {
        String result = delegate.triggerFunction(tenant, namespace, functionName, triggerValue, triggerStream, topic, clientRole, null);
        return Response.status(Response.Status.OK).entity(result).build();
    }

    public Response getFunctionState(String tenant, String namespace, String functionName, String key, String clientRole) {
        FunctionState functionState = delegate.getFunctionState(
                tenant, namespace, functionName, key, clientRole, null);
>>>>>>> f773c602c... Test pr 10 (#27)

        String value;
        if (functionState.getNumberValue() != null) {
            value = "value : " + functionState.getNumberValue() + ", version : " + functionState.getVersion();
        } else {
            value = "value : " + functionState.getStringValue() + ", version : " + functionState.getVersion();
        }
        return Response.status(Response.Status.OK)
                .entity(value)
                .build();
    }

    public Response restartFunctionInstance(String tenant, String namespace, String functionName, String instanceId, URI
<<<<<<< HEAD
            uri) {
        delegate.restartFunctionInstance(tenant, namespace, functionName, instanceId, uri);
        return Response.ok().build();
    }

    public Response restartFunctionInstances(String tenant, String namespace, String functionName) {
        delegate.restartFunctionInstances(tenant, namespace, functionName);
=======
            uri, String clientRole) {
        delegate.restartFunctionInstance(tenant, namespace, functionName, instanceId, uri, clientRole, null);
        return Response.ok().build();
    }

    public Response restartFunctionInstances(String tenant, String namespace, String functionName, String clientRole) {
        delegate.restartFunctionInstances(tenant, namespace, functionName, clientRole, null);
>>>>>>> f773c602c... Test pr 10 (#27)
        return Response.ok().build();
    }

    public Response stopFunctionInstance(String tenant, String namespace, String functionName, String instanceId, URI
<<<<<<< HEAD
            uri) {
        delegate.stopFunctionInstance(tenant, namespace, functionName, instanceId, uri);
        return Response.ok().build();
    }

    public Response stopFunctionInstances(String tenant, String namespace, String functionName) {
        delegate.stopFunctionInstances(tenant, namespace, functionName);
        return Response.ok().build();
    }

    public Response uploadFunction(InputStream uploadedInputStream, String path) {
        delegate.uploadFunction(uploadedInputStream, path);
        return Response.ok().build();
    }

    public Response downloadFunction(String path) {
        return Response.status(Response.Status.OK).entity(delegate.downloadFunction(path)).build();
=======
            uri, String clientRole) {
        delegate.stopFunctionInstance(tenant, namespace, functionName, instanceId, uri, clientRole ,null);
        return Response.ok().build();
    }

    public Response stopFunctionInstances(String tenant, String namespace, String functionName, String clientRole) {
        delegate.stopFunctionInstances(tenant, namespace, functionName, clientRole, null);
        return Response.ok().build();
    }

    public Response uploadFunction(InputStream uploadedInputStream, String path, String clientRole) {
        delegate.uploadFunction(uploadedInputStream, path, clientRole);
        return Response.ok().build();
    }

    public Response downloadFunction(String path, String clientRole) {
        return Response.status(Response.Status.OK).entity(delegate.downloadFunction(path, clientRole, null)).build();
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    public List<ConnectorDefinition> getListOfConnectors() {
        return delegate.getListOfConnectors();
    }

    private InstanceCommunication.FunctionStatus toProto(
            org.apache.pulsar.common.policies.data.FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData
                    functionInstanceStatus, String instanceId) {
        List<InstanceCommunication.FunctionStatus.ExceptionInformation> latestSysExceptions
                = functionInstanceStatus.getLatestSystemExceptions()
                .stream()
                .map(exceptionInformation -> InstanceCommunication.FunctionStatus.ExceptionInformation.newBuilder()
                        .setExceptionString(exceptionInformation.getExceptionString())
                        .setMsSinceEpoch(exceptionInformation.getTimestampMs())
                        .build())
                .collect(Collectors.toList());

        List<InstanceCommunication.FunctionStatus.ExceptionInformation> latestUserExceptions
                = functionInstanceStatus.getLatestUserExceptions()
                .stream()
                .map(exceptionInformation -> InstanceCommunication.FunctionStatus.ExceptionInformation.newBuilder()
                        .setExceptionString(exceptionInformation.getExceptionString())
                        .setMsSinceEpoch(exceptionInformation.getTimestampMs())
                        .build())
                .collect(Collectors.toList());


        InstanceCommunication.FunctionStatus functionStatus = InstanceCommunication.FunctionStatus.newBuilder()
                .setRunning(functionInstanceStatus.isRunning())
                .setFailureException(functionInstanceStatus.getError())
                .setNumRestarts(functionInstanceStatus.getNumRestarts())
                .setNumSuccessfullyProcessed(functionInstanceStatus.getNumSuccessfullyProcessed())
                .setNumUserExceptions(functionInstanceStatus.getNumUserExceptions())
                .addAllLatestUserExceptions(latestUserExceptions)
                .setNumSystemExceptions(functionInstanceStatus.getNumSystemExceptions())
                .addAllLatestSystemExceptions(latestSysExceptions)
                .setAverageLatency(functionInstanceStatus.getAverageLatency())
                .setLastInvocationTime(functionInstanceStatus.getLastInvocationTime())
                .setInstanceId(instanceId)
                .setWorkerId(delegate.worker().getWorkerConfig().getWorkerId())
                .build();

        return functionStatus;
    }
}