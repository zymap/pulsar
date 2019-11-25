/*
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
 *
 */

package org.apache.pulsar.client.admin.internal;

import org.apache.pulsar.client.admin.Packages;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.packages.manager.PackageMetadata;
import org.apache.pulsar.packages.manager.naming.PackageName;
import org.asynchttpclient.*;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.rmi.UnexpectedException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.request.body.multipart.FilePart;
import org.asynchttpclient.request.body.multipart.StringPart;

import static org.asynchttpclient.Dsl.*;

public class PackagesImpl extends ComponentResource implements Packages {
    private final WebTarget adminPackages;
    private final AsyncHttpClient asyncHttpClient;

    public PackagesImpl(WebTarget webTarget, Authentication authentication, AsyncHttpClient asyncHttpClient, long readTimeoutMs) {
        super(authentication, readTimeoutMs);
        this.adminPackages = webTarget.path("/admin/v1/packages");
        this.asyncHttpClient = asyncHttpClient;
    }

    @Override
    public PackageMetadata getMetadata(String packageName) throws PulsarAdminException {
        try {
            return getMetadataAsync(packageName).get();
        } catch (InterruptedException | ExecutionException e) {
            throw getApiException(e);
        }
    }

    @Override
    public CompletableFuture<PackageMetadata> getMetadataAsync(String packageName) {
        try {
            PackageName name = PackageName.get(packageName);
            return requestAsync(adminPackages.path(packageRestPath(name)))
                .thenApply(builder -> builder.get(PackageMetadata.class));
        } catch (RuntimeException e) {
            return FutureUtil.failedFuture(getApiException(e));
        }
    }

    @Override
    public void updateMetadata(String packageName, PackageMetadata metadata) throws PulsarAdminException {
        try {
            updateMetadataAsync(packageName, metadata).get();
        } catch (InterruptedException | ExecutionException e) {
            throw getApiException(e);
        }
    }

    @Override
    public CompletableFuture<Void> updateMetadataAsync(String packageName, PackageMetadata metadata) {
        CompletableFuture<Void> resultFuture = new CompletableFuture<>();

        PackageName name = null;
        try {
            name = PackageName.get(packageName);
        } catch (RuntimeException e) {
            resultFuture.completeExceptionally(e);
        }

        if (name != null) {
            RequestBuilder builder = post(adminPackages.path(packageRestPath(name)).getUri().toASCIIString());
            builder.addBodyPart(new StringPart("meta", metadata.toJSON(), MediaType.APPLICATION_JSON));
            asyncHttpClient.executeRequest(builder.build()).toCompletableFuture()
                .whenComplete((response, throwable) -> {
                    if (throwable != null) {
                        resultFuture.completeExceptionally(throwable);
                        return;
                    }
                    if (response.getStatusCode() != Response.Status.OK.getStatusCode()) {
                        resultFuture.completeExceptionally(
                            getApiException(
                                Response.status(response.getStatusCode())
                                    .entity(response.getResponseBody())
                                    .build()));
                    }
                    resultFuture.complete(null);
                });
        } else {
            resultFuture.completeExceptionally(
                new UnexpectedException(String.format("Uploading package [%s] encounters an unexpected error, " +
                    "the object packageName is null", packageName)));
        }

        return resultFuture;
    }

    @Override
    public void download(String packageName, String path) throws PulsarAdminException {
        try {
            downloadAsync(packageName, path).get();
        } catch (InterruptedException | ExecutionException e) {
            throw getApiException(e);
        }
    }

    @Override
    public CompletableFuture<Void> downloadAsync(String packageName, String path) {
        CompletableFuture<Void> resultFuture = new CompletableFuture<>();

        PackageName name = null;
        try {
            name = PackageName.get(packageName);
        } catch (RuntimeException e) {
            resultFuture.completeExceptionally(e);
        }

        if (name != null) {
            RequestBuilder builder = get(adminPackages.path(packageRestPath(name)).getUri().toASCIIString());
            try {
                builder = addAuthHeaders(adminPackages, builder);
            } catch (PulsarAdminException e) {
                resultFuture.completeExceptionally(e);
            }
            asyncHttpClient.executeRequest(builder.build()).toCompletableFuture()
                .thenCombine(createFileAsOutputStreamAsync(path), this::writeResponseBodyToFile)
                .thenApply(ignore -> resultFuture.complete(null));
        } else {
            resultFuture.completeExceptionally(new UnknownError(String.format("Downloading package [%s] encounter an unknown error", packageName)));
        }

        return resultFuture;
    }

    private CompletableFuture<Void> writeResponseBodyToFile(org.asynchttpclient.Response response, FileOutputStream outputStream) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        CompletableFuture.runAsync(() -> {
            if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
                future.completeExceptionally(getApiException(Response.status(response.getStatusCode()).entity(response.getResponseBody()).build()));
                return;
            }
            try {
                outputStream.getChannel().write(response.getResponseBodyAsByteBuffer());
                future.complete(null);
            } catch (IOException e) {
                future.completeExceptionally(getApiException(e));
            }
        });

        return future;
    }

    private CompletableFuture<FileOutputStream> createFileAsOutputStreamAsync(String path) {
        CompletableFuture<FileOutputStream> future = new CompletableFuture<>();

        CompletableFuture.runAsync(() -> {
            File file = new File(path);
            try {
                if (file.createNewFile()) {
                    future.complete(new FileOutputStream(file));
                }
                if (file.delete()) {
                    if (file.createNewFile()) {
                        future.complete(new FileOutputStream(file));
                    } else {
                        future.completeExceptionally(new UnknownError(String.format("Creating file [%s] encounter unknown error", path)));
                    }
                } else {
                    future.completeExceptionally(new UnknownError(String.format("Deleting file [%s] encounter unknown error", path)));
                }
            } catch (IOException e) {
                future.completeExceptionally(e);
            }
        });

        return future;
    }

    @Override
    public void upload(String packageName, PackageMetadata metadata, String path) throws PulsarAdminException {
        try {
            uploadAsync(packageName, metadata, path).get();
        } catch (InterruptedException | ExecutionException e) {
            throw getApiException(e);
        }
    }

    @Override
    public CompletableFuture<Void> uploadAsync(String packageName, PackageMetadata metadata, String path) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        PackageName name = null;
        try {
            name = PackageName.get(packageName);
        } catch (RuntimeException e) {
            future.completeExceptionally(e);
        }

        if (name != null) {
            RequestBuilder builder = post(adminPackages.path(packageRestPath(name)).getUri().toASCIIString());
            if (metadata != null) {
                builder.addBodyPart(new StringPart("meta", metadata.toJSON(), MediaType.APPLICATION_JSON));
            }
            builder.addBodyPart(new FilePart("data", new File(path), MediaType.APPLICATION_OCTET_STREAM));

            try {
                builder = addAuthHeaders(adminPackages, builder);
            } catch (PulsarAdminException e) {
                future.completeExceptionally(e);
            }

            asyncHttpClient.executeRequest(builder).toCompletableFuture()
                .whenComplete((response, throwable) -> {
                    if (throwable != null) {
                        future.completeExceptionally(getApiException(throwable));
                        return;
                    }
                    if (response.getStatusCode() < Response.Status.OK.getStatusCode()
                        || response.getStatusCode() >= 300) {
                        future.completeExceptionally(
                            getApiException(
                                Response.status(response.getStatusCode()).entity(response.getResponseBody()).build()));
                        return;
                    }
                    future.complete(null);
                });
        } else {
            future.completeExceptionally(
                new UnexpectedException(String.format("Uploading package [%s] encounters an unexpected error, " +
                        "the object packageName is null", packageName)));
        }

        return future;
    }

    @Override
    public void delete(String packageName) throws PulsarAdminException {
        try {
            deleteAsync(packageName).get();
        } catch (InterruptedException | ExecutionException e) {
            throw getApiException(e);
        }
    }

    @Override
    public CompletableFuture<Void> deleteAsync(String packageName) {
        try {
            PackageName name = PackageName.get(packageName);
            return requestAsync(adminPackages.path(packageRestPath(name)))
                .thenAccept(builder -> builder.delete(ErrorData.class));
        } catch (RuntimeException e) {
            return FutureUtil.failedFuture(getApiException(e));
        }
    }

    @Override
    public List<String> listVersionsOfPackages(String packageNameWithoutVersion) throws PulsarAdminException {
        try {
            return listVersionsOfPackagesAsync(packageNameWithoutVersion).get();
        } catch (InterruptedException | ExecutionException e) {
            throw getApiException(e);
        }
    }

    @Override
    public CompletableFuture<List<String>> listVersionsOfPackagesAsync(String packageNameWithoutVersion) {
        try {
            PackageName name = PackageName.get(packageNameWithoutVersion);
            return requestAsync(adminPackages.path(packageWithoutVersionRestPath(name)))
                .thenApply(builder -> builder.get(new GenericType<List<PackageName>>(){}))
                .thenApply(packageNames -> packageNames.stream().map(PackageName::toString).collect(Collectors.toList()));
        } catch (RuntimeException e) {
            return FutureUtil.failedFuture(getApiException(e));
        }
    }

    @Override
    public List<String> listPackagesWithType(String type, String namespace) throws PulsarAdminException {
        try {
            return listPackagesWithTypeAsync(type, namespace).get();
        } catch (InterruptedException | ExecutionException e) {
            throw getApiException(e);
        }
    }

    @Override
    public CompletableFuture<List<String>> listPackagesWithTypeAsync(String type, String namespace) {
        try {
            NamespaceName nsName = NamespaceName.get(namespace);
            return requestAsync(adminPackages.path(type).path(nsName.getTenant()).path(nsName.getLocalName()))
                .thenApply(builder -> builder.get(new GenericType<List<PackageName>>() {}))
                .thenApply(packageNames -> packageNames.stream().map(PackageName::toString).collect(Collectors.toList()));
        } catch (RuntimeException e) {
            return FutureUtil.failedFuture(getApiException(e));
        }

    }

    @Override
    public List<String> listAllPackages(String namespace) throws PulsarAdminException {
        try {
            return listAllPackagesAsync(namespace).get();
        } catch (InterruptedException | ExecutionException e) {
            throw getApiException(e);
        }
    }

    @Override
    public CompletableFuture<List<String>> listAllPackagesAsync(String namespace) {
        try {
            NamespaceName nsName = NamespaceName.get(namespace);
            return requestAsync(adminPackages.path(nsName.getTenant()).path(nsName.getLocalName()))
                .thenApply(builder -> builder.get(new GenericType<List<PackageName>>() {}))
                .thenApply(packageNames -> packageNames.stream().map(PackageName::toString).collect(Collectors.toList()));
        } catch (RuntimeException e) {
            return FutureUtil.failedFuture(getApiException(e));
        }
    }

    private String packageRestPath(PackageName packageName) {
            return String.format("%s/%s/%s/%s/%s",
                packageName.getPkgType().toString(),
                packageName.getNamespaceName().getTenant(),
                packageName.getNamespaceName().getLocalName(),
                packageName.getName(),
                packageName.getVersion());
    }

    private String packageWithoutVersionRestPath(PackageName packageName) {
        return String.format("%s/%s/%s/%s",
            packageName.getPkgType().toString(),
            packageName.getNamespaceName().getTenant(),
            packageName.getNamespaceName().getLocalName(),
            packageName.getName());
    }
}

