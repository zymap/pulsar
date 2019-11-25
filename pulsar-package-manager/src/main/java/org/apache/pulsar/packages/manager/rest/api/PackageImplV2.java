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

package org.apache.pulsar.packages.manager.rest.api;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.packages.manager.PackageMetadata;
import org.apache.pulsar.packages.manager.exception.*;
import org.apache.pulsar.packages.manager.impl.PackageImpl;
import org.apache.pulsar.packages.manager.naming.PackageName;
import org.apache.pulsar.packages.manager.naming.PackageType;
import org.apache.pulsar.packages.manager.service.PackageManagerService;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

@Slf4j
public class PackageImplV2 {

    private final PackageImpl packageImpl;
    private final Supplier<PackageManagerService> packageManagerServiceSupplier;

    public PackageImplV2(Supplier<PackageManagerService> packageStorageSupplier) {
        this.packageManagerServiceSupplier = packageStorageSupplier;
        this.packageImpl = packageStorageSupplier.get().getPackageImpl();
    }

    public Response getPackageMeta(String type, String tenant, String namespace, String name, String version) {
        try {
            PackageName packageName = PackageName.get(type, tenant, namespace, name, version);
            PackageMetadata meta = packageImpl.getMeta(packageName).get();
            return Response.status(Response.Status.OK).entity(meta.toJSON()).build();
        } catch (InterruptedException | ExecutionException | RuntimeException e) {
            return checkError(e);
        }
    }

    public Response updatePackageMeta(String type, String tenant, String namespace, String name, String version, PackageMetadata packageMetadata) {
        try {
            PackageName packageName = PackageName.get(type, tenant, namespace, name, version);
            packageImpl.updateMeta(packageName, packageMetadata).get();
            return Response.status(Response.Status.OK).build();
        } catch (InterruptedException | ExecutionException | RuntimeException e) {
            return checkError(e);
        }
    }

    public Response download(String type, String tenant, String namespace, String name, String version) {
        StreamingOutput output;
        try {
            PackageName packageName = PackageName.get(type, tenant, namespace, name, version);
            output = outputStream -> {
                try {
                    packageImpl.download(packageName, outputStream).get();
                } catch (InterruptedException | ExecutionException e) {
                    if (e.getCause() instanceof PackageAlreadyExistsException
                        || e.getCause() instanceof PackageMetaAlreadyExistsException) {
                        throw new WebApplicationException(e.getMessage(), Response.Status.FORBIDDEN);
                    } else if (e.getCause() instanceof PackageNotFoundException
                        || e.getCause() instanceof PackageMetaNotFoundException) {
                        throw new WebApplicationException(e.getMessage(), Response.Status.NOT_FOUND);
                    } else {
                        throw new WebApplicationException(e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR);
                    }
                }
            };
            return Response.status(Response.Status.OK).entity(output).build();
        } catch (RuntimeException e) {
            return checkError(e);
        }
    }

    public Response upload(String type, String tenant, String namespace, String name, String version, PackageMetadata metadata, InputStream inputStream) {
        try {
            PackageName packageName = PackageName.get(type, tenant, namespace, name, version);
            packageImpl.upload(packageName, metadata, inputStream).get();
            return Response.status(Response.Status.OK).build();
        } catch (InterruptedException | ExecutionException | RuntimeException e) {
            return checkError(e);
        }
    }

    public Response delete(String type, String tenant, String namespace, String name, String version) {
        try {
            PackageName packageName = PackageName.get(type, tenant, namespace, name, version);
            packageImpl.delete(packageName).get();
            return Response.status(Response.Status.OK).build();
        } catch (InterruptedException | ExecutionException | RuntimeException e) {
            return checkError(e);
        }
    }

    public Response list(String type, String tenant, String namespace, String name) {
        try {
            PackageName packageName = PackageName.get(type, tenant, namespace, name);
            List<PackageName> packageNameList = packageImpl.list(packageName).get();
            return Response.status(Response.Status.OK).entity(new Gson().toJson(packageNameList)).build();
        } catch (InterruptedException | ExecutionException | RuntimeException e) {
            return checkError(e);
        }
    }

    public Response list(String type, String tenant, String namespace) {
        try {
            List<PackageName> packageNames = packageImpl
                .list(PackageType.getEnum(type), NamespaceName.get(tenant, namespace)).get();
            return Response.status(Response.Status.OK).entity(new Gson().toJson(packageNames)).build();
        } catch (Exception e) {
            return checkError(e);
        }
    }

    public Response list(String tenant, String namespace) {
        List<PackageName> packages = new ArrayList<>();
        try {
            NamespaceName nsName = NamespaceName.get(tenant, namespace);
            for (PackageType value : PackageType.values()) {
                packages.addAll(packageImpl.list(value, nsName).get());
            }
        } catch (InterruptedException | ExecutionException | RuntimeException e) {
            return checkError(e);
        }
        return Response.status(Response.Status.OK).entity(new Gson().toJson(packages)).build();
    }

    private Response checkError(Exception e) {
        if (e.getCause() instanceof PackageAlreadyExistsException
            || e.getCause() instanceof PackageMetaAlreadyExistsException) {
            return Response.status(Response.Status.FORBIDDEN).entity(e.getMessage()).build();
        } else if (e.getCause() instanceof PackageNotFoundException
            || e.getCause() instanceof PackageMetaNotFoundException) {
            return Response.status(Response.Status.NOT_FOUND).entity(e.getMessage()).build();
        } else if (e.getCause() instanceof RuntimeException) {
            return Response.status(Response.Status.PRECONDITION_FAILED).entity(e.getMessage()).build();
        } else {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
        }
    }
}
