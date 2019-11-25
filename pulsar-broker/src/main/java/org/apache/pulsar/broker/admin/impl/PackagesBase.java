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

package org.apache.pulsar.broker.admin.impl;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.packages.manager.PackageMetadata;
import org.apache.pulsar.packages.manager.rest.api.PackageImplV2;
import org.apache.pulsar.packages.manager.service.PackageManagerService;
import org.glassfish.jersey.media.multipart.FormDataParam;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.util.function.Supplier;

@Path("/packages")
@Api(value = "/packages", description = "PackagesBase admin apis", tags = "packages")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class PackagesBase extends AdminResource implements Supplier<PackageManagerService> {

    private final PackageImplV2 packages;

    public PackagesBase(PackageImplV2 packages) {
        this.packages = packages;
    }

    @Override
    public PackageManagerService get() {
        return pulsar().getPackageManagerService();
    }

    @GET
    @Path("/{type}/{tenant}/{namespace}/{name}/{version}")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = ""),
        @ApiResponse(code = 403, message = "The package or the package metadata already exists."),
        @ApiResponse(code = 404, message = "The package or the package metadata does not exist."),
        @ApiResponse(code = 412, message = "The package or namespace name is not valid."),
        @ApiResponse(code = 500, message = "Internal server error.")})
    public Response getPackageMetadata(
        @PathParam("type") String type,
        @PathParam("tenant") String tenant,
        @PathParam("namespace") String namespace,
        @PathParam("name") String packageName,
        @PathParam("version") String version) {

        return packages.getPackageMeta(type, tenant, namespace, packageName, version);
    }

    @POST
    @Path("/{type}/{tenant}/{namespace}/{name}/{version}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @ApiOperation(value = "")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = ""),
        @ApiResponse(code = 403, message = "The package or the package metadata already exists."),
        @ApiResponse(code = 404, message = "The package or the package metadata does not exist."),
        @ApiResponse(code = 412, message = "The package or namespace name is not valid."),
        @ApiResponse(code = 500, message = "Internal server error.")})
    public Response updatePackageMetadata(
        @PathParam("type") String type,
        @PathParam("tenant") String tenant,
        @PathParam("namespace") String namespace,
        @PathParam("name") String packageName,
        @PathParam("version") String version,
        @FormDataParam("meta") PackageMetadata metadata) {

        return packages.updatePackageMeta(type, tenant, namespace, packageName, version, metadata);
    }

    @GET
    @Path("/{type}/{tenant}/{namespace}/{name}/{version}")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = ""),
        @ApiResponse(code = 403, message = "The package or the package metadata already exists."),
        @ApiResponse(code = 404, message = "The package or the package metadata does not exist."),
        @ApiResponse(code = 412, message = "The package or namespace name is not valid."),
        @ApiResponse(code = 500, message = "Internal server error.")})
    public Response downloadPackage(
        @PathParam("type") String type,
        @PathParam("tenant") String tenant,
        @PathParam("namespace") String namespace,
        @PathParam("name") String packageName,
        @PathParam("version") String version) {

        return packages.download(type, tenant, namespace, packageName, version);
    }


    @POST
    @Path("/{type}/{tenant}/{namespace}/{name}/{version}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @ApiOperation(value = "")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = ""),
        @ApiResponse(code = 403, message = "The package or the package metadata already exists."),
        @ApiResponse(code = 404, message = "The package or the package metadata does not exist."),
        @ApiResponse(code = 412, message = "The package or namespace name is not valid."),
        @ApiResponse(code = 500, message = "Internal server error.")})
    public Response uploadPackage(
        @PathParam("type") String type,
        @PathParam("tenant") String tenant,
        @PathParam("namespace") String namespace,
        @PathParam("name") String packageName,
        @PathParam("version") String version,
        @FormDataParam("data") InputStream inputStream,
        @FormDataParam("meta") PackageMetadata metadata) {

        return packages.upload(type, tenant, namespace, packageName, version, metadata, inputStream);
    }

    @DELETE
    @Path("/{type}/{tenant}/{namespace}/{name}/{version}")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = ""),
        @ApiResponse(code = 403, message = "The package or the package metadata already exists."),
        @ApiResponse(code = 404, message = "The package or the package metadata does not exist."),
        @ApiResponse(code = 412, message = "The package or namespace name is not valid."),
        @ApiResponse(code = 500, message = "Internal server error.")})
    public Response deletePackage(
        @PathParam("type") String type,
        @PathParam("tenant") String tenant,
        @PathParam("namespace") String namespace,
        @PathParam("name") String packageName,
        @PathParam("version") String version) {

        return packages.delete(type, tenant, namespace, packageName, version);
    }

    @GET
    @Path("/{type}/{tenant}/{namespace}/{name}")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = ""),
        @ApiResponse(code = 403, message = "The package or the package metadata already exists."),
        @ApiResponse(code = 404, message = "The package or the package metadata does not exist."),
        @ApiResponse(code = 412, message = "The package or namespace name is not valid."),
        @ApiResponse(code = 500, message = "Internal server error.")})
    public Response listAllVersionOfPackages(
        @PathParam("type") String type,
        @PathParam("tenant") String tenant,
        @PathParam("namespace") String namespace,
        @PathParam("name") String packageName) {

        return packages.list(type, tenant, namespace, packageName);
    }

    @GET
    @Path("/{type}/{tenant}/{namespace}")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = ""),
        @ApiResponse(code = 403, message = "The package or the package metadata already exists."),
        @ApiResponse(code = 404, message = "The package or the package metadata does not exist."),
        @ApiResponse(code = 412, message = "The package or namespace name is not valid."),
        @ApiResponse(code = 500, message = "Internal server error.")})
    public Response listPackagesOfNamespace(
        @PathParam("type") String type,
        @PathParam("tenant") String tenant,
        @PathParam("namespace") String namespace) {

        return packages.list(type, tenant, namespace);
    }

    @GET
    @Path("/{tenant}/{namespace}")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = ""),
        @ApiResponse(code = 404, message = "There are no packages in the namespace."),
        @ApiResponse(code = 412, message = "The namespace name is not valid."),
        @ApiResponse(code = 500, message = "Internal server error.")})
    public Response listAllPackagesOfNamespace(
        @PathParam("tenant") String tenant,
        @PathParam("namespace") String namespace) {

        return packages.list(tenant, namespace);
    }
}
