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
package org.apache.pulsar.broker.admin.v3;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.pulsar.broker.admin.impl.PackageManagerBase;
import org.apache.pulsar.packages.manager.PackageMetadata;
import org.apache.pulsar.packages.manager.naming.PackageName;
import org.glassfish.jersey.media.multipart.FormDataParam;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.util.List;

@Path("/packages")
@Api(value = "packages", tags = "packages")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class Packages extends PackageManagerBase {

    @GET
    @Path("/hello/{message}")
    public String hello(final @PathParam("message") String message) {
        if (getPackageManager() != null) {
            return message;
        }
        return "failed";
    }

    @GET
    @Path("/{type}/{tenant}/{namespace}/{packageName}/{version}")
    public PackageMetadata getMeta(
        final @PathParam("type") String type,
        final @PathParam("tenant") String tenant,
        final @PathParam("namespace") String namespace,
        final @PathParam("packageName") String packageName,
        final @PathParam("version") String version
    ) {
        return internalGetMeta(type, tenant, namespace, packageName, version);
    }

    @PUT
    @Path("/{type}/{tenant}/{namespace}/{packageName}/{version}/metadata")
    public void uploadMeta(
        final @PathParam("type") String type,
        final @PathParam("tenant") String tenant,
        final @PathParam("namespace") String namespace,
        final @PathParam("packageName") String packageName,
        final @PathParam("version") String version,
        final @FormDataParam("metadata") PackageMetadata meta
    ) {
        PackageMetadata metadata = PackageMetadata.builder().description("test").build();
        internalUploadMeta(type, tenant, namespace, packageName, version, metadata);
    }

    @POST
    @Path("/{type}/{tenant}/{namespace}/{packageName}/{version}/metadata")
    public void updateMeta(
        final @PathParam("type") String type,
        final @PathParam("tenant") String tenant,
        final @PathParam("namespace") String namespace,
        final @PathParam("packageName") String packageName,
        final @PathParam("version") String version,
        final @FormDataParam("metadata") PackageMetadata meta
    ) {
        PackageMetadata metadata = PackageMetadata.builder().description("test").build();
        internalUploadMeta(type, tenant, namespace, packageName, version, metadata);
    }

    @POST
    @Path("/{type}/{tenant}/{namespace}/{packageName}/{version}")
    @ApiOperation(value = "Upload a packages")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void upload(
        final @PathParam("type") String type,
        final @PathParam("tenant") String tenant,
        final @PathParam("namespace") String namespace,
        final @PathParam("packageName") String packageName,
        final @PathParam("version") String version,
        final @FormDataParam("metadata") PackageMetadata packageMetadata,
        final @FormDataParam("data") InputStream uploadedInputStream) {
        internalUpload(type, tenant, namespace, packageName, version, packageMetadata, uploadedInputStream);
    }

    @GET
    @Path("/download/{type}/{tenant}/{namespace}/{packageName}/{version}")
    public Response download(
        final @PathParam("type") String type,
        final @PathParam("tenant") String tenant,
        final @PathParam("namespace") String namespace,
        final @PathParam("packageName") String packageName,
        final @PathParam("version") String version
    ) {
        return Response.status(Response.Status.OK)
            .entity(internalDownload(type, tenant, namespace, packageName, version)).build();
    }

    @DELETE
    @Path("/{type}/{tenant}/{namespace}/{packageName}/{version}")
    @ApiOperation(value = "Upload a packages")
    public void delete(
        final @PathParam("type") String type,
        final @PathParam("tenant") String tenant,
        final @PathParam("namespace") String namespace,
        final @PathParam("packageName") String packageName,
        final @PathParam("version") String version
        ){
        internalDelete(type, tenant, namespace, packageName, version);
    }

    @GET
    @Path("/{type}/{tenant}/{namespace}/{packageName}/{version}")
    public List<PackageName> listPackageVersion(
        final @PathParam("type") String type,
        final @PathParam("tenant") String tenant,
        final @PathParam("namespace") String namespace,
        final @PathParam("packageName") String packageName,
        final @PathParam("version") String version
    ) {
        return internalList(type, tenant, namespace, packageName, version);
    }

    @GET
    @Path("/{type}/{tenant}/{namespace}")
    public List<PackageName> listPackages(
        final @PathParam("type") String type,
        final @PathParam("tenant") String tenant,
        final @PathParam("namespace") String namespace,
    ) {
        return internalList(type, tenant, namespace);
    }
}
