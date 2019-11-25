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

package org.apache.pulsar.admin.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.Packages;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.packages.manager.PackageMetadata;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@Parameters(commandDescription = "Operations about packages")
public class CmdPackages extends CmdBase{
    private final Packages packages;

    public CmdPackages(PulsarAdmin admin) {
        super("packages", admin);
        packages = admin.packages();

        jcommander.addCommand("get-metadata", new GetPackageMetadata());
        jcommander.addCommand("update-metadata", new UpdatePackageMetadata());
        jcommander.addCommand("download", new DownloadPackage());
        jcommander.addCommand("upload", new UploadPackage());
        jcommander.addCommand("delete", new DeletePackage());
        jcommander.addCommand("list", new ListPackages());
    }

    @Parameters(commandDescription = "Get the metadata of a package.")
    private class GetPackageMetadata extends CliCommand{
        @Parameter(
            description = "type://tenant/namespace/package-name@version",
            required= true)
        private java.util.List<String> params;

        @Override
        void run() throws Exception {
            String packageName = validatePackageName(params);
            print(packages.getMetadata(packageName).toJSON());
        }
    }

    @Parameters(commandDescription = "Update the metadata of a package.")
    private class UpdatePackageMetadata extends CliCommand {
        @Parameter(
            description = "type://tenant/namespace/package-name@version",
            required = true)
        private java.util.List<String> params;

        @Parameter(names = "-m, --metadata", description = "JSON string of a package metadata.")
        private String metadataJSON;

        @Parameter(names = "-d, --description", description = "Update the description of a package.")
        private String metaDescription;

        @Parameter(names = "-c, --contact", description = "Update the contact of a package.")
        private String metaContact;

        @Parameter(names = "-p, --properties", description = "Update the properties of a package")
        private String metaProperties;

        @Override
        void run() throws Exception {
            if (StringUtils.isNotBlank(metadataJSON)) {
                if (StringUtils.isNoneBlank(metaDescription, metaContact, metaProperties)) {
                    throw new PulsarAdminException("The metadata and other flag can not specified at the same time.");
                }
            }
            String packageName = validatePackageName(params);
            PackageMetadata packageMetadata = packages.getMetadata(packageName);

            PackageMetadata newMetadata = packageMetadata;
            if (StringUtils.isNotBlank(metadataJSON)) {
                PackageMetadata updateMetadata = new Gson().fromJson(metadataJSON, PackageMetadata.class);
                newMetadata.setDescription(updateMetadata.getDescription());
                newMetadata.setContact(updateMetadata.getContact());
                newMetadata.setProperties(updateMetadata.getProperties());
            } else if (StringUtils.isNotBlank(metaDescription)) {
                newMetadata.setDescription(metaDescription);
            } else if (StringUtils.isNotBlank(metaContact)) {
                newMetadata.setContact(metaContact);
            } else if (StringUtils.isNotBlank(metaProperties)) {
                newMetadata.setProperties(new Gson()
                    .fromJson(metaProperties, new TypeToken<Map<String, String>>() {}.getType()));
            }

            packages.updateMetadata(packageName, newMetadata);
            print(String.format("Update package metadata from %s to %s successfully.", packageMetadata.toJSON(), newMetadata.toJSON()));
        }
    }

    @Parameters(commandDescription = "Download a package from the package storage to the local.")
    private class DownloadPackage extends CliCommand {
        @Parameter(
            description = "type://tenant/namespace/package-name@version",
            required = true)
        private List<String> params;

        @Parameter(names = "-p, --path", description = "The file download to the path.", required = true)
        private String path;

        @Override
        void run() throws Exception {
            String packageName = validatePackageName(params);
            packages.download(packageName, path);
            print(String.format("Download package %s to %s successfully.", packageName, path));
        }
    }

    @Parameters(commandDescription = "Upload a file to the package storage.")
    private class UploadPackage extends CliCommand {
        @Parameter(
            description = "type://tenant/namespace/package-name@version",
            required = true)
        private List<String> params;

        @Parameter(names = "-m, --metadata", description = "The package metadata info.", required = true)
        private String metadata;

        @Parameter(names = "-p, --path", description = "The upload package location.", required = true)
        private String path;

        @Override
        void run() throws Exception {
            String packageName = validatePackageName(params);
            PackageMetadata packageMetadata = new Gson().fromJson(metadata, PackageMetadata.class);
            packages.upload(packageName, packageMetadata, path);
            print(String.format("Upload package %s from %s successfully.", packageName, path));
        }
    }

    @Parameters(commandDescription = "Delete a package.")
    private class DeletePackage extends CliCommand{
        @Parameter(
            description = "type://tenant/namespace/package-name@version",
            required = true)
        private java.util.List<String> params;

        @Override
        void run() throws Exception {
            String packageName = validatePackageName(params);
            packages.delete(packageName);
            print(String.format("Delete package %s successfully.", packageName));
        }
    }

    @Parameters(commandDescription = "List all packages in a namespace or list all packages with the " +
        "specified type in a namespace or list all version of a package.")
    private class ListPackages extends CliCommand{
        @Parameter(
            description = "tenant/namespace", required = true)
        private java.util.List<String> params;

        @Parameter(description = "List all packages with the specified type.", names = "-t, --type")
        private String type;

        @Parameter(
            description = "List all versions of the specified package. Need to specified type at the same time.",
            names = "-n, --package-name")
        private String packageName;

        @Override
        void run() throws Exception {
            String nsName = validateNamespace(params);

            List<String> packageList;
            if (StringUtils.isNotBlank(packageName) && StringUtils.isBlank(type)) {
                throw new PulsarAdminException("Unsupported operation. If you want to list all versions of a " +
                    "package, please specified the type of the packages. Otherwise please use `pulsar-admin " +
                    "packages` to check out how to use the command.");
            } else if (StringUtils.isNotBlank(type) && StringUtils.isNotBlank(packageName)) {
                packageList = packages.listVersionsOfPackages(type + "://" + nsName + "/" + packageName);
            } else if (StringUtils.isNotBlank(type) && StringUtils.isBlank(packageName)) {
                packageList = packages.listPackagesWithType(type, nsName);
            } else if (StringUtils.isBlank(type) && StringUtils.isBlank(packageName)) {
                packageList = packages.listAllPackages(nsName);
            } else {
                throw new PulsarAdminException("Unsupported operation. List can list all packages in a namespace " +
                    "or list all packages with the specified type in a namespace or list all versions of a package. " +
                    "Please check your command option.");
            }

            print(new Gson().toJson(packageList));
        }
    }
}
