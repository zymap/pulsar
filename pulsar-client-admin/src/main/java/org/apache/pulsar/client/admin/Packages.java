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

package org.apache.pulsar.client.admin;

import org.apache.pulsar.packages.manager.PackageMetadata;
import org.apache.pulsar.packages.manager.naming.PackageName;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Packages APIs of admin client.
 */
public interface Packages {

    PackageMetadata getMetadata(String packageName) throws PulsarAdminException;

    CompletableFuture<PackageMetadata> getMetadataAsync(String packageName);

    void updateMetadata(String packageName, PackageMetadata metadata) throws PulsarAdminException;

    CompletableFuture<Void> updateMetadataAsync(String packageName, PackageMetadata metadata);

    void download(String packageName, String path) throws PulsarAdminException;

    CompletableFuture<Void> downloadAsync(String packageName, String path);

    void upload(String packageName, PackageMetadata metadata, String path) throws PulsarAdminException;

    CompletableFuture<Void> uploadAsync(String packageName, PackageMetadata metadata, String path);

    void delete(String packageName) throws PulsarAdminException;

    CompletableFuture<Void> deleteAsync(String packageName);

    List<String> listVersionsOfPackages(String packageNameWithoutVersion) throws PulsarAdminException;

    CompletableFuture<List<String>> listVersionsOfPackagesAsync(String packageNameWithoutVersion);

    List<String> listPackagesWithType(String type, String namespace) throws PulsarAdminException;

    CompletableFuture<List<String>> listPackagesWithTypeAsync(String type, String namespace);

    List<String> listAllPackages(String namespace) throws PulsarAdminException;

    CompletableFuture<List<String>> listAllPackagesAsync(String namespace);
}
