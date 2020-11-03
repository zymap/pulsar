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
package org.apache.pulsar.broker.web;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;
<<<<<<< HEAD
import static org.apache.pulsar.zookeeper.ZooKeeperCache.cacheTimeOutInSec;
=======
>>>>>>> f773c602c... Test pr 10 (#27)

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Iterator;
<<<<<<< HEAD
import java.util.List;
=======
>>>>>>> f773c602c... Test pr 10 (#27)
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
<<<<<<< HEAD
=======
import java.util.concurrent.TimeoutException;
>>>>>>> f773c602c... Test pr 10 (#27)

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

<<<<<<< HEAD
=======
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.authentication.AuthenticationDataHttps;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
<<<<<<< HEAD
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.common.naming.*;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TenantInfo;
=======
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.common.naming.Constants;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundles;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PolicyName;
import org.apache.pulsar.common.policies.data.PolicyOperation;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantOperation;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

<<<<<<< HEAD
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

=======
>>>>>>> f773c602c... Test pr 10 (#27)
/**
 * Base class for Web resources in Pulsar. It provides basic authorization functions.
 */
public abstract class PulsarWebResource {

    private static final Logger log = LoggerFactory.getLogger(PulsarWebResource.class);

    static final String ORIGINAL_PRINCIPAL_HEADER = "X-Original-Principal";

    @Context
    protected ServletContext servletContext;

    @Context
    protected HttpServletRequest httpRequest;

    @Context
    protected UriInfo uri;

    private PulsarService pulsar;

    protected PulsarService pulsar() {
        if (pulsar == null) {
            pulsar = (PulsarService) servletContext.getAttribute(WebService.ATTRIBUTE_PULSAR_NAME);
        }

        return pulsar;
    }

    protected ServiceConfiguration config() {
        return pulsar().getConfiguration();
    }

    public static String path(String... parts) {
        StringBuilder sb = new StringBuilder();
        sb.append("/admin/");
        Joiner.on('/').appendTo(sb, parts);
        return sb.toString();
    }

    public static String joinPath(String... parts) {
        StringBuilder sb = new StringBuilder();
        Joiner.on('/').appendTo(sb, parts);
        return sb.toString();
    }

    public static String splitPath(String source, int slice) {
        Iterable<String> parts = Splitter.on('/').limit(slice).split(source);
        Iterator<String> s = parts.iterator();
<<<<<<< HEAD
        String result = new String();
=======
        String result = "";
>>>>>>> f773c602c... Test pr 10 (#27)
        for (int i = 0; i < slice; i++) {
            result = s.next();
        }
        return result;
    }

    /**
     * Gets a caller id (IP + role)
     *
     * @return the web service caller identification
     */
    public String clientAppId() {
        return (String) httpRequest.getAttribute(AuthenticationFilter.AuthenticatedRoleAttributeName);
    }

    public String originalPrincipal() {
        return httpRequest.getHeader(ORIGINAL_PRINCIPAL_HEADER);
    }

    public AuthenticationDataHttps clientAuthData() {
        return (AuthenticationDataHttps) httpRequest.getAttribute(AuthenticationFilter.AuthenticatedDataAttributeName);
    }

    public boolean isRequestHttps() {
    	return "https".equalsIgnoreCase(httpRequest.getScheme());
    }

    public static boolean isClientAuthenticated(String appId) {
        return appId != null;
    }

    private static void validateOriginalPrincipal(Set<String> proxyRoles, String authenticatedPrincipal,
                                                  String originalPrincipal) {
        if (proxyRoles.contains(authenticatedPrincipal)) {
            // Request has come from a proxy
            if (StringUtils.isBlank(originalPrincipal)) {
                log.warn("Original principal empty in request authenticated as {}", authenticatedPrincipal);
<<<<<<< HEAD
                throw new RestException(Status.UNAUTHORIZED, "Original principal cannot be empty if the request is via proxy.");               
            }
            if (proxyRoles.contains(originalPrincipal)) {
                log.warn("Original principal {} cannot be a proxy role ({})", originalPrincipal, proxyRoles);
                throw new RestException(Status.UNAUTHORIZED, "Original principal cannot be a proxy role");           
            } 
        }
    }

=======
                throw new RestException(Status.UNAUTHORIZED, "Original principal cannot be empty if the request is via proxy.");
            }
            if (proxyRoles.contains(originalPrincipal)) {
                log.warn("Original principal {} cannot be a proxy role ({})", originalPrincipal, proxyRoles);
                throw new RestException(Status.UNAUTHORIZED, "Original principal cannot be a proxy role");
            }
        }
    }

    protected boolean hasSuperUserAccess() {
        try {
            validateSuperUserAccess();
        } catch (Exception e) {
            return false;
        }
        return true;
    }

>>>>>>> f773c602c... Test pr 10 (#27)
    /**
     * Checks whether the user has Pulsar Super-User access to the system.
     *
     * @throws WebApplicationException
     *             if not authorized
     */
    protected void validateSuperUserAccess() {
        if (config().isAuthenticationEnabled()) {
            String appId = clientAppId();
            if(log.isDebugEnabled()) {
                log.debug("[{}] Check super user access: Authenticated: {} -- Role: {}", uri.getRequestUri(),
                        isClientAuthenticated(appId), appId);
            }
            String originalPrincipal = originalPrincipal();
            validateOriginalPrincipal(pulsar.getConfiguration().getProxyRoles(), appId, originalPrincipal);

            if (pulsar.getConfiguration().getProxyRoles().contains(appId)) {

                CompletableFuture<Boolean> proxyAuthorizedFuture;
                CompletableFuture<Boolean> originalPrincipalAuthorizedFuture;

                try {
                    proxyAuthorizedFuture = pulsar.getBrokerService()
                            .getAuthorizationService()
<<<<<<< HEAD
                            .isSuperUser(appId);

                    originalPrincipalAuthorizedFuture = pulsar.getBrokerService()
                            .getAuthorizationService()
                            .isSuperUser(originalPrincipal);
=======
                            .isSuperUser(appId, clientAuthData());

                    originalPrincipalAuthorizedFuture = pulsar.getBrokerService()
                            .getAuthorizationService()
                            .isSuperUser(originalPrincipal, clientAuthData());
>>>>>>> f773c602c... Test pr 10 (#27)

                    if (!proxyAuthorizedFuture.get() || !originalPrincipalAuthorizedFuture.get()) {
                        throw new RestException(Status.UNAUTHORIZED,
                                String.format("Proxy not authorized for super-user operation (proxy:%s,original:%s)",
                                              appId, originalPrincipal));
                    }
                } catch (InterruptedException | ExecutionException e) {
<<<<<<< HEAD
=======
                    log.error("Error validating super-user access : "+ e.getMessage(), e);
>>>>>>> f773c602c... Test pr 10 (#27)
                    throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
                }
                log.debug("Successfully authorized {} (proxied by {}) as super-user",
                          originalPrincipal, appId);
<<<<<<< HEAD
            } else if (!config().getSuperUserRoles().contains(appId)) {
                throw new RestException(Status.UNAUTHORIZED, "This operation requires super-user access");
=======
            } else {
                if (config().isAuthorizationEnabled() && !pulsar.getBrokerService()
                        .getAuthorizationService()
                        .isSuperUser(appId, clientAuthData())
                        .join()) {
                    throw new RestException(Status.UNAUTHORIZED, "This operation requires super-user access");
                }
                log.debug("Successfully authorized {} as super-user",
                          appId);
>>>>>>> f773c602c... Test pr 10 (#27)
            }
        }
    }

    /**
     * Checks that the http client role has admin access to the specified tenant.
     *
     * @param tenant
     *            the tenant id
     * @throws WebApplicationException
     *             if not authorized
     */
    protected void validateAdminAccessForTenant(String tenant) {
        try {
<<<<<<< HEAD
            validateAdminAccessForTenant(pulsar(), clientAppId(), originalPrincipal(), tenant);
=======
            validateAdminAccessForTenant(pulsar(), clientAppId(), originalPrincipal(), tenant, clientAuthData());
>>>>>>> f773c602c... Test pr 10 (#27)
        } catch (RestException e) {
            throw e;
        } catch (Exception e) {
            log.error("Failed to get tenant admin data for tenant {}", tenant);
            throw new RestException(e);
        }
    }

    protected static void validateAdminAccessForTenant(PulsarService pulsar, String clientAppId,
<<<<<<< HEAD
                                                       String originalPrincipal, String tenant)
=======
                                                       String originalPrincipal, String tenant,
                                                       AuthenticationDataSource authenticationData)
>>>>>>> f773c602c... Test pr 10 (#27)
            throws RestException, Exception {
        if (log.isDebugEnabled()) {
            log.debug("check admin access on tenant: {} - Authenticated: {} -- role: {}", tenant,
                    (isClientAuthenticated(clientAppId)), clientAppId);
        }

        TenantInfo tenantInfo;

        try {
            tenantInfo = pulsar.getConfigurationCache().propertiesCache().get(path(POLICIES, tenant))
                    .orElseThrow(() -> new RestException(Status.NOT_FOUND, "Tenant does not exist"));
        } catch (KeeperException.NoNodeException e) {
            log.warn("Failed to get tenant info data for non existing tenant {}", tenant);
            throw new RestException(Status.NOT_FOUND, "Tenant does not exist");
        }

        if (pulsar.getConfiguration().isAuthenticationEnabled() && pulsar.getConfiguration().isAuthorizationEnabled()) {
            if (!isClientAuthenticated(clientAppId)) {
                throw new RestException(Status.FORBIDDEN, "Need to authenticate to perform the request");
            }

            validateOriginalPrincipal(pulsar.getConfiguration().getProxyRoles(), clientAppId, originalPrincipal);

            if (pulsar.getConfiguration().getProxyRoles().contains(clientAppId)) {
<<<<<<< HEAD

                CompletableFuture<Boolean> isProxySuperUserFuture;
                CompletableFuture<Boolean> isOriginalPrincipalSuperUserFuture;
                try {
                    isProxySuperUserFuture = pulsar.getBrokerService()
                            .getAuthorizationService()
                            .isSuperUser(clientAppId);

                    isOriginalPrincipalSuperUserFuture = pulsar.getBrokerService()
                            .getAuthorizationService()
                            .isSuperUser(originalPrincipal);

                Set<String> adminRoles = tenantInfo.getAdminRoles();
                boolean proxyAuthorized = isProxySuperUserFuture.get() || adminRoles.contains(clientAppId);
                boolean originalPrincipalAuthorized
                    = isOriginalPrincipalSuperUserFuture.get() || adminRoles.contains(originalPrincipal);
=======
                CompletableFuture<Boolean> isProxySuperUserFuture;
                CompletableFuture<Boolean> isOriginalPrincipalSuperUserFuture;
                try {
                    AuthorizationService authorizationService = pulsar.getBrokerService().getAuthorizationService();
                    isProxySuperUserFuture = authorizationService.isSuperUser(clientAppId, authenticationData);

                    isOriginalPrincipalSuperUserFuture = authorizationService.isSuperUser(originalPrincipal, authenticationData);

                    boolean proxyAuthorized = isProxySuperUserFuture.get() || authorizationService.isTenantAdmin(tenant, clientAppId, tenantInfo, authenticationData).get();
                    boolean originalPrincipalAuthorized
                    = isOriginalPrincipalSuperUserFuture.get() || authorizationService.isTenantAdmin(tenant, originalPrincipal, tenantInfo, authenticationData).get();
>>>>>>> f773c602c... Test pr 10 (#27)
                    if (!proxyAuthorized || !originalPrincipalAuthorized) {
                        throw new RestException(Status.UNAUTHORIZED,
                                String.format("Proxy not authorized to access resource (proxy:%s,original:%s)",
                                              clientAppId, originalPrincipal));
                    }
                } catch (InterruptedException | ExecutionException e) {
                    throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
                }
                log.debug("Successfully authorized {} (proxied by {}) on tenant {}",
                          originalPrincipal, clientAppId, tenant);
<<<<<<< HEAD
            } else if (pulsar.getConfiguration().getSuperUserRoles().contains(clientAppId)) {
                // Super-user has access to configure all the policies
                log.debug("granting access to super-user {} on tenant {}", clientAppId, tenant);
            } else {

                if (!tenantInfo.getAdminRoles().contains(clientAppId)) {
                    throw new RestException(Status.UNAUTHORIZED,
                            "Don't have permission to administrate resources on this tenant");
=======
            } else {
                if (!pulsar.getBrokerService()
                        .getAuthorizationService()
                        .isSuperUser(clientAppId, authenticationData)
                        .join()) {
                    if (!pulsar.getBrokerService().getAuthorizationService().isTenantAdmin(tenant, clientAppId, tenantInfo, authenticationData).get()) {
                        throw new RestException(Status.UNAUTHORIZED,
                                "Don't have permission to administrate resources on this tenant");
                    }
>>>>>>> f773c602c... Test pr 10 (#27)
                }

                log.debug("Successfully authorized {} on tenant {}", clientAppId, tenant);
            }
        }
    }

    protected void validateClusterForTenant(String tenant, String cluster) {
        TenantInfo tenantInfo;
        try {
            tenantInfo = pulsar().getConfigurationCache().propertiesCache().get(path(POLICIES, tenant))
                    .orElseThrow(() -> new RestException(Status.NOT_FOUND, "Tenant does not exist"));
<<<<<<< HEAD
        } catch (Exception e) {
            log.error("Failed to get tenant admin data for tenant");
=======
        } catch (RestException e) {
            log.warn("Failed to get tenant admin data for tenant {}", tenant);
            throw e;
        } catch (Exception e) {
            log.error("Failed to get tenant admin data for tenant {}", tenant, e);
>>>>>>> f773c602c... Test pr 10 (#27)
            throw new RestException(e);
        }

        // Check if tenant is allowed on the cluster
        if (!tenantInfo.getAllowedClusters().contains(cluster)) {
            String msg = String.format("Cluster [%s] is not in the list of allowed clusters list for tenant [%s]",
                    cluster, tenant);
            log.info(msg);
            throw new RestException(Status.FORBIDDEN, msg);
        }
        log.info("Successfully validated clusters on tenant [{}]", tenant);
    }

    /**
     * Check if the cluster exists and redirect the call to the owning cluster
     *
     * @param cluster
     *            Cluster name
     * @throws Exception
     *             In case the redirect happens
     */
    protected void validateClusterOwnership(String cluster) throws WebApplicationException {
        try {
            ClusterData differentClusterData = getClusterDataIfDifferentCluster(pulsar(), cluster, clientAppId()).get();
            if (differentClusterData != null) {
                URI redirect = getRedirectionUrl(differentClusterData);
                // redirect to the cluster requested
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Redirecting the rest call to {}: cluster={}", clientAppId(), redirect, cluster);
<<<<<<< HEAD

=======
>>>>>>> f773c602c... Test pr 10 (#27)
                }
                throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
            }
        } catch (WebApplicationException wae) {
            throw wae;
        } catch (Exception e) {
            if (e.getCause() instanceof WebApplicationException) {
                throw (WebApplicationException) e.getCause();
            }
            throw new RestException(Status.SERVICE_UNAVAILABLE, String
                    .format("Failed to validate Cluster configuration : cluster=%s  emsg=%s", cluster, e.getMessage()));
        }

    }

    private URI getRedirectionUrl(ClusterData differentClusterData) throws MalformedURLException {
        URL webUrl = null;
        if (isRequestHttps() && pulsar.getConfiguration().getWebServicePortTls().isPresent()
                && StringUtils.isNotBlank(differentClusterData.getServiceUrlTls())) {
            webUrl = new URL(differentClusterData.getServiceUrlTls());
        } else {
            webUrl = new URL(differentClusterData.getServiceUrl());
        }
        return UriBuilder.fromUri(uri.getRequestUri()).host(webUrl.getHost()).port(webUrl.getPort()).build();
    }

    protected static CompletableFuture<ClusterData> getClusterDataIfDifferentCluster(PulsarService pulsar,
         String cluster, String clientAppId) {

        CompletableFuture<ClusterData> clusterDataFuture = new CompletableFuture<>();

        if (!isValidCluster(pulsar, cluster)) {
            try {
                // this code should only happen with a v1 namespace format prop/cluster/namespaces
                if (!pulsar.getConfiguration().getClusterName().equals(cluster)) {
                    // redirect to the cluster requested
                    pulsar.getConfigurationCache().clustersCache().getAsync(path("clusters", cluster))
                            .thenAccept(clusterDataResult -> {
                                if (clusterDataResult.isPresent()) {
                                    clusterDataFuture.complete(clusterDataResult.get());
                                } else {
                                    log.warn("[{}] Cluster does not exist: requested={}", clientAppId, cluster);
                                    clusterDataFuture.completeExceptionally(new RestException(Status.NOT_FOUND,
                                            "Cluster does not exist: cluster=" + cluster));
                                }
                            }).exceptionally(ex -> {
                                clusterDataFuture.completeExceptionally(ex);
                                return null;
                            });
                } else {
                    clusterDataFuture.complete(null);
                }
            } catch (Exception e) {
                clusterDataFuture.completeExceptionally(e);
            }
        } else {
            clusterDataFuture.complete(null);
        }
        return clusterDataFuture;
    }

    static boolean isValidCluster(PulsarService pulsarService, String cluster) {// If the cluster name is
        // cluster == null or "global", don't validate the
        // cluster ownership. Cluster will be null in v2 naming.
        // The validation will be done by checking the namespace configuration
        if (cluster == null || Constants.GLOBAL_CLUSTER.equals(cluster)) {
            return true;
        }

        if (!pulsarService.getConfiguration().isAuthorizationEnabled()) {
            // Without authorization, any cluster name should be valid and accepted by the broker
            return true;
        }
        return false;
    }

    /**
     * Checks whether the broker is the owner of all the namespace bundles. Otherwise, if authoritative is false, it
     * will throw an exception to redirect to assigned owner or leader; if authoritative is true then it will try to
     * acquire all the namespace bundles.
     *
<<<<<<< HEAD
     * @param fqnn
     * @param authoritative
     * @param readOnly
     * @param bundleData
=======
     * @param tenant tenant name
     * @param cluster cluster name
     * @param namespace namespace name
     * @param authoritative if it is an authoritative request
     * @param readOnly if the request is read-only
     * @param bundleData bundle data
>>>>>>> f773c602c... Test pr 10 (#27)
     */
    protected void validateNamespaceOwnershipWithBundles(String tenant, String cluster, String namespace,
            boolean authoritative, boolean readOnly, BundlesData bundleData) {
        NamespaceName fqnn = NamespaceName.get(tenant, cluster, namespace);

        try {
            NamespaceBundles bundles = pulsar().getNamespaceService().getNamespaceBundleFactory().getBundles(fqnn,
                    bundleData);
            for (NamespaceBundle bundle : bundles.getBundles()) {
                validateBundleOwnership(bundle, authoritative, readOnly);
            }
        } catch (WebApplicationException wae) {
            // propagate already wrapped-up WebApplicationExceptions
            throw wae;
        } catch (Exception oe) {
<<<<<<< HEAD
            log.debug(String.format("Failed to find owner for namespace %s", fqnn), oe);
=======
            log.debug("Failed to find owner for namespace {}", fqnn, oe);
>>>>>>> f773c602c... Test pr 10 (#27)
            throw new RestException(oe);
        }
    }

    protected void validateBundleOwnership(String tenant, String cluster, String namespace, boolean authoritative,
            boolean readOnly, NamespaceBundle bundle) {
        NamespaceName fqnn = NamespaceName.get(tenant, cluster, namespace);

        try {
            validateBundleOwnership(bundle, authoritative, readOnly);
        } catch (WebApplicationException wae) {
            // propagate already wrapped-up WebApplicationExceptions
            throw wae;
        } catch (Exception oe) {
<<<<<<< HEAD
            log.debug(String.format("Failed to find owner for namespace %s", fqnn), oe);
=======
            log.debug("Failed to find owner for namespace {}", fqnn, oe);
>>>>>>> f773c602c... Test pr 10 (#27)
            throw new RestException(oe);
        }
    }

    protected NamespaceBundle validateNamespaceBundleRange(NamespaceName fqnn, BundlesData bundles,
            String bundleRange) {
        try {
            checkArgument(bundleRange.contains("_"), "Invalid bundle range");
            String[] boundaries = bundleRange.split("_");
            Long lowerEndpoint = Long.decode(boundaries[0]);
            Long upperEndpoint = Long.decode(boundaries[1]);
            Range<Long> hashRange = Range.range(lowerEndpoint, BoundType.CLOSED, upperEndpoint,
                    (upperEndpoint.equals(NamespaceBundles.FULL_UPPER_BOUND)) ? BoundType.CLOSED : BoundType.OPEN);
            NamespaceBundle nsBundle = pulsar().getNamespaceService().getNamespaceBundleFactory().getBundle(fqnn,
                    hashRange);
            NamespaceBundles nsBundles = pulsar().getNamespaceService().getNamespaceBundleFactory().getBundles(fqnn,
                    bundles);
            nsBundles.validateBundle(nsBundle);
            return nsBundle;
        } catch (Exception e) {
            log.error("[{}] Failed to validate namespace bundle {}/{}", clientAppId(), fqnn.toString(), bundleRange, e);
            throw new RestException(e);
        }
    }

    /**
     * Checks whether a given bundle is currently loaded by any broker
     */
    protected boolean isBundleOwnedByAnyBroker(NamespaceName fqnn, BundlesData bundles,
            String bundleRange) {
        NamespaceBundle nsBundle = validateNamespaceBundleRange(fqnn, bundles, bundleRange);
        NamespaceService nsService = pulsar().getNamespaceService();
<<<<<<< HEAD
        try {
            return nsService.getWebServiceUrl(nsBundle, /*authoritative */ false, isRequestHttps(), /* read-only */ true).isPresent();
=======

        LookupOptions options = LookupOptions.builder()
                .authoritative(false)
                .requestHttps(isRequestHttps())
                .readOnly(true)
                .loadTopicsInBundle(false).build();
        try {
            return nsService.getWebServiceUrl(nsBundle, options).isPresent();
>>>>>>> f773c602c... Test pr 10 (#27)
        } catch (Exception e) {
            log.error("[{}] Failed to check whether namespace bundle is owned {}/{}", clientAppId(), fqnn.toString(), bundleRange, e);
            throw new RestException(e);
        }
    }

    protected NamespaceBundle validateNamespaceBundleOwnership(NamespaceName fqnn, BundlesData bundles,
            String bundleRange, boolean authoritative, boolean readOnly) {
        try {
            NamespaceBundle nsBundle = validateNamespaceBundleRange(fqnn, bundles, bundleRange);
            validateBundleOwnership(nsBundle, authoritative, readOnly);
            return nsBundle;
        } catch (WebApplicationException wae) {
            throw wae;
        } catch (Exception e) {
            log.error("[{}] Failed to validate namespace bundle {}/{}", clientAppId(), fqnn.toString(), bundleRange, e);
            throw new RestException(e);
        }
    }

    public void validateBundleOwnership(NamespaceBundle bundle, boolean authoritative, boolean readOnly)
            throws Exception {
        NamespaceService nsService = pulsar().getNamespaceService();

        try {
            // Call getWebServiceUrl() to acquire or redirect the request
            // Get web service URL of owning broker.
            // 1: If namespace is assigned to this broker, continue
            // 2: If namespace is assigned to another broker, redirect to the webservice URL of another broker
            // authoritative flag is ignored
            // 3: If namespace is unassigned and readOnly is true, return 412
            // 4: If namespace is unassigned and readOnly is false:
            // - If authoritative is false and this broker is not leader, forward to leader
            // - If authoritative is false and this broker is leader, determine owner and forward w/ authoritative=true
            // - If authoritative is true, own the namespace and continue
<<<<<<< HEAD
            Optional<URL> webUrl = nsService.getWebServiceUrl(bundle, authoritative, isRequestHttps(), readOnly);
=======
            LookupOptions options = LookupOptions.builder()
                    .authoritative(authoritative)
                    .requestHttps(isRequestHttps())
                    .readOnly(readOnly)
                    .loadTopicsInBundle(false).build();
            Optional<URL> webUrl = nsService.getWebServiceUrl(bundle, options);
>>>>>>> f773c602c... Test pr 10 (#27)
            // Ensure we get a url
            if (webUrl == null || !webUrl.isPresent()) {
                log.warn("Unable to get web service url");
                throw new RestException(Status.PRECONDITION_FAILED,
                        "Failed to find ownership for ServiceUnit:" + bundle.toString());
            }

            if (!nsService.isServiceUnitOwned(bundle)) {
                boolean newAuthoritative = this.isLeaderBroker();
                // Replace the host and port of the current request and redirect
                URI redirect = UriBuilder.fromUri(uri.getRequestUri()).host(webUrl.get().getHost())
                        .port(webUrl.get().getPort()).replaceQueryParam("authoritative", newAuthoritative).build();

                log.debug("{} is not a service unit owned", bundle);

                // Redirect
                log.debug("Redirecting the rest call to {}", redirect);
                throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
            }
<<<<<<< HEAD
        } catch (IllegalArgumentException iae) {
            // namespace format is not valid
            log.debug(String.format("Failed to find owner for ServiceUnit %s", bundle), iae);
            throw new RestException(Status.PRECONDITION_FAILED,
                    "ServiceUnit format is not expected. ServiceUnit " + bundle);
        } catch (IllegalStateException ise) {
            log.debug(String.format("Failed to find owner for ServiceUnit %s", bundle), ise);
=======
        } catch (TimeoutException te) {
            String msg = String.format("Finding owner for ServiceUnit %s timed out", bundle);
            log.error(msg, te);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, msg);
        } catch (IllegalArgumentException iae) {
            // namespace format is not valid
            log.debug("Failed to find owner for ServiceUnit {}", bundle, iae);
            throw new RestException(Status.PRECONDITION_FAILED,
                    "ServiceUnit format is not expected. ServiceUnit " + bundle);
        } catch (IllegalStateException ise) {
            log.debug("Failed to find owner for ServiceUnit {}", bundle, ise);
>>>>>>> f773c602c... Test pr 10 (#27)
            throw new RestException(Status.PRECONDITION_FAILED, "ServiceUnit bundle is actived. ServiceUnit " + bundle);
        } catch (NullPointerException e) {
            log.warn("Unable to get web service url");
            throw new RestException(Status.PRECONDITION_FAILED, "Failed to find ownership for ServiceUnit:" + bundle);
        } catch (WebApplicationException wae) {
            throw wae;
        }
    }

    /**
     * Checks whether the broker is the owner of the namespace. Otherwise it will raise an exception to redirect the
     * client to the appropriate broker. If no broker owns the namespace yet, this function will try to acquire the
     * ownership by default.
     *
<<<<<<< HEAD
     * @param authoritative
     *
     * @param tenant
     * @param cluster
     * @param namespace
=======
     * @param topicName topic name
     * @param authoritative
>>>>>>> f773c602c... Test pr 10 (#27)
     */
    protected void validateTopicOwnership(TopicName topicName, boolean authoritative) {
        NamespaceService nsService = pulsar().getNamespaceService();

        try {
            // per function name, this is trying to acquire the whole namespace ownership
<<<<<<< HEAD
            Optional<URL> webUrl = nsService.getWebServiceUrl(topicName, authoritative, isRequestHttps(), false);
=======
            LookupOptions options = LookupOptions.builder()
                    .authoritative(authoritative)
                    .requestHttps(isRequestHttps())
                    .readOnly(false)
                    .loadTopicsInBundle(false).build();
            Optional<URL> webUrl = nsService.getWebServiceUrl(topicName, options);
>>>>>>> f773c602c... Test pr 10 (#27)
            // Ensure we get a url
            if (webUrl == null || !webUrl.isPresent()) {
                log.info("Unable to get web service url");
                throw new RestException(Status.PRECONDITION_FAILED, "Failed to find ownership for topic:" + topicName);
            }

            if (!nsService.isServiceUnitOwned(topicName)) {
                boolean newAuthoritative = isLeaderBroker(pulsar());
                // Replace the host and port of the current request and redirect
                URI redirect = UriBuilder.fromUri(uri.getRequestUri()).host(webUrl.get().getHost())
                        .port(webUrl.get().getPort()).replaceQueryParam("authoritative", newAuthoritative).build();
                // Redirect
                log.debug("Redirecting the rest call to {}", redirect);
                throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
            }
<<<<<<< HEAD
        } catch (IllegalArgumentException iae) {
            // namespace format is not valid
            log.debug(String.format("Failed to find owner for topic :%s", topicName), iae);
            throw new RestException(Status.PRECONDITION_FAILED, "Can't find owner for topic " + topicName);
        } catch (IllegalStateException ise) {
            log.debug(String.format("Failed to find owner for topic:%s", topicName), ise);
=======
        } catch (TimeoutException te) {
            String msg = String.format("Finding owner for topic %s timed out", topicName);
            log.error(msg, te);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, msg);
        } catch (IllegalArgumentException iae) {
            // namespace format is not valid
            log.debug("Failed to find owner for topic: {}", topicName, iae);
            throw new RestException(Status.PRECONDITION_FAILED, "Can't find owner for topic " + topicName);
        } catch (IllegalStateException ise) {
            log.debug("Failed to find owner for topic: {}", topicName, ise);
>>>>>>> f773c602c... Test pr 10 (#27)
            throw new RestException(Status.PRECONDITION_FAILED, "Can't find owner for topic " + topicName);
        } catch (WebApplicationException wae) {
            throw wae;
        } catch (Exception oe) {
<<<<<<< HEAD
            log.debug(String.format("Failed to find owner for topic:%s", topicName), oe);
=======
            log.debug("Failed to find owner for topic: {}", topicName, oe);
>>>>>>> f773c602c... Test pr 10 (#27)
            throw new RestException(oe);
        }
    }

    /**
     * If the namespace is global, validate the following - 1. If replicated clusters are configured for this global
     * namespace 2. If local cluster belonging to this namespace is replicated 3. If replication is enabled for this
     * namespace <br/>
     * It validates if local cluster is part of replication-cluster. If local cluster is not part of the replication
     * cluster then it redirects request to peer-cluster if any of the peer-cluster is part of replication-cluster of
     * this namespace. If none of the cluster is part of the replication cluster then it fails the validation.
     *
     * @param namespace
     * @throws Exception
     */
    protected void validateGlobalNamespaceOwnership(NamespaceName namespace) {
<<<<<<< HEAD
        try {
            ClusterData peerClusterData = checkLocalOrGetPeerReplicationCluster(pulsar(), namespace)
                    .get(cacheTimeOutInSec, SECONDS);
=======
        int timeout = pulsar().getConfiguration().getZooKeeperOperationTimeoutSeconds();
        try {
            ClusterData peerClusterData = checkLocalOrGetPeerReplicationCluster(pulsar(), namespace)
                    .get(timeout, SECONDS);
>>>>>>> f773c602c... Test pr 10 (#27)
            // if peer-cluster-data is present it means namespace is owned by that peer-cluster and request should be
            // redirect to the peer-cluster
            if (peerClusterData != null) {
                URI redirect = getRedirectionUrl(peerClusterData);
                // redirect to the cluster requested
                if (log.isDebugEnabled()) {
<<<<<<< HEAD
                    log.debug("[{}] Redirecting the rest call to {}: cluster={}", redirect, peerClusterData);
=======
                    log.debug("[{}] Redirecting the rest call to {}: cluster={}", clientAppId(),redirect, peerClusterData);
>>>>>>> f773c602c... Test pr 10 (#27)

                }
                throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
            }
        } catch (InterruptedException e) {
<<<<<<< HEAD
            log.warn("Time-out {} sec while validating policy on {} ", cacheTimeOutInSec, namespace);
=======
            log.warn("Time-out {} sec while validating policy on {} ", timeout, namespace);
>>>>>>> f773c602c... Test pr 10 (#27)
            throw new RestException(Status.SERVICE_UNAVAILABLE, String.format(
                    "Failed to validate global cluster configuration : ns=%s  emsg=%s", namespace, e.getMessage()));
        } catch (WebApplicationException e) {
            throw e;
        } catch (Exception e) {
            if (e.getCause() instanceof WebApplicationException) {
                throw (WebApplicationException) e.getCause();
            }
            throw new RestException(Status.SERVICE_UNAVAILABLE, String.format(
                    "Failed to validate global cluster configuration : ns=%s  emsg=%s", namespace, e.getMessage()));
        }
    }

    public static CompletableFuture<ClusterData> checkLocalOrGetPeerReplicationCluster(PulsarService pulsarService,
            NamespaceName namespace) {
        if (!namespace.isGlobal()) {
            return CompletableFuture.completedFuture(null);
        }
        final CompletableFuture<ClusterData> validationFuture = new CompletableFuture<>();
        final String localCluster = pulsarService.getConfiguration().getClusterName();
        final String path = AdminResource.path(POLICIES, namespace.toString());

        pulsarService.getConfigurationCache().policiesCache().getAsync(path).thenAccept(policiesResult -> {
            if (policiesResult.isPresent()) {
                Policies policies = policiesResult.get();
                if (policies.replication_clusters.isEmpty()) {
                    String msg = String.format(
                            "Namespace does not have any clusters configured : local_cluster=%s ns=%s",
                            localCluster, namespace.toString());
                    log.warn(msg);
                    validationFuture.completeExceptionally(new RestException(Status.PRECONDITION_FAILED, msg));
                } else if (!policies.replication_clusters.contains(localCluster)) {
                    ClusterData ownerPeerCluster = getOwnerFromPeerClusterList(pulsarService,
                            policies.replication_clusters);
                    if (ownerPeerCluster != null) {
                        // found a peer that own this namespace
                        validationFuture.complete(ownerPeerCluster);
                        return;
                    }
                    String msg = String.format(
                            "Namespace missing local cluster name in clusters list: local_cluster=%s ns=%s clusters=%s",
                            localCluster, namespace.toString(), policies.replication_clusters);

                    log.warn(msg);
                    validationFuture.completeExceptionally(new RestException(Status.PRECONDITION_FAILED, msg));
                } else {
                    validationFuture.complete(null);
                }
            } else {
                String msg = String.format("Policies not found for %s namespace", namespace.toString());
<<<<<<< HEAD
                log.error(msg);
=======
                log.warn(msg);
>>>>>>> f773c602c... Test pr 10 (#27)
                validationFuture.completeExceptionally(new RestException(Status.NOT_FOUND, msg));
            }
        }).exceptionally(ex -> {
            String msg = String.format("Failed to validate global cluster configuration : cluster=%s ns=%s  emsg=%s",
                    localCluster, namespace, ex.getMessage());
            log.error(msg);
            validationFuture.completeExceptionally(new RestException(ex));
            return null;
        });
        return validationFuture;
    }

    private static ClusterData getOwnerFromPeerClusterList(PulsarService pulsar, Set<String> replicationClusters) {
        String currentCluster = pulsar.getConfiguration().getClusterName();
        if (replicationClusters == null || replicationClusters.isEmpty() || isBlank(currentCluster)) {
            return null;
        }

        try {
            Optional<ClusterData> cluster = pulsar.getConfigurationCache().clustersCache()
                    .get(path("clusters", currentCluster));
            if (!cluster.isPresent() || cluster.get().getPeerClusterNames() == null) {
                return null;
            }
            for (String peerCluster : cluster.get().getPeerClusterNames()) {
                if (replicationClusters.contains(peerCluster)) {
                    return pulsar.getConfigurationCache().clustersCache().get(path("clusters", peerCluster))
                            .orElseThrow(() -> new RestException(Status.NOT_FOUND,
                                    "Peer cluster " + peerCluster + " data not found"));
                }
            }
        } catch (Exception e) {
            log.error("Failed to get peer-cluster {}-{}", currentCluster, e.getMessage());
            if (e instanceof RestException) {
                throw (RestException) e;
            } else {
                throw new RestException(e);
            }
        }
        return null;
    }

    protected void checkConnect(TopicName topicName) throws RestException, Exception {
        checkAuthorization(pulsar(), topicName, clientAppId(), clientAuthData());
    }

    protected static void checkAuthorization(PulsarService pulsarService, TopicName topicName, String role,
            AuthenticationDataSource authenticationData) throws RestException, Exception {
        if (!pulsarService.getConfiguration().isAuthorizationEnabled()) {
            // No enforcing of authorization policies
            return;
        }
        // get zk policy manager
        if (!pulsarService.getBrokerService().getAuthorizationService().canLookup(topicName, role, authenticationData)) {
            log.warn("[{}] Role {} is not allowed to lookup topic", topicName, role);
            throw new RestException(Status.UNAUTHORIZED, "Don't have permission to connect to this namespace");
        }
    }

    // Used for unit tests access
    public void setPulsar(PulsarService pulsar) {
        this.pulsar = pulsar;
    }

    protected boolean isLeaderBroker() {
        return isLeaderBroker(pulsar());
    }

    protected static boolean isLeaderBroker(PulsarService pulsar) {

        String leaderAddress = pulsar.getLeaderElectionService().getCurrentLeader().getServiceUrl();

<<<<<<< HEAD
        String myAddress = pulsar.getWebServiceAddress();
=======
        String myAddress = pulsar.getSafeWebServiceAddress();
>>>>>>> f773c602c... Test pr 10 (#27)

        return myAddress.equals(leaderAddress); // If i am the leader, my decisions are
    }

    // Non-Usual HTTP error codes
    protected static final int NOT_IMPLEMENTED = 501;

<<<<<<< HEAD
=======
    public void validateTenantOperation(String tenant, TenantOperation operation) {
        if (pulsar().getConfiguration().isAuthenticationEnabled()
            && pulsar().getBrokerService().isAuthorizationEnabled()) {
            if (!isClientAuthenticated(clientAppId())) {
                throw new RestException(Status.UNAUTHORIZED, "Need to authenticate to perform the request");
            }

            boolean isAuthorized = pulsar().getBrokerService().getAuthorizationService()
                .allowTenantOperation(tenant, operation, originalPrincipal(), clientAppId(), clientAuthData());
            if (!isAuthorized) {
                throw new RestException(Status.UNAUTHORIZED,
                    String.format("Unauthorized to validateTenantOperation for"
                            + " originalPrincipal [%s] and clientAppId [%s] about operation [%s] on tenant [%s]",
                        originalPrincipal(), clientAppId(), operation.toString(), tenant));
            }
        }
    }

    public void validateNamespaceOperation(NamespaceName namespaceName, NamespaceOperation operation) {
        if (pulsar().getConfiguration().isAuthenticationEnabled()
            && pulsar().getBrokerService().isAuthorizationEnabled()) {
            if (!isClientAuthenticated(clientAppId())) {
                throw new RestException(Status.FORBIDDEN, "Need to authenticate to perform the request");
            }

            boolean isAuthorized = pulsar().getBrokerService().getAuthorizationService()
                    .allowNamespaceOperation(namespaceName, operation, originalPrincipal(),
                        clientAppId(), clientAuthData());

            if (!isAuthorized) {
                throw new RestException(Status.FORBIDDEN, String.format("Unauthorized to validateNamespaceOperation for" +
                        " operation [%s] on namespace [%s]", operation.toString(), namespaceName));
            }
        }
    }

    public void validateNamespacePolicyOperation(NamespaceName namespaceName,
                                                 PolicyName policy,
                                                 PolicyOperation operation) {
        if (pulsar().getConfiguration().isAuthenticationEnabled()
            && pulsar().getBrokerService().isAuthorizationEnabled()) {
            if (!isClientAuthenticated(clientAppId())) {
                throw new RestException(Status.FORBIDDEN, "Need to authenticate to perform the request");
            }

            boolean isAuthorized = pulsar().getBrokerService().getAuthorizationService()
                    .allowNamespacePolicyOperation(namespaceName, policy, operation,
                        originalPrincipal(), clientAppId(), clientAuthData());

            if (!isAuthorized) {
                throw new RestException(Status.FORBIDDEN, String.format("Unauthorized to validateNamespacePolicyOperation for" +
                        " operation [%s] on namespace [%s] on policy [%s]", operation.toString(), namespaceName, policy.toString()));
            }
        }
    }
>>>>>>> f773c602c... Test pr 10 (#27)
}
