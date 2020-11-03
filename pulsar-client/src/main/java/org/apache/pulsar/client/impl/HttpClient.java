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
package org.apache.pulsar.client.impl;

<<<<<<< HEAD
import com.google.common.util.concurrent.MoreExecutors;

import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.ssl.SslContext;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.NotFoundException;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.util.SecurityUtility;
import org.asynchttpclient.AsyncCompletionHandler;
=======
import java.io.Closeable;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.ssl.SslContext;
import javax.net.ssl.SSLContext;
import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.KeyStoreParams;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.NotFoundException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.util.SecurityUtility;
import org.apache.pulsar.common.util.keystoretls.KeyStoreSSLContext;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
<<<<<<< HEAD
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;
import org.asynchttpclient.channel.DefaultKeepAliveStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

=======
import org.asynchttpclient.Request;
import org.asynchttpclient.channel.DefaultKeepAliveStrategy;
import org.asynchttpclient.netty.ssl.JsseSslEngineFactory;


@Slf4j
>>>>>>> f773c602c... Test pr 10 (#27)
public class HttpClient implements Closeable {

    protected final static int DEFAULT_CONNECT_TIMEOUT_IN_SECONDS = 10;
    protected final static int DEFAULT_READ_TIMEOUT_IN_SECONDS = 30;

    protected final AsyncHttpClient httpClient;
    protected final ServiceNameResolver serviceNameResolver;
    protected final Authentication authentication;

<<<<<<< HEAD
    protected HttpClient(String serviceUrl, Authentication authentication,
            EventLoopGroup eventLoopGroup, boolean tlsAllowInsecureConnection, String tlsTrustCertsFilePath)
            throws PulsarClientException {
        this(serviceUrl, authentication, eventLoopGroup, tlsAllowInsecureConnection,
                tlsTrustCertsFilePath, DEFAULT_CONNECT_TIMEOUT_IN_SECONDS, DEFAULT_READ_TIMEOUT_IN_SECONDS);
    }

    protected HttpClient(String serviceUrl, Authentication authentication,
            EventLoopGroup eventLoopGroup, boolean tlsAllowInsecureConnection, String tlsTrustCertsFilePath,
            int connectTimeoutInSeconds, int readTimeoutInSeconds) throws PulsarClientException {
        this.authentication = authentication;
        this.serviceNameResolver = new PulsarServiceNameResolver();
        this.serviceNameResolver.updateServiceUrl(serviceUrl);

        DefaultAsyncHttpClientConfig.Builder confBuilder = new DefaultAsyncHttpClientConfig.Builder();
        confBuilder.setFollowRedirect(true);
        confBuilder.setConnectTimeout(connectTimeoutInSeconds * 1000);
        confBuilder.setReadTimeout(readTimeoutInSeconds * 1000);
        confBuilder.setUserAgent(String.format("Pulsar-Java-v%s", getPulsarClientVersion()));
        confBuilder.setKeepAliveStrategy(new DefaultKeepAliveStrategy() {
            @Override
            public boolean keepAlive(Request ahcRequest, HttpRequest request, HttpResponse response) {
                // Close connection upon a server error or per HTTP spec
                return (response.status().code() / 100 != 5) && super.keepAlive(ahcRequest, request, response);
=======
    protected HttpClient(ClientConfigurationData conf, EventLoopGroup eventLoopGroup) throws PulsarClientException {
        this.authentication = conf.getAuthentication();
        this.serviceNameResolver = new PulsarServiceNameResolver();
        this.serviceNameResolver.updateServiceUrl(conf.getServiceUrl());

        DefaultAsyncHttpClientConfig.Builder confBuilder = new DefaultAsyncHttpClientConfig.Builder();
        confBuilder.setFollowRedirect(true);
        confBuilder.setMaxRedirects(conf.getMaxLookupRedirects());
        confBuilder.setConnectTimeout(DEFAULT_CONNECT_TIMEOUT_IN_SECONDS * 1000);
        confBuilder.setReadTimeout(DEFAULT_READ_TIMEOUT_IN_SECONDS * 1000);
        confBuilder.setUserAgent(String.format("Pulsar-Java-v%s", PulsarVersion.getVersion()));
        confBuilder.setKeepAliveStrategy(new DefaultKeepAliveStrategy() {
            @Override
            public boolean keepAlive(InetSocketAddress remoteAddress, Request ahcRequest,
                                     HttpRequest request, HttpResponse response) {
                // Close connection upon a server error or per HTTP spec
                return (response.status().code() / 100 != 5)
                       && super.keepAlive(remoteAddress, ahcRequest, request, response);
>>>>>>> f773c602c... Test pr 10 (#27)
            }
        });

        if ("https".equals(serviceNameResolver.getServiceUri().getServiceName())) {
            try {
<<<<<<< HEAD
                SslContext sslCtx = null;

                // Set client key and certificate if available
                AuthenticationDataProvider authData = authentication.getAuthData();
                if (authData.hasDataForTls()) {
                    sslCtx = SecurityUtility.createNettySslContextForClient(tlsAllowInsecureConnection, tlsTrustCertsFilePath,
                            authData.getTlsCertificates(), authData.getTlsPrivateKey());
                } else {
                    sslCtx = SecurityUtility.createNettySslContextForClient(tlsAllowInsecureConnection, tlsTrustCertsFilePath);
                }

                confBuilder.setSslContext(sslCtx);
                confBuilder.setUseInsecureTrustManager(tlsAllowInsecureConnection);
=======
                // Set client key and certificate if available
                AuthenticationDataProvider authData = authentication.getAuthData();

                if (conf.isUseKeyStoreTls()) {
                    SSLContext sslCtx = null;
                    KeyStoreParams params = authData.hasDataForTls() ? authData.getTlsKeyStoreParams() : null;

                    sslCtx = KeyStoreSSLContext.createClientSslContext(
                            conf.getSslProvider(),
                            params != null ? params.getKeyStoreType() : null,
                            params != null ? params.getKeyStorePath() : null,
                            params != null ? params.getKeyStorePassword() : null,
                            conf.isTlsAllowInsecureConnection(),
                            conf.getTlsTrustStoreType(),
                            conf.getTlsTrustStorePath(),
                            conf.getTlsTrustStorePassword(),
                            conf.getTlsCiphers(),
                            conf.getTlsProtocols());

                    JsseSslEngineFactory sslEngineFactory = new JsseSslEngineFactory(sslCtx);
                    confBuilder.setSslEngineFactory(sslEngineFactory);
                } else {
                    SslContext sslCtx = null;
                    if (authData.hasDataForTls()) {
                        sslCtx = authData.getTlsTrustStoreStream() == null
                                ? SecurityUtility.createNettySslContextForClient(conf.isTlsAllowInsecureConnection(),
                                        conf.getTlsTrustCertsFilePath(), authData.getTlsCertificates(),
                                        authData.getTlsPrivateKey())
                                : SecurityUtility.createNettySslContextForClient(conf.isTlsAllowInsecureConnection(),
                                        authData.getTlsTrustStoreStream(), authData.getTlsCertificates(),
                                        authData.getTlsPrivateKey());
                    }
                    else {
                        sslCtx = SecurityUtility.createNettySslContextForClient(
                                conf.isTlsAllowInsecureConnection(),
                                conf.getTlsTrustCertsFilePath());
                    }
                    confBuilder.setSslContext(sslCtx);
                }

                confBuilder.setUseInsecureTrustManager(conf.isTlsAllowInsecureConnection());
>>>>>>> f773c602c... Test pr 10 (#27)
            } catch (Exception e) {
                throw new PulsarClientException.InvalidConfigurationException(e);
            }
        }
        confBuilder.setEventLoopGroup(eventLoopGroup);
        AsyncHttpClientConfig config = confBuilder.build();
        httpClient = new DefaultAsyncHttpClient(config);

<<<<<<< HEAD
        log.debug("Using HTTP url: {}", serviceUrl);
=======
        log.debug("Using HTTP url: {}", conf.getServiceUrl());
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    String getServiceUrl() {
        return this.serviceNameResolver.getServiceUrl();
    }

    void setServiceUrl(String serviceUrl) throws PulsarClientException {
        this.serviceNameResolver.updateServiceUrl(serviceUrl);
    }

    @Override
    public void close() throws IOException {
        httpClient.close();
    }

    public <T> CompletableFuture<T> get(String path, Class<T> clazz) {
        final CompletableFuture<T> future = new CompletableFuture<>();
        try {
<<<<<<< HEAD
            String requestUrl = new URL(serviceNameResolver.resolveHostUri().toURL(), path).toString();
            AuthenticationDataProvider authData = authentication.getAuthData();
            BoundRequestBuilder builder = httpClient.prepareGet(requestUrl);

            // Add headers for authentication if any
            if (authData.hasDataForHttp()) {
                for (Map.Entry<String, String> header : authData.getHttpHeaders()) {
                    builder.setHeader(header.getKey(), header.getValue());
                }
            }

            final ListenableFuture<Response> responseFuture = builder.setHeader("Accept", "application/json")
                    .execute(new AsyncCompletionHandler<Response>() {

                        @Override
                        public Response onCompleted(Response response) throws Exception {
                            return response;
                        }

                        @Override
                        public void onThrowable(Throwable t) {
                            log.warn("[{}] Failed to perform http request: {}", requestUrl, t.getMessage());
                            future.completeExceptionally(new PulsarClientException(t));
                        }
                    });

            responseFuture.addListener(() -> {
                try {
                    Response response = responseFuture.get();
                    if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                        log.warn("[{}] HTTP get request failed: {}", requestUrl, response.getStatusText());
                        Exception e;
                        if (response.getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                            e = new NotFoundException("Not found: " + response.getStatusText());
                        } else {
                            e = new PulsarClientException("HTTP get request failed: " + response.getStatusText());
=======
            URI hostUri = serviceNameResolver.resolveHostUri();
            String requestUrl = new URL(hostUri.toURL(), path).toString();
            String remoteHostName = hostUri.getHost();
            AuthenticationDataProvider authData = authentication.getAuthData(remoteHostName);

            CompletableFuture<Map<String, String>>  authFuture = new CompletableFuture<>();

            // bring a authenticationStage for sasl auth.
            if (authData.hasDataForHttp()) {
                authentication.authenticationStage(requestUrl, authData, null, authFuture);
            } else {
                authFuture.complete(null);
            }

            // auth complete, do real request
            authFuture.whenComplete((respHeaders, ex) -> {
                if (ex != null) {
                    log.warn("[{}] Failed to perform http request at authentication stage: {}",
                        requestUrl, ex.getMessage());
                    future.completeExceptionally(new PulsarClientException(ex));
                    return;
                }

                // auth complete, use a new builder
                BoundRequestBuilder builder = httpClient.prepareGet(requestUrl)
                    .setHeader("Accept", "application/json");

                if (authData.hasDataForHttp()) {
                    Set<Entry<String, String>> headers;
                    try {
                        headers = authentication.newRequestHeader(requestUrl, authData, respHeaders);
                    } catch (Exception e) {
                        log.warn("[{}] Error during HTTP get headers: {}", requestUrl, e.getMessage());
                        future.completeExceptionally(new PulsarClientException(e));
                        return;
                    }
                    if (headers != null) {
                        headers.forEach(entry -> builder.addHeader(entry.getKey(), entry.getValue()));
                    }
                }

                builder.execute().toCompletableFuture().whenComplete((response2, t) -> {
                    if (t != null) {
                        log.warn("[{}] Failed to perform http request: {}", requestUrl, t.getMessage());
                        future.completeExceptionally(new PulsarClientException(t));
                        return;
                    }

                    // request not success
                    if (response2.getStatusCode() != HttpURLConnection.HTTP_OK) {
                        log.warn("[{}] HTTP get request failed: {}", requestUrl, response2.getStatusText());
                        Exception e;
                        if (response2.getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                            e = new NotFoundException("Not found: " + response2.getStatusText());
                        } else {
                            e = new PulsarClientException("HTTP get request failed: " + response2.getStatusText());
>>>>>>> f773c602c... Test pr 10 (#27)
                        }
                        future.completeExceptionally(e);
                        return;
                    }

<<<<<<< HEAD
                    T data = ObjectMapperFactory.getThreadLocal().readValue(response.getResponseBodyAsBytes(), clazz);
                    future.complete(data);
                } catch (Exception e) {
                    log.warn("[{}] Error during HTTP get request: {}", requestUrl, e.getMessage());
                    future.completeExceptionally(new PulsarClientException(e));
                }
            }, MoreExecutors.directExecutor());

        } catch (Exception e) {
            log.warn("[{}] Failed to get authentication data for lookup: {}", path, e.getMessage());
=======
                    try {
                        T data = ObjectMapperFactory.getThreadLocal().readValue(response2.getResponseBodyAsBytes(), clazz);
                        future.complete(data);
                    } catch (Exception e) {
                        log.warn("[{}] Error during HTTP get request: {}", requestUrl, e.getMessage());
                        future.completeExceptionally(new PulsarClientException(e));
                    }
                });
            });
        } catch (Exception e) {
            log.warn("[{}]PulsarClientImpl: {}", path, e.getMessage());
>>>>>>> f773c602c... Test pr 10 (#27)
            if (e instanceof PulsarClientException) {
                future.completeExceptionally(e);
            } else {
                future.completeExceptionally(new PulsarClientException(e));
            }
        }

        return future;
<<<<<<< HEAD

    }

    /**
     * Looks for a file called pulsar-client-version.properties and returns the client version
     *
     * @return client version or unknown version depending on whether the file is found or not.
     */
    public static String getPulsarClientVersion() {
        String path = "/pulsar-client-version.properties";
        String unknownClientIdentifier = "UnknownClient";

        try {
            InputStream stream = HttpClient.class.getResourceAsStream(path);
            if (stream == null) {
                return unknownClientIdentifier;
            }
            Properties props = new Properties();
            try {
                props.load(stream);
                String version = (String) props.get("pulsar-client-version");
                return version;
            } catch (IOException e) {
                return unknownClientIdentifier;
            } finally {
                stream.close();
            }
        } catch (Throwable t) {
            return unknownClientIdentifier;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(HttpClient.class);
=======
    }
>>>>>>> f773c602c... Test pr 10 (#27)
}
