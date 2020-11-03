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
package org.apache.pulsar.proxy.server;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

import io.netty.bootstrap.ServerBootstrap;
<<<<<<< HEAD
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
=======
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
>>>>>>> f773c602c... Test pr 10 (#27)
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
<<<<<<< HEAD

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
=======
import lombok.Getter;
import lombok.Setter;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.apache.pulsar.proxy.server.plugin.servlet.ProxyAdditionalServlets;
import org.apache.pulsar.proxy.stats.TopicStats;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.pulsar.zookeeper.ZookeeperClientFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

<<<<<<< HEAD
=======
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

>>>>>>> f773c602c... Test pr 10 (#27)
/**
 * Pulsar proxy service
 */
public class ProxyService implements Closeable {

    private final ProxyConfiguration proxyConfig;
<<<<<<< HEAD
    private final String serviceUrl;
    private final String serviceUrlTls;
=======
    private String serviceUrl;
    private String serviceUrlTls;
>>>>>>> f773c602c... Test pr 10 (#27)
    private ConfigurationCacheService configurationCacheService;
    private final AuthenticationService authenticationService;
    private AuthorizationService authorizationService;
    private ZooKeeperClientFactory zkClientFactory = null;

    private final EventLoopGroup acceptorGroup;
    private final EventLoopGroup workerGroup;

<<<<<<< HEAD
    private final DefaultThreadFactory acceptorThreadFactory = new DefaultThreadFactory("pulsar-discovery-acceptor");
    private final DefaultThreadFactory workersThreadFactory = new DefaultThreadFactory("pulsar-discovery-io");
=======
    private Channel listenChannel;
    private Channel listenChannelTls;

    private final DefaultThreadFactory acceptorThreadFactory = new DefaultThreadFactory("pulsar-proxy-acceptor");
    private final DefaultThreadFactory workersThreadFactory = new DefaultThreadFactory("pulsar-proxy-io");
>>>>>>> f773c602c... Test pr 10 (#27)

    private BrokerDiscoveryProvider discoveryProvider;

    protected final AtomicReference<Semaphore> lookupRequestSemaphore;

<<<<<<< HEAD
=======
    @Getter
    @Setter
    protected int proxyLogLevel;

    private final ScheduledExecutorService statsExecutor;

>>>>>>> f773c602c... Test pr 10 (#27)
    private static final int numThreads = Runtime.getRuntime().availableProcessors();

    static final Gauge activeConnections = Gauge
            .build("pulsar_proxy_active_connections", "Number of connections currently active in the proxy").create()
            .register();

    static final Counter newConnections = Counter
            .build("pulsar_proxy_new_connections", "Counter of connections being opened in the proxy").create()
            .register();

    static final Counter rejectedConnections = Counter
            .build("pulsar_proxy_rejected_connections", "Counter for connections rejected due to throttling").create()
            .register();

    static final Counter opsCounter = Counter
            .build("pulsar_proxy_binary_ops", "Counter of proxy operations").create().register();

    static final Counter bytesCounter = Counter
            .build("pulsar_proxy_binary_bytes", "Counter of proxy bytes").create().register();

<<<<<<< HEAD
=======
    @Getter
    private final Set<ProxyConnection> clientCnxs;
    @Getter
    private final Map<String, TopicStats> topicStats;
    @Getter
    private ProxyAdditionalServlets proxyAdditionalServlets;

>>>>>>> f773c602c... Test pr 10 (#27)
    public ProxyService(ProxyConfiguration proxyConfig,
                        AuthenticationService authenticationService) throws IOException {
        checkNotNull(proxyConfig);
        this.proxyConfig = proxyConfig;
<<<<<<< HEAD
=======
        this.clientCnxs = Sets.newConcurrentHashSet();
        this.topicStats = Maps.newConcurrentMap();
>>>>>>> f773c602c... Test pr 10 (#27)

        this.lookupRequestSemaphore = new AtomicReference<Semaphore>(
                new Semaphore(proxyConfig.getMaxConcurrentLookupRequests(), false));

<<<<<<< HEAD
        String hostname;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
        if (proxyConfig.getServicePort().isPresent()) {
            this.serviceUrl = String.format("pulsar://%s:%d/", hostname, proxyConfig.getServicePort().get());
        } else {
            this.serviceUrl = null;
        }
        
        if (proxyConfig.getServicePortTls().isPresent()) {
            this.serviceUrlTls = String.format("pulsar://%s:%d/", hostname, proxyConfig.getServicePortTls().get());
        } else {
            this.serviceUrlTls = null;
        }

        this.acceptorGroup = EventLoopUtil.newEventLoopGroup(1, acceptorThreadFactory);
        this.workerGroup = EventLoopUtil.newEventLoopGroup(numThreads, workersThreadFactory);
        this.authenticationService = authenticationService;
=======
        if (proxyConfig.getProxyLogLevel().isPresent()) {
            proxyLogLevel = Integer.valueOf(proxyConfig.getProxyLogLevel().get());
        } else {
            proxyLogLevel = 0;
        }
        this.acceptorGroup = EventLoopUtil.newEventLoopGroup(1, acceptorThreadFactory);
        this.workerGroup = EventLoopUtil.newEventLoopGroup(numThreads, workersThreadFactory);
        this.authenticationService = authenticationService;

        statsExecutor = Executors
                .newSingleThreadScheduledExecutor(new DefaultThreadFactory("proxy-stats-executor"));
        statsExecutor.schedule(()->{
            this.clientCnxs.forEach(cnx -> {
                if (cnx.getDirectProxyHandler() != null
                        && cnx.getDirectProxyHandler().getInboundChannelRequestsRate() != null) {
                    cnx.getDirectProxyHandler().getInboundChannelRequestsRate().calculateRate();
                }
            });
            this.topicStats.forEach((topic, stats) -> {
                stats.calculate();
            });
        }, 60, TimeUnit.SECONDS);
        this.proxyAdditionalServlets = ProxyAdditionalServlets.load(proxyConfig);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    public void start() throws Exception {
        if (!isBlank(proxyConfig.getZookeeperServers()) && !isBlank(proxyConfig.getConfigurationStoreServers())) {
            discoveryProvider = new BrokerDiscoveryProvider(this.proxyConfig, getZooKeeperClientFactory());
            this.configurationCacheService = new ConfigurationCacheService(discoveryProvider.globalZkCache);
            authorizationService = new AuthorizationService(PulsarConfigurationLoader.convertFrom(proxyConfig),
                                                            configurationCacheService);
        }

        ServerBootstrap bootstrap = new ServerBootstrap();
<<<<<<< HEAD
        bootstrap.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
=======
        bootstrap.childOption(ChannelOption.ALLOCATOR, PulsarByteBufAllocator.DEFAULT);
>>>>>>> f773c602c... Test pr 10 (#27)
        bootstrap.group(acceptorGroup, workerGroup);
        bootstrap.childOption(ChannelOption.TCP_NODELAY, true);
        bootstrap.childOption(ChannelOption.RCVBUF_ALLOCATOR,
                new AdaptiveRecvByteBufAllocator(1024, 16 * 1024, 1 * 1024 * 1024));

        bootstrap.channel(EventLoopUtil.getServerSocketChannelClass(workerGroup));
        EventLoopUtil.enableTriggeredMode(bootstrap);

        bootstrap.childHandler(new ServiceChannelInitializer(this, proxyConfig, false));
        // Bind and start to accept incoming connections.
        if (proxyConfig.getServicePort().isPresent()) {
            try {
<<<<<<< HEAD
                bootstrap.bind(proxyConfig.getServicePort().get()).sync();
                LOG.info("Started Pulsar Proxy at {}", serviceUrl);
=======
                listenChannel = bootstrap.bind(proxyConfig.getServicePort().get()).sync().channel();
                LOG.info("Started Pulsar Proxy at {}", listenChannel.localAddress());
>>>>>>> f773c602c... Test pr 10 (#27)
            } catch (Exception e) {
                throw new IOException("Failed to bind Pulsar Proxy on port " + proxyConfig.getServicePort().get(), e);
            }
        }
<<<<<<< HEAD
        LOG.info("Started Pulsar Proxy at {}", serviceUrl);
=======
>>>>>>> f773c602c... Test pr 10 (#27)

        if (proxyConfig.getServicePortTls().isPresent()) {
            ServerBootstrap tlsBootstrap = bootstrap.clone();
            tlsBootstrap.childHandler(new ServiceChannelInitializer(this, proxyConfig, true));
<<<<<<< HEAD
            tlsBootstrap.bind(proxyConfig.getServicePortTls().get()).sync();
            LOG.info("Started Pulsar TLS Proxy on port {}", proxyConfig.getServicePortTls().get());
=======
            listenChannelTls = tlsBootstrap.bind(proxyConfig.getServicePortTls().get()).sync().channel();
            LOG.info("Started Pulsar TLS Proxy on {}", listenChannelTls.localAddress());
        }

        final String hostname =
            ServiceConfigurationUtils.getDefaultOrConfiguredAddress(proxyConfig.getAdvertisedAddress());

        if (proxyConfig.getServicePort().isPresent()) {
            this.serviceUrl = String.format("pulsar://%s:%d/", hostname, getListenPort().get());
        } else {
            this.serviceUrl = null;
        }

        if (proxyConfig.getServicePortTls().isPresent()) {
            this.serviceUrlTls = String.format("pulsar+ssl://%s:%d/", hostname, getListenPortTls().get());
        } else {
            this.serviceUrlTls = null;
>>>>>>> f773c602c... Test pr 10 (#27)
        }
    }

    public ZooKeeperClientFactory getZooKeeperClientFactory() {
        if (zkClientFactory == null) {
            zkClientFactory = new ZookeeperClientFactoryImpl();
        }
        // Return default factory
        return zkClientFactory;
    }

    public BrokerDiscoveryProvider getDiscoveryProvider() {
        return discoveryProvider;
    }

    public void close() throws IOException {
        if (discoveryProvider != null) {
            discoveryProvider.close();
        }
<<<<<<< HEAD
=======

        if (listenChannel != null) {
            listenChannel.close();
        }

        if (listenChannelTls != null) {
            listenChannelTls.close();
        }

        if (statsExecutor != null) {
            statsExecutor.shutdown();
        }

        if (proxyAdditionalServlets != null) {
            proxyAdditionalServlets.close();
            proxyAdditionalServlets = null;
        }

>>>>>>> f773c602c... Test pr 10 (#27)
        acceptorGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

    public String getServiceUrl() {
        return serviceUrl;
    }

    public String getServiceUrlTls() {
        return serviceUrlTls;
    }

    public ProxyConfiguration getConfiguration() {
        return proxyConfig;
    }

    public AuthenticationService getAuthenticationService() {
        return authenticationService;
    }

    public AuthorizationService getAuthorizationService() {
        return authorizationService;
    }

    public ConfigurationCacheService getConfigurationCacheService() {
        return configurationCacheService;
    }

    public void setConfigurationCacheService(ConfigurationCacheService configurationCacheService) {
        this.configurationCacheService = configurationCacheService;
    }

    public Semaphore getLookupRequestSemaphore() {
        return lookupRequestSemaphore.get();
    }

    public EventLoopGroup getWorkerGroup() {
        return workerGroup;
    }

<<<<<<< HEAD
=======
    public Optional<Integer> getListenPort() {
        if (listenChannel != null) {
            return Optional.of(((InetSocketAddress) listenChannel.localAddress()).getPort());
        } else {
            return Optional.empty();
        }
    }

    public Optional<Integer> getListenPortTls() {
        if (listenChannelTls != null) {
            return Optional.of(((InetSocketAddress) listenChannelTls.localAddress()).getPort());
        } else {
            return Optional.empty();
        }
    }

>>>>>>> f773c602c... Test pr 10 (#27)
    private static final Logger LOG = LoggerFactory.getLogger(ProxyService.class);
}
