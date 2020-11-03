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
package org.apache.pulsar.discovery.service;

<<<<<<< HEAD
import com.google.common.base.Preconditions;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
=======
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Preconditions;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import lombok.Getter;
>>>>>>> f773c602c... Test pr 10 (#27)

import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
<<<<<<< HEAD
=======
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.apache.pulsar.discovery.service.server.ServiceConfig;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.pulsar.zookeeper.ZookeeperClientFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

<<<<<<< HEAD
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;

=======
>>>>>>> f773c602c... Test pr 10 (#27)
/**
 * Main discovery-service which starts component to serve incoming discovery-request over binary-proto channel and
 * redirects to one of the active broker
 *
 */
public class DiscoveryService implements Closeable {

    private final ServiceConfig config;
<<<<<<< HEAD
    private final String serviceUrl;
    private final String serviceUrlTls;
=======
    private String serviceUrl;
    private String serviceUrlTls;
>>>>>>> f773c602c... Test pr 10 (#27)
    private ConfigurationCacheService configurationCacheService;
    private AuthenticationService authenticationService;
    private AuthorizationService authorizationService;
    private ZooKeeperClientFactory zkClientFactory = null;
    private BrokerDiscoveryProvider discoveryProvider;
    private final EventLoopGroup acceptorGroup;
<<<<<<< HEAD
=======
    @Getter
>>>>>>> f773c602c... Test pr 10 (#27)
    private final EventLoopGroup workerGroup;
    private final DefaultThreadFactory acceptorThreadFactory = new DefaultThreadFactory("pulsar-discovery-acceptor");
    private final DefaultThreadFactory workersThreadFactory = new DefaultThreadFactory("pulsar-discovery-io");
    private final int numThreads = Runtime.getRuntime().availableProcessors();

<<<<<<< HEAD
    public DiscoveryService(ServiceConfig serviceConfig) {
        checkNotNull(serviceConfig);
        this.config = serviceConfig;
        this.serviceUrl = serviceUrl();
        this.serviceUrlTls = serviceUrlTls();
=======
    private Channel channelListen;
    private Channel channelListenTls;

    public DiscoveryService(ServiceConfig serviceConfig) {
        checkNotNull(serviceConfig);
        this.config = serviceConfig;
>>>>>>> f773c602c... Test pr 10 (#27)
        this.acceptorGroup = EventLoopUtil.newEventLoopGroup(1, acceptorThreadFactory);
        this.workerGroup = EventLoopUtil.newEventLoopGroup(numThreads, workersThreadFactory);
    }

    /**
<<<<<<< HEAD
     * Starts discovery service by initializing zookkeeper and server
=======
     * Starts discovery service by initializing ZooKeeper and server
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @throws Exception
     */
    public void start() throws Exception {
        discoveryProvider = new BrokerDiscoveryProvider(this.config, getZooKeeperClientFactory());
        this.configurationCacheService = new ConfigurationCacheService(discoveryProvider.globalZkCache);
        ServiceConfiguration serviceConfiguration = PulsarConfigurationLoader.convertFrom(config);
        authenticationService = new AuthenticationService(serviceConfiguration);
        authorizationService = new AuthorizationService(serviceConfiguration, configurationCacheService);
        startServer();
    }

    /**
     * starts server to handle discovery-request from client-channel
     *
     * @throws Exception
     */
    public void startServer() throws Exception {

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

        bootstrap.childHandler(new ServiceChannelInitializer(this, config, false));
        // Bind and start to accept incoming connections.
<<<<<<< HEAD
        
        Preconditions.checkArgument(config.getServicePort().isPresent() || config.getServicePortTls().isPresent(), 
                "Either ServicePort or ServicePortTls should be configured.");
        
        if (config.getServicePort().isPresent()) {
            // Bind and start to accept incoming connections.
            bootstrap.bind(config.getServicePort().get()).sync();
            LOG.info("Started Pulsar Discovery service on port {}", config.getServicePort());
        }
        
        if (config.getServicePortTls().isPresent()) {
            ServerBootstrap tlsBootstrap = bootstrap.clone();
            tlsBootstrap.childHandler(new ServiceChannelInitializer(this, config, true));
            tlsBootstrap.bind(config.getServicePortTls().get()).sync();
            LOG.info("Started Pulsar Discovery TLS service on port {}", config.getServicePortTls().get());
        }
        
=======

        Preconditions.checkArgument(config.getServicePort().isPresent() || config.getServicePortTls().isPresent(),
                "Either ServicePort or ServicePortTls should be configured.");

        if (config.getServicePort().isPresent()) {
            // Bind and start to accept incoming connections.
            channelListen = bootstrap.bind(config.getServicePort().get()).sync().channel();
            LOG.info("Started Pulsar Discovery service on {}", channelListen.localAddress());
        }

        if (config.getServicePortTls().isPresent()) {
            ServerBootstrap tlsBootstrap = bootstrap.clone();
            tlsBootstrap.childHandler(new ServiceChannelInitializer(this, config, true));
            channelListenTls = tlsBootstrap.bind(config.getServicePortTls().get()).sync().channel();
            LOG.info("Started Pulsar Discovery TLS service on port {}", channelListenTls.localAddress());
        }

        this.serviceUrl = serviceUrl();
        this.serviceUrlTls = serviceUrlTls();
>>>>>>> f773c602c... Test pr 10 (#27)
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
        discoveryProvider.close();
        acceptorGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

    /**
     * Derive the host
     *
<<<<<<< HEAD
     * @param isBindOnLocalhost
     * @return
=======
     * @return String containing the hostname
>>>>>>> f773c602c... Test pr 10 (#27)
     */
    public String host() {
        try {
            if (!config.isBindOnLocalhost()) {
                return InetAddress.getLocalHost().getHostName();
            } else {
                return "localhost";
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new IllegalStateException("failed to find host", e);
        }
    }

    public String serviceUrl() {
        if (config.getServicePort().isPresent()) {
<<<<<<< HEAD
            return new StringBuilder("pulsar://").append(host()).append(":").append(config.getServicePort().get())
=======
            return new StringBuilder("pulsar://").append(host()).append(":")
                    .append(((InetSocketAddress) channelListen.localAddress()).getPort())
>>>>>>> f773c602c... Test pr 10 (#27)
                    .toString();
        } else {
            return null;
        }
    }

    public String serviceUrlTls() {
        if (config.getServicePortTls().isPresent()) {
<<<<<<< HEAD
            return new StringBuilder("pulsar+ssl://").append(host()).append(":").append(config.getServicePortTls().get())
=======
            return new StringBuilder("pulsar+ssl://").append(host()).append(":")
                    .append(((InetSocketAddress) channelListenTls.localAddress()).getPort())
>>>>>>> f773c602c... Test pr 10 (#27)
                    .toString();
        } else {
            return null;
        }
    }

    public String getServiceUrl() {
        return serviceUrl;
    }

    public String getServiceUrlTls() {
        return serviceUrlTls;
    }

    public ServiceConfig getConfiguration() {
        return config;
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

    private static final Logger LOG = LoggerFactory.getLogger(DiscoveryService.class);
<<<<<<< HEAD
}
=======
}
>>>>>>> f773c602c... Test pr 10 (#27)
