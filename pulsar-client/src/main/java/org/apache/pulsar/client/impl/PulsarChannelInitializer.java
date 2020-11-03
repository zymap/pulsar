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
import java.security.cert.X509Certificate;
import java.util.function.Supplier;

=======
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.util.ObjectCache;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.SecurityUtility;
import org.apache.pulsar.common.util.keystoretls.NettySSLContextAutoRefreshBuilder;

import io.netty.channel.Channel;
>>>>>>> f773c602c... Test pr 10 (#27)
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.ssl.SslContext;
<<<<<<< HEAD

import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.api.ByteBufPair;
import org.apache.pulsar.common.api.PulsarDecoder;
import org.apache.pulsar.common.util.SecurityUtility;

=======
import io.netty.handler.ssl.SslHandler;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
>>>>>>> f773c602c... Test pr 10 (#27)
public class PulsarChannelInitializer extends ChannelInitializer<SocketChannel> {

    public static final String TLS_HANDLER = "tls";

    private final Supplier<ClientCnx> clientCnxSupplier;
<<<<<<< HEAD
    private final SslContext sslCtx;
=======
    @Getter
    private final boolean tlsEnabled;
    private final boolean tlsEnabledWithKeyStore;

    private final Supplier<SslContext> sslContextSupplier;
    private NettySSLContextAutoRefreshBuilder nettySSLContextAutoRefreshBuilder;

    private static final long TLS_CERTIFICATE_CACHE_MILLIS = TimeUnit.MINUTES.toMillis(1);
>>>>>>> f773c602c... Test pr 10 (#27)

    public PulsarChannelInitializer(ClientConfigurationData conf, Supplier<ClientCnx> clientCnxSupplier)
            throws Exception {
        super();
        this.clientCnxSupplier = clientCnxSupplier;
<<<<<<< HEAD
        if (conf.isUseTls()) {
            // Set client certificate if available
            AuthenticationDataProvider authData = conf.getAuthentication().getAuthData();
            if (authData.hasDataForTls()) {
                this.sslCtx = SecurityUtility.createNettySslContextForClient(conf.isTlsAllowInsecureConnection(),
                        conf.getTlsTrustCertsFilePath(), (X509Certificate[]) authData.getTlsCertificates(),
                        authData.getTlsPrivateKey());
            } else {
                this.sslCtx = SecurityUtility.createNettySslContextForClient(conf.isTlsAllowInsecureConnection(),
                        conf.getTlsTrustCertsFilePath());
            }
        } else {
            this.sslCtx = null;
=======
        this.tlsEnabled = conf.isUseTls();
        this.tlsEnabledWithKeyStore = conf.isUseKeyStoreTls();

        if (tlsEnabled) {
            if (tlsEnabledWithKeyStore) {
                AuthenticationDataProvider authData1 = conf.getAuthentication().getAuthData();

                nettySSLContextAutoRefreshBuilder = new NettySSLContextAutoRefreshBuilder(
                            conf.getSslProvider(),
                            conf.isTlsAllowInsecureConnection(),
                            conf.getTlsTrustStoreType(),
                            conf.getTlsTrustStorePath(),
                            conf.getTlsTrustStorePassword(),
                            conf.getTlsCiphers(),
                            conf.getTlsProtocols(),
                            TLS_CERTIFICATE_CACHE_MILLIS,
                            authData1);
            }

            sslContextSupplier = new ObjectCache<SslContext>(() -> {
                try {
                    // Set client certificate if available
                    AuthenticationDataProvider authData = conf.getAuthentication().getAuthData();
                    if (authData.hasDataForTls()) {
                        return authData.getTlsTrustStoreStream() == null
                                ? SecurityUtility.createNettySslContextForClient(conf.isTlsAllowInsecureConnection(),
                                        conf.getTlsTrustCertsFilePath(),
                                        authData.getTlsCertificates(), authData.getTlsPrivateKey())
                                : SecurityUtility.createNettySslContextForClient(conf.isTlsAllowInsecureConnection(),
                                        authData.getTlsTrustStoreStream(),
                                        authData.getTlsCertificates(), authData.getTlsPrivateKey());
                    } else {
                        return SecurityUtility.createNettySslContextForClient(conf.isTlsAllowInsecureConnection(),
                                conf.getTlsTrustCertsFilePath());
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Failed to create TLS context", e);
                }
            }, TLS_CERTIFICATE_CACHE_MILLIS, TimeUnit.MILLISECONDS);
        } else {
            sslContextSupplier = null;
>>>>>>> f773c602c... Test pr 10 (#27)
        }
    }

    @Override
    public void initChannel(SocketChannel ch) throws Exception {
<<<<<<< HEAD
        if (sslCtx != null) {
            ch.pipeline().addLast(TLS_HANDLER, sslCtx.newHandler(ch.alloc()));
            ch.pipeline().addLast("ByteBufPairEncoder", ByteBufPair.COPYING_ENCODER);
        } else {
            ch.pipeline().addLast("ByteBufPairEncoder", ByteBufPair.ENCODER);
        }

        ch.pipeline().addLast("frameDecoder", new LengthFieldBasedFrameDecoder(PulsarDecoder.MaxFrameSize, 0, 4, 0, 4));
        ch.pipeline().addLast("handler", clientCnxSupplier.get());
    }
}
=======

        // Setup channel except for the SsHandler for TLS enabled connections

        ch.pipeline().addLast("ByteBufPairEncoder", tlsEnabled ? ByteBufPair.COPYING_ENCODER : ByteBufPair.ENCODER);

        ch.pipeline().addLast("frameDecoder", new LengthFieldBasedFrameDecoder(
                Commands.DEFAULT_MAX_MESSAGE_SIZE + Commands.MESSAGE_SIZE_FRAME_PADDING, 0, 4, 0, 4));
        ch.pipeline().addLast("handler", clientCnxSupplier.get());
    }

   /**
     * Initialize TLS for a channel. Should be invoked before the channel is connected to the remote address.
     *
     * @param ch      the channel
     * @param sniHost the value of this argument will be passed as peer host and port when creating the SSLEngine (which
     *                in turn will use these values to set SNI header when doing the TLS handshake). Cannot be
     *                <code>null</code>.
     * @return a {@link CompletableFuture} that completes when the TLS is set up.
     */
    CompletableFuture<Channel> initTls(Channel ch, InetSocketAddress sniHost) {
        Objects.requireNonNull(ch, "A channel is required");
        Objects.requireNonNull(sniHost, "A sniHost is required");
        if (!tlsEnabled) {
            throw new IllegalStateException("TLS is not enabled in client configuration");
        }
        CompletableFuture<Channel> initTlsFuture = new CompletableFuture<>();
        ch.eventLoop().execute(() -> {
            try {
                SslHandler handler = tlsEnabledWithKeyStore
                        ? new SslHandler(nettySSLContextAutoRefreshBuilder.get()
                                .createSSLEngine(sniHost.getHostString(), sniHost.getPort()))
                        : sslContextSupplier.get().newHandler(ch.alloc(), sniHost.getHostString(), sniHost.getPort());
                ch.pipeline().addFirst(TLS_HANDLER, handler);
                initTlsFuture.complete(ch);
            } catch (Throwable t) {
                initTlsFuture.completeExceptionally(t);
            }
        });

        return initTlsFuture;
    }
}

>>>>>>> f773c602c... Test pr 10 (#27)
