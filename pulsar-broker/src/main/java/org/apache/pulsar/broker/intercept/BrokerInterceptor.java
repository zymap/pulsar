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
package org.apache.pulsar.broker.intercept;

import com.google.common.annotations.Beta;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.common.api.proto.PulsarApi.BaseCommand;
import org.apache.pulsar.common.intercept.InterceptException;

import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import java.io.IOException;

/**
 * A plugin interface that allows you to intercept the
 * client requests to the Pulsar brokers.
 *
 * <p>BrokerInterceptor callbacks may be called from multiple threads. Interceptor
 * implementation must ensure thread-safety, if needed.
 */
@Beta
public interface BrokerInterceptor extends AutoCloseable {

    /**
     * Called by the broker while new command incoming.
     */
    void onPulsarCommand(BaseCommand command, ServerCnx cnx) throws InterceptException;

    /**
     * Called by the broker while connection closed.
     */
    void onConnectionClosed(ServerCnx cnx);

    /**
     * Called by the web service while new request incoming.
     */
    void onWebserviceRequest(ServletRequest request) throws IOException, ServletException, InterceptException;

    /**
     * Intercept the webservice response before send to client.
     */
    void onWebserviceResponse(ServletRequest request, ServletResponse response) throws IOException, ServletException;

    /**
     * Initialize the broker interceptor.
     *
     * @throws Exception when fail to initialize the broker interceptor.
     */
    void initialize(PulsarService pulsarService) throws Exception;

    BrokerInterceptor DISABLED = new BrokerInterceptorDisabled();

    /**
     * Broker interceptor disabled implementation.
     */
    class BrokerInterceptorDisabled implements BrokerInterceptor {

        @Override
        public void onPulsarCommand(BaseCommand command, ServerCnx cnx) throws InterceptException {
            // no-op
        }

        @Override
        public void onConnectionClosed(ServerCnx cnx) {
            // no-op
        }

        @Override
        public void onWebserviceRequest(ServletRequest request) throws IOException, ServletException, InterceptException {
            // no-op
        }

        @Override
        public void onWebserviceResponse(ServletRequest request, ServletResponse response) throws IOException, ServletException {
            // no-op
        }

        @Override
        public void initialize(PulsarService pulsarService) throws Exception {
            // no-op
        }

        @Override
        public void close() {
            // no-op
        }
    }

    /**
     * Close this broker interceptor.
     */
    @Override
    void close();
}
