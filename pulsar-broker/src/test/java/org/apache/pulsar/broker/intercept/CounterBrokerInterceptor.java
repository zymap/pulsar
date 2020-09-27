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

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.intercept.InterceptException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

@Slf4j
public class CounterBrokerInterceptor implements BrokerInterceptor {

    int count = 0;

    @Override
    public void onPulsarCommand(PulsarApi.BaseCommand command, ServerCnx cnx) throws InterceptException {
        log.info("[{}] On [{}] Pulsar command", count, command.getType().name());
        count ++;
    }

    @Override
    public void onConnectionClosed(ServerCnx cnx) {
        // np-op
    }

    @Override
    public void onWebserviceRequest(ServletRequest request) throws IOException, ServletException, InterceptException {
        count ++;
        log.info("[{}] On [{}] Webservice request", count, ((HttpServletRequest)request).getRequestURL().toString());
    }

    @Override
    public void onWebserviceResponse(ServletRequest request, ServletResponse response) throws IOException, ServletException {
        count ++;
        log.info("[{}] On [{}] Webservice response", count, ((HttpServletRequest)request).getRequestURL().toString());
    }

    @Override
    public void initialize(PulsarService pulsarService) throws Exception {

    }

    @Override
    public void close() {

    }

    public int getCount() {
        return count;
    }
}
