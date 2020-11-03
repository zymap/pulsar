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
package org.apache.pulsar.client.api;

/**
 * The provider to provide the service url.
<<<<<<< HEAD
 * <p>
 * This allows applications to retrieve the service URL from an external configuration provider and, more importantly,
 * to force the Pulsar client to reconnect if the service URL has been changed.
 * <p>
 * It can be passed with {@link ClientBuilder#serviceUrlProvider(ServiceUrlProvider)}
=======
 *
 * <p>This allows applications to retrieve the service URL from an external configuration provider and,
 * more importantly, to force the Pulsar client to reconnect if the service URL has been changed.
 *
 * <p>It can be passed with {@link ClientBuilder#serviceUrlProvider(ServiceUrlProvider)}
>>>>>>> f773c602c... Test pr 10 (#27)
 */
public interface ServiceUrlProvider {

    /**
     * Initialize the service url provider with Pulsar client instance.
<<<<<<< HEAD
     * <p>
     * This can be used by the provider to force the Pulsar client to reconnect whenever the service url might have
=======
     *
     * <p>This can be used by the provider to force the Pulsar client to reconnect whenever the service url might have
>>>>>>> f773c602c... Test pr 10 (#27)
     * changed. See {@link PulsarClient#updateServiceUrl(String)}.
     *
     * @param client
     *            created pulsar client.
     */
    void initialize(PulsarClient client);

    /**
     * Get the current service URL the Pulsar client should connect to.
     *
     * @return the pulsar service url.
     */
    String getServiceUrl();

}
