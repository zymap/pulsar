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

import java.util.HashMap;
import java.util.Map;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TlsHostVerification extends TlsProducerConsumerBase {
    @Test
    public void testTlsHostVerificationAdminClient() throws Exception {
        Map<String, String> authParams = new HashMap<>();
        authParams.put("tlsCertFile", TLS_CLIENT_CERT_FILE_PATH);
        authParams.put("tlsKeyFile", TLS_CLIENT_KEY_FILE_PATH);
<<<<<<< HEAD
        PulsarAdmin adminClientTls = PulsarAdmin.builder()
                .serviceHttpUrl("https://127.0.0.1:" + BROKER_WEBSERVICE_PORT_TLS)
=======
        String websocketTlsAddress = pulsar.getWebServiceAddressTls();
        PulsarAdmin adminClientTls = PulsarAdmin.builder()
                .serviceHttpUrl(websocketTlsAddress.replace("localhost", "127.0.0.1"))
>>>>>>> f773c602c... Test pr 10 (#27)
                .tlsTrustCertsFilePath(TLS_TRUST_CERT_FILE_PATH).allowTlsInsecureConnection(false)
                .authentication(AuthenticationTls.class.getName(), authParams).enableTlsHostnameVerification(true)
                .build();

        try {
            adminClientTls.tenants().getTenants();
            Assert.fail("Admin call should be failed due to hostnameVerification enabled");
        } catch (PulsarAdminException e) {
            // Ok
        }
    }

    @Test
    public void testTlsHostVerificationDisabledAdminClient() throws Exception {
        Map<String, String> authParams = new HashMap<>();
        authParams.put("tlsCertFile", TLS_CLIENT_CERT_FILE_PATH);
        authParams.put("tlsKeyFile", TLS_CLIENT_KEY_FILE_PATH);
        PulsarAdmin adminClient = PulsarAdmin.builder()
<<<<<<< HEAD
                .serviceHttpUrl("https://127.0.0.1:" + BROKER_WEBSERVICE_PORT_TLS)
=======
                .serviceHttpUrl(pulsar.getWebServiceAddressTls())
>>>>>>> f773c602c... Test pr 10 (#27)
                .tlsTrustCertsFilePath(TLS_TRUST_CERT_FILE_PATH).allowTlsInsecureConnection(false)
                .authentication(AuthenticationTls.class.getName(), authParams).enableTlsHostnameVerification(false)
                .build();

        // Should not fail, since verification is disabled
        adminClient.tenants().getTenants();
    }
}
