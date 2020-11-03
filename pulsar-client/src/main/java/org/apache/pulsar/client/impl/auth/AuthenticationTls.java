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
package org.apache.pulsar.client.impl.auth;

<<<<<<< HEAD
import java.io.IOException;
import java.security.Security;
import java.util.Map;
=======
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.function.Supplier;
>>>>>>> f773c602c... Test pr 10 (#27)

import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.EncodedAuthenticationParameterSupport;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.AuthenticationUtil;

<<<<<<< HEAD
=======
import com.google.common.annotations.VisibleForTesting;

>>>>>>> f773c602c... Test pr 10 (#27)
/**
 *
 * This plugin requires these parameters
 *
 * tlsCertFile: A file path for a client certificate. tlsKeyFile: A file path for a client private key.
 *
 */
public class AuthenticationTls implements Authentication, EncodedAuthenticationParameterSupport {
<<<<<<< HEAD

=======
    private final static String AUTH_NAME = "tls";
>>>>>>> f773c602c... Test pr 10 (#27)
    private static final long serialVersionUID = 1L;

    private String certFilePath;
    private String keyFilePath;
<<<<<<< HEAD

    // Load Bouncy Castle
    static {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
    }
    
=======
    private Supplier<ByteArrayInputStream> certStreamProvider, keyStreamProvider, trustStoreStreamProvider;

>>>>>>> f773c602c... Test pr 10 (#27)
    public AuthenticationTls() {
    }

    public AuthenticationTls(String certFilePath, String keyFilePath) {
        this.certFilePath = certFilePath;
        this.keyFilePath = keyFilePath;
    }

<<<<<<< HEAD
=======
    public AuthenticationTls(Supplier<ByteArrayInputStream> certStreamProvider,
            Supplier<ByteArrayInputStream> keyStreamProvider) {
        this(certStreamProvider, keyStreamProvider, null);
    }

    public AuthenticationTls(Supplier<ByteArrayInputStream> certStreamProvider,
            Supplier<ByteArrayInputStream> keyStreamProvider, Supplier<ByteArrayInputStream> trustStoreStreamProvider) {
        this.certStreamProvider = certStreamProvider;
        this.keyStreamProvider = keyStreamProvider;
        this.trustStoreStreamProvider = trustStoreStreamProvider;
    }

>>>>>>> f773c602c... Test pr 10 (#27)
    @Override
    public void close() throws IOException {
        // noop
    }

    @Override
    public String getAuthMethodName() {
<<<<<<< HEAD
        return "tls";
=======
        return AUTH_NAME;
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Override
    public AuthenticationDataProvider getAuthData() throws PulsarClientException {
        try {
<<<<<<< HEAD
            return new AuthenticationDataTls(certFilePath, keyFilePath);
        } catch (Exception e) {
            throw new PulsarClientException(e);
        }
=======
            if (certFilePath != null && keyFilePath != null) {
                return new AuthenticationDataTls(certFilePath, keyFilePath);
            } else if (certStreamProvider != null && keyStreamProvider != null) {
                return new AuthenticationDataTls(certStreamProvider, keyStreamProvider, trustStoreStreamProvider);
            }
        } catch (Exception e) {
            throw new PulsarClientException(e);
        }
        throw new IllegalArgumentException("cert/key file path or cert/key stream must be present");
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Override
    public void configure(String encodedAuthParamString) {
<<<<<<< HEAD
        setAuthParams(AuthenticationUtil.configureFromPulsar1AuthParamString(encodedAuthParamString));
=======
        Map<String, String> authParamsMap = null;
        try {
            authParamsMap = AuthenticationUtil.configureFromJsonString(encodedAuthParamString);
        } catch (Exception e) {
            // auth-param is not in json format
        }
        authParamsMap = (authParamsMap == null || authParamsMap.isEmpty())
                ? AuthenticationUtil.configureFromPulsar1AuthParamString(encodedAuthParamString)
                : authParamsMap;
        setAuthParams(authParamsMap);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Override
    @Deprecated
    public void configure(Map<String, String> authParams) {
        setAuthParams(authParams);
    }

    @Override
    public void start() throws PulsarClientException {
        // noop
    }

    private void setAuthParams(Map<String, String> authParams) {
        certFilePath = authParams.get("tlsCertFile");
        keyFilePath = authParams.get("tlsKeyFile");
    }

<<<<<<< HEAD
=======
    @VisibleForTesting
    public String getCertFilePath() {
        return certFilePath;
    }

    @VisibleForTesting
    public String getKeyFilePath() {
        return keyFilePath;
    }

>>>>>>> f773c602c... Test pr 10 (#27)
}
