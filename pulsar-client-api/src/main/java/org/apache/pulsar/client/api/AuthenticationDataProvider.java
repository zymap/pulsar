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

<<<<<<< HEAD
=======
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.InputStream;
>>>>>>> f773c602c... Test pr 10 (#27)
import java.io.Serializable;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.util.Map;
import java.util.Set;

<<<<<<< HEAD
/**
 * Interface for accessing data which are used in variety of authentication schemes on client side
=======
import javax.naming.AuthenticationException;

import org.apache.pulsar.common.api.AuthData;

/**
 * Interface for accessing data which are used in variety of authentication schemes on client side.
>>>>>>> f773c602c... Test pr 10 (#27)
 */
public interface AuthenticationDataProvider extends Serializable {
    /*
     * TLS
     */

    /**
     * Check if data for TLS are available.
<<<<<<< HEAD
     * 
=======
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @return true if this authentication data contain data for TLS
     */
    default boolean hasDataForTls() {
        return false;
    }

    /**
<<<<<<< HEAD
     * 
=======
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @return a client certificate chain, or null if the data are not available
     */
    default Certificate[] getTlsCertificates() {
        return null;
    }

    /**
<<<<<<< HEAD
     * 
=======
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @return a private key for the client certificate, or null if the data are not available
     */
    default PrivateKey getTlsPrivateKey() {
        return null;
    }

<<<<<<< HEAD
=======
    /**
     *
     * @return an input-stream of the trust store, or null if the trust-store provided at
     *         {@link ClientConfigurationData#getTlsTrustStorePath()}
     */
    default InputStream getTlsTrustStoreStream() {
        return null;
    }

    /**
     * Used for TLS authentication with keystore type.
     *
     * @return a KeyStoreParams for the client certificate chain, or null if the data are not available
     */
    default KeyStoreParams getTlsKeyStoreParams() {
        return null;
    }

>>>>>>> f773c602c... Test pr 10 (#27)
    /*
     * HTTP
     */

    /**
     * Check if data for HTTP are available.
<<<<<<< HEAD
     * 
=======
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @return true if this authentication data contain data for HTTP
     */
    default boolean hasDataForHttp() {
        return false;
    }

    /**
<<<<<<< HEAD
     * 
     * @return a authentication scheme, or <code>null<c/ode> if the request will not be authenticated
=======
     *
     * @return a authentication scheme, or {@code null} if the request will not be authenticated.
>>>>>>> f773c602c... Test pr 10 (#27)
     */
    default String getHttpAuthType() {
        return null;
    }

    /**
<<<<<<< HEAD
     * 
     * @return an enumeration of all the header names
     */
    default Set<Map.Entry<String, String>> getHttpHeaders() {
=======
     *
     * @return an enumeration of all the header names
     */
    default Set<Map.Entry<String, String>> getHttpHeaders() throws Exception {
>>>>>>> f773c602c... Test pr 10 (#27)
        return null;
    }

    /*
     * Command
     */

    /**
     * Check if data from Pulsar protocol are available.
<<<<<<< HEAD
     * 
=======
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @return true if this authentication data contain data from Pulsar protocol
     */
    default boolean hasDataFromCommand() {
        return false;
    }

    /**
<<<<<<< HEAD
     * 
=======
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @return authentication data which will be stored in a command
     */
    default String getCommandData() {
        return null;
    }

<<<<<<< HEAD
=======
    /**
     * For mutual authentication, This method use passed in `data` to evaluate and challenge,
     * then returns null if authentication has completed;
     * returns authenticated data back to server side, if authentication has not completed.
     *
     * <p>Mainly used for mutual authentication like sasl.
     */
    default AuthData authenticate(AuthData data) throws AuthenticationException {
        byte[] bytes = (hasDataFromCommand() ? this.getCommandData() : "").getBytes(UTF_8);
        return AuthData.of(bytes);
    }
>>>>>>> f773c602c... Test pr 10 (#27)
}
