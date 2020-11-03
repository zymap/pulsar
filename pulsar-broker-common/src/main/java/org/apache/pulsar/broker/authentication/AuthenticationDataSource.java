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
package org.apache.pulsar.broker.authentication;

import java.net.SocketAddress;
import java.security.cert.Certificate;
<<<<<<< HEAD
=======
import javax.naming.AuthenticationException;
import org.apache.pulsar.common.api.AuthData;
>>>>>>> f773c602c... Test pr 10 (#27)

/**
 * Interface for accessing data which are used in variety of authentication schemes on server side
 */
public interface AuthenticationDataSource {
    /*
     * TLS
     */

    /**
     * Check if data from TLS are available.
<<<<<<< HEAD
     * 
=======
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @return true if this authentication data contain data from TLS
     */
    default boolean hasDataFromTls() {
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

    /*
     * HTTP
     */

    /**
     * Check if data from HTTP are available.
<<<<<<< HEAD
     * 
=======
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @return true if this authentication data contain data from HTTP
     */
    default boolean hasDataFromHttp() {
        return false;
    }

    /**
<<<<<<< HEAD
     * 
=======
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @return a authentication scheme, or <code>null<c/ode> if the request is not be authenticated
     */
    default String getHttpAuthType() {
        return null;
    }

    /**
<<<<<<< HEAD
     * 
=======
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @return a <code>String</code> containing the value of the specified header, or <code>null</code> if the header
     *         does not exist.
     */
    default String getHttpHeader(String name) {
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
     * @return authentication data which is stored in a command
     */
    default String getCommandData() {
        return null;
    }

<<<<<<< HEAD
=======
    /**
     * Evaluate and challenge the data that passed in, and return processed data back.
     * It is used for mutual authentication like SASL.
     */
    default AuthData authenticate(AuthData data) throws AuthenticationException {
        throw new AuthenticationException("Not supported");
    }

>>>>>>> f773c602c... Test pr 10 (#27)
    /*
     * Peer
     */

    /**
     * Check if data from peer are available.
<<<<<<< HEAD
     * 
=======
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @return true if this authentication data contain data from peer
     */
    default boolean hasDataFromPeer() {
        return false;
    }

    /**
<<<<<<< HEAD
     * 
=======
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @return a <code>String</code> containing the IP address of the client
     */
    default SocketAddress getPeerAddress() {
        return null;
    }
<<<<<<< HEAD
=======

    /**
     * Check if subscription is defined available.
     *
     * @return true if this authentication data contain subscription
     */
    default boolean hasSubscription() {
        return false;
    }

    /**
     * Subscription name can be necessary for consumption
     *
     * @return a <code>String</code> containing the subscription name
     */
    default String getSubscription() { return null; }

    /**
     * Subscription name can be necessary for consumption
     *
     * @return a <code>String</code> containing the subscription name
     */
    default void setSubscription(String subscription) { };
>>>>>>> f773c602c... Test pr 10 (#27)
}
