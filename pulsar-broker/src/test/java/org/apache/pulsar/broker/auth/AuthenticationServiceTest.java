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
package org.apache.pulsar.broker.auth;

<<<<<<< HEAD
import static org.mockito.Matchers.anyString;
=======
import static org.mockito.Mockito.anyString;
>>>>>>> f773c602c... Test pr 10 (#27)
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.io.IOException;
import java.util.Set;

import javax.naming.AuthenticationException;
import javax.servlet.http.HttpServletRequest;

import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;

public class AuthenticationServiceTest {

    private static final String s_authentication_success = "authenticated";

    @Test(timeOut = 10000)
    public void testAuthentication() throws Exception {
        ServiceConfiguration config = new ServiceConfiguration();
        Set<String> providersClassNames = Sets.newHashSet(MockAuthenticationProvider.class.getName());
        config.setAuthenticationProviders(providersClassNames);
        config.setAuthenticationEnabled(true);
        AuthenticationService service = new AuthenticationService(config);
        String result = service.authenticate(null, "auth");
        assertEquals(result, s_authentication_success);
        service.close();
    }

    @Test(timeOut = 10000)
    public void testAuthenticationHttp() throws Exception {
        ServiceConfiguration config = new ServiceConfiguration();
        Set<String> providersClassNames = Sets.newHashSet(MockAuthenticationProvider.class.getName());
        config.setAuthenticationProviders(providersClassNames);
        config.setAuthenticationEnabled(true);
        AuthenticationService service = new AuthenticationService(config);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("192.168.1.1");
        when(request.getRemotePort()).thenReturn(8080);
        when(request.getHeader(anyString())).thenReturn("data");
        String result = service.authenticateHttpRequest(request);
        assertEquals(result, s_authentication_success);
        service.close();
    }

    public static class MockAuthenticationProvider implements AuthenticationProvider {

        @Override
        public void close() throws IOException {
        }

        @Override
        public void initialize(ServiceConfiguration config) throws IOException {
        }

        @Override
        public String getAuthMethodName() {
            return "auth";
        }

        @Override
        public String authenticate(AuthenticationDataSource authData) throws AuthenticationException {
            return s_authentication_success;
        }
    }
}
