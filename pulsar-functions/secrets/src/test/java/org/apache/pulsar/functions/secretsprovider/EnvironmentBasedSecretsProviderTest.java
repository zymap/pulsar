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

package org.apache.pulsar.functions.secretsprovider;

<<<<<<< HEAD
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.Assert;
import org.testng.annotations.Test;
=======
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
>>>>>>> f773c602c... Test pr 10 (#27)

import java.lang.reflect.Field;
import java.util.Map;

<<<<<<< HEAD
import static org.mockito.Matchers.anyString;

/**
 * Unit test of {@link Exceptions}.
 */
=======
import org.testng.annotations.Test;

>>>>>>> f773c602c... Test pr 10 (#27)
public class EnvironmentBasedSecretsProviderTest {
    @Test
    public void testConfigValidation() throws Exception {
        EnvironmentBasedSecretsProvider provider = new EnvironmentBasedSecretsProvider();
<<<<<<< HEAD
        Assert.assertEquals(provider.provideSecret("mySecretName", "Ignored"), null);
        injectEnvironmentVariable("mySecretName", "SecretValue");
        Assert.assertEquals(provider.provideSecret("mySecretName", "Ignored"), "SecretValue");
=======
        assertNull(provider.provideSecret("mySecretName", "Ignored"));
        injectEnvironmentVariable("mySecretName", "SecretValue");
        assertEquals(provider.provideSecret("mySecretName", "Ignored"), "SecretValue");
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    private static void injectEnvironmentVariable(String key, String value)
            throws Exception {

        Class<?> processEnvironment = Class.forName("java.lang.ProcessEnvironment");

        Field unmodifiableMapField = getAccessibleField(processEnvironment, "theUnmodifiableEnvironment");
        Object unmodifiableMap = unmodifiableMapField.get(null);
        injectIntoUnmodifiableMap(key, value, unmodifiableMap);

        Field mapField = getAccessibleField(processEnvironment, "theEnvironment");
        Map<String, String> map = (Map<String, String>) mapField.get(null);
        map.put(key, value);
    }

    private static Field getAccessibleField(Class<?> clazz, String fieldName)
            throws NoSuchFieldException {

        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field;
    }

    private static void injectIntoUnmodifiableMap(String key, String value, Object map)
            throws ReflectiveOperationException {

        Class unmodifiableMap = Class.forName("java.util.Collections$UnmodifiableMap");
        Field field = getAccessibleField(unmodifiableMap, "m");
        Object obj = field.get(map);
        ((Map<String, String>) obj).put(key, value);
    }
}
