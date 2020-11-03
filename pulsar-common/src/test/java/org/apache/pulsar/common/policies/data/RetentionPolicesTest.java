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
package org.apache.pulsar.common.policies.data;

<<<<<<< HEAD
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.testng.Assert;
=======
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

>>>>>>> f773c602c... Test pr 10 (#27)
import org.testng.annotations.Test;

public class RetentionPolicesTest {

    @Test
    public void testRetentionPolices() {
        RetentionPolicies retentionPolicy0 = new RetentionPolicies();
        RetentionPolicies retentionPolicy1 = new RetentionPolicies(1, 100);
<<<<<<< HEAD
        Assert.assertFalse(retentionPolicy0.equals(null));
        Assert.assertTrue(retentionPolicy0.equals(retentionPolicy0));
        Assert.assertFalse(retentionPolicy0.hashCode() == retentionPolicy1.hashCode());
        Assert.assertFalse(retentionPolicy0.toString().equals(retentionPolicy1.toString()));
=======
        assertNotEquals(retentionPolicy0, null);
        assertEquals(retentionPolicy0, retentionPolicy0);
        assertNotEquals(retentionPolicy1.hashCode(), retentionPolicy0.hashCode());
        assertNotEquals(retentionPolicy1.toString(), retentionPolicy0.toString());
>>>>>>> f773c602c... Test pr 10 (#27)
    }
}
