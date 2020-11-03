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
package org.apache.pulsar.common.policies.impl;

<<<<<<< HEAD
import org.apache.pulsar.common.policies.data.OldPolicies;
import org.apache.pulsar.common.policies.impl.MinAvailablePolicy;
import org.testng.Assert;
=======
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import org.apache.pulsar.common.policies.data.OldPolicies;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.testng.annotations.Test;

public class MinAvailablePolicyTest {

    @Test
    public void testMinAvailablePolicty() {
        MinAvailablePolicy m = new MinAvailablePolicy(3, 10);
<<<<<<< HEAD
        Assert.assertFalse(m.equals(new OldPolicies()));
        Assert.assertFalse(m.shouldFailoverToSecondary(15));
        Assert.assertTrue(m.shouldFailoverToSecondary(2));
=======
        assertNotEquals(new OldPolicies(), m);
        assertFalse(m.shouldFailoverToSecondary(15));
        assertTrue(m.shouldFailoverToSecondary(2));
>>>>>>> f773c602c... Test pr 10 (#27)
    }
}
