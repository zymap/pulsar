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
package org.apache.pulsar.functions.instance;

import lombok.Data;
<<<<<<< HEAD
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
=======
import org.apache.pulsar.functions.proto.Function;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.functions.proto.Function.FunctionDetails;

/**
 * This is the config passed to the Java Instance. Contains all the information
<<<<<<< HEAD
 * passed to run functions
 */
@Data
@Getter
@Setter
@EqualsAndHashCode
@ToString
=======
 * passed to run functions.
 */
@Data
>>>>>>> f773c602c... Test pr 10 (#27)
public class InstanceConfig {
    private int instanceId;
    private String functionId;
    private String functionVersion;
    private FunctionDetails functionDetails;
    private int maxBufferedTuples;
<<<<<<< HEAD
    private int port;
    private String clusterName;
=======
    private Function.FunctionAuthenticationSpec functionAuthenticationSpec;
    private int port;
    private String clusterName;
    // Max pending async requests per instance to avoid large number of concurrent requests.
    // Only used in AsyncFunction. Default: 1000
    private int maxPendingAsyncRequests = 1000;
>>>>>>> f773c602c... Test pr 10 (#27)

    /**
     * Get the string representation of {@link #getInstanceId()}.
     *
     * @return the string representation of {@link #getInstanceId()}.
     */
    public String getInstanceName() {
        return "" + instanceId;
    }
<<<<<<< HEAD
=======

    public FunctionDetails getFunctionDetails() {
        return functionDetails;
    }
>>>>>>> f773c602c... Test pr 10 (#27)
}
