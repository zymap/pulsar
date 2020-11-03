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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
<<<<<<< HEAD
import lombok.Data;
import org.apache.pulsar.common.util.ObjectMapperFactory;

=======
>>>>>>> f773c602c... Test pr 10 (#27)
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
<<<<<<< HEAD

@Data
@JsonInclude(JsonInclude.Include.ALWAYS)
@JsonPropertyOrder({ "receivedTotal", "processedSuccessfullyTotal", "systemExceptionsTotal", "userExceptionsTotal", "avgProcessLatency", "1min", "lastInvocation", "instances" })
public class FunctionStats {

    /**
     * Overall total number of records function received from source
=======
import lombok.Data;
import org.apache.pulsar.common.util.ObjectMapperFactory;

/**
 * Statistics for Pulsar Function.
 */
@Data
@JsonInclude(JsonInclude.Include.ALWAYS)
@JsonPropertyOrder({"receivedTotal", "processedSuccessfullyTotal", "systemExceptionsTotal", "userExceptionsTotal",
    "avgProcessLatency", "1min", "lastInvocation", "instances"})
public class FunctionStats {

    /**
     * Overall total number of records function received from source.
>>>>>>> f773c602c... Test pr 10 (#27)
     **/
    public long receivedTotal;

    /**
<<<<<<< HEAD
     * Overall total number of records successfully processed by user function
=======
     * Overall total number of records successfully processed by user function.
>>>>>>> f773c602c... Test pr 10 (#27)
     **/
    public long processedSuccessfullyTotal;

    /**
<<<<<<< HEAD
     * Overall total number of system exceptions thrown
=======
     * Overall total number of system exceptions thrown.
>>>>>>> f773c602c... Test pr 10 (#27)
     **/
    public long systemExceptionsTotal;

    /**
<<<<<<< HEAD
     * Overall total number of user exceptions thrown
=======
     * Overall total number of user exceptions thrown.
>>>>>>> f773c602c... Test pr 10 (#27)
     **/
    public long userExceptionsTotal;

    /**
<<<<<<< HEAD
     * Average process latency for function
=======
     * Average process latency for function.
>>>>>>> f773c602c... Test pr 10 (#27)
     **/
    public Double avgProcessLatency;

    @JsonProperty("1min")
<<<<<<< HEAD
    public FunctionInstanceStats.FunctionInstanceStatsDataBase oneMin = new FunctionInstanceStats.FunctionInstanceStatsDataBase();

    /**
     * Timestamp of when the function was last invoked by any instance
     **/
    public Long lastInvocation;

=======
    public FunctionInstanceStats.FunctionInstanceStatsDataBase oneMin =
        new FunctionInstanceStats.FunctionInstanceStatsDataBase();

    /**
     * Timestamp of when the function was last invoked by any instance.
     **/
    public Long lastInvocation;

    /**
     * Function instance statistics.
     */
>>>>>>> f773c602c... Test pr 10 (#27)
    @Data
    @JsonInclude(JsonInclude.Include.ALWAYS)
    @JsonPropertyOrder({ "instanceId", "metrics" })
    public static class FunctionInstanceStats {

<<<<<<< HEAD
        /** Instance Id of function instance **/
        public int instanceId;

        @Data
        @JsonInclude(JsonInclude.Include.ALWAYS)
        @JsonPropertyOrder({ "receivedTotal", "processedSuccessfullyTotal", "systemExceptionsTotal", "userExceptionsTotal", "avgProcessLatency" })
        public static class FunctionInstanceStatsDataBase {
            /**
             * Total number of records function received from source for instance
=======
        /** Instance Id of function instance. **/
        public int instanceId;

      /**
       * Function instance statistics data base.
       */
      @Data
        @JsonInclude(JsonInclude.Include.ALWAYS)
        @JsonPropertyOrder({ "receivedTotal", "processedSuccessfullyTotal", "systemExceptionsTotal",
            "userExceptionsTotal", "avgProcessLatency" })
        public static class FunctionInstanceStatsDataBase {
            /**
             * Total number of records function received from source for instance.
>>>>>>> f773c602c... Test pr 10 (#27)
             **/
            public long receivedTotal;

            /**
<<<<<<< HEAD
             * Total number of records successfully processed by user function for instance
=======
             * Total number of records successfully processed by user function for instance.
>>>>>>> f773c602c... Test pr 10 (#27)
             **/
            public long processedSuccessfullyTotal;

            /**
<<<<<<< HEAD
             * Total number of system exceptions thrown for instance
=======
             * Total number of system exceptions thrown for instance.
>>>>>>> f773c602c... Test pr 10 (#27)
             **/
            public long systemExceptionsTotal;

            /**
<<<<<<< HEAD
             * Total number of user exceptions thrown for instance
=======
             * Total number of user exceptions thrown for instance.
>>>>>>> f773c602c... Test pr 10 (#27)
             **/
            public long userExceptionsTotal;

            /**
<<<<<<< HEAD
             * Average process latency for function for instance
=======
             * Average process latency for function for instance.
>>>>>>> f773c602c... Test pr 10 (#27)
             **/
            public Double avgProcessLatency;
        }

<<<<<<< HEAD
        @Data
        @JsonInclude(JsonInclude.Include.ALWAYS)
        @JsonPropertyOrder({ "receivedTotal", "processedSuccessfullyTotal", "systemExceptionsTotal", "userExceptionsTotal", "avgProcessLatency", "1min", "lastInvocation", "userMetrics" })
=======
        /**
         * Function instance statistics data.
         */
        @Data
        @JsonInclude(JsonInclude.Include.ALWAYS)
        @JsonPropertyOrder({ "receivedTotal", "processedSuccessfullyTotal", "systemExceptionsTotal",
            "userExceptionsTotal", "avgProcessLatency", "1min", "lastInvocation", "userMetrics" })
>>>>>>> f773c602c... Test pr 10 (#27)
        public static class FunctionInstanceStatsData extends FunctionInstanceStatsDataBase {

            @JsonProperty("1min")
            public FunctionInstanceStatsDataBase oneMin = new FunctionInstanceStatsDataBase();

            /**
<<<<<<< HEAD
             * Timestamp of when the function was last invoked for instance
=======
             * Timestamp of when the function was last invoked for instance.
>>>>>>> f773c602c... Test pr 10 (#27)
             **/
            public Long lastInvocation;

            /**
<<<<<<< HEAD
             * Map of user defined metrics
=======
             * Map of user defined metrics.
>>>>>>> f773c602c... Test pr 10 (#27)
             **/
            public Map<String, Double> userMetrics = new HashMap<>();
        }

        public FunctionInstanceStatsData metrics = new FunctionInstanceStatsData();
    }

    public List<FunctionInstanceStats> instances = new LinkedList<>();

    public void addInstance(FunctionInstanceStats functionInstanceStats) {
        instances.add(functionInstanceStats);
    }

    public FunctionStats calculateOverall() {

        int nonNullInstances = 0;
        int nonNullInstancesOneMin = 0;
        for (FunctionInstanceStats functionInstanceStats : instances) {
<<<<<<< HEAD
                FunctionInstanceStats.FunctionInstanceStatsData functionInstanceStatsData = functionInstanceStats.getMetrics();
                receivedTotal += functionInstanceStatsData.receivedTotal;
                processedSuccessfullyTotal += functionInstanceStatsData.processedSuccessfullyTotal;
                systemExceptionsTotal += functionInstanceStatsData.systemExceptionsTotal;
                userExceptionsTotal += functionInstanceStatsData.userExceptionsTotal;
                if (functionInstanceStatsData.avgProcessLatency != null) {
                    if (avgProcessLatency == null) {
                        avgProcessLatency = 0.0;
                    }
                    avgProcessLatency += functionInstanceStatsData.avgProcessLatency;
                    nonNullInstances ++;
                }

                oneMin.receivedTotal += functionInstanceStatsData.oneMin.receivedTotal;
                oneMin.processedSuccessfullyTotal += functionInstanceStatsData.oneMin.processedSuccessfullyTotal;
                oneMin.systemExceptionsTotal += functionInstanceStatsData.oneMin.systemExceptionsTotal;
                oneMin.userExceptionsTotal += functionInstanceStatsData.oneMin.userExceptionsTotal;
                if (functionInstanceStatsData.oneMin.avgProcessLatency != null) {
                    if (oneMin.avgProcessLatency == null) {
                        oneMin.avgProcessLatency = 0.0;
                    }
                    oneMin.avgProcessLatency += functionInstanceStatsData.oneMin.avgProcessLatency;
                    nonNullInstancesOneMin ++;
                }

                if (functionInstanceStatsData.lastInvocation != null) {
                    if (lastInvocation == null || functionInstanceStatsData.lastInvocation > lastInvocation) {
                        lastInvocation = functionInstanceStatsData.lastInvocation;
                    }
                }
            }
=======
            FunctionInstanceStats.FunctionInstanceStatsData functionInstanceStatsData =
                functionInstanceStats.getMetrics();
            receivedTotal += functionInstanceStatsData.receivedTotal;
            processedSuccessfullyTotal += functionInstanceStatsData.processedSuccessfullyTotal;
            systemExceptionsTotal += functionInstanceStatsData.systemExceptionsTotal;
            userExceptionsTotal += functionInstanceStatsData.userExceptionsTotal;
            if (functionInstanceStatsData.avgProcessLatency != null) {
                if (avgProcessLatency == null) {
                    avgProcessLatency = 0.0;
                }
                avgProcessLatency += functionInstanceStatsData.avgProcessLatency;
                nonNullInstances++;
            }

            oneMin.receivedTotal += functionInstanceStatsData.oneMin.receivedTotal;
            oneMin.processedSuccessfullyTotal += functionInstanceStatsData.oneMin.processedSuccessfullyTotal;
            oneMin.systemExceptionsTotal += functionInstanceStatsData.oneMin.systemExceptionsTotal;
            oneMin.userExceptionsTotal += functionInstanceStatsData.oneMin.userExceptionsTotal;
            if (functionInstanceStatsData.oneMin.avgProcessLatency != null) {
                if (oneMin.avgProcessLatency == null) {
                    oneMin.avgProcessLatency = 0.0;
                }
                oneMin.avgProcessLatency += functionInstanceStatsData.oneMin.avgProcessLatency;
                nonNullInstancesOneMin++;
            }

            if (functionInstanceStatsData.lastInvocation != null) {
                if (lastInvocation == null || functionInstanceStatsData.lastInvocation > lastInvocation) {
                    lastInvocation = functionInstanceStatsData.lastInvocation;
                }
            }
        }
>>>>>>> f773c602c... Test pr 10 (#27)

        // calculate average from sum
        if (nonNullInstances > 0) {
            avgProcessLatency = avgProcessLatency / nonNullInstances;
        } else {
            avgProcessLatency = null;
        }

        // calculate 1min average from sum
        if (nonNullInstancesOneMin > 0) {
            oneMin.avgProcessLatency = oneMin.avgProcessLatency / nonNullInstancesOneMin;
        } else {
            oneMin.avgProcessLatency = null;
        }

        return this;
    }

    public static FunctionStats decode (String json) throws IOException {
        return ObjectMapperFactory.getThreadLocal().readValue(json, FunctionStats.class);
    }
}
