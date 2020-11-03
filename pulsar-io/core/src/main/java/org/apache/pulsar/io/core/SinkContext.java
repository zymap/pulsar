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
package org.apache.pulsar.io.core;

<<<<<<< HEAD
import org.slf4j.Logger;

import java.util.Collection;

public interface SinkContext {

    /**
     * The id of the instance that invokes this sink.
     *
     * @return the instance id
     */
    int getInstanceId();

    /**
     * Get the number of instances that invoke this sink.
     *
     * @return the number of instances that invoke this sink.
     */
    int getNumInstances();

    /**
     * Record a user defined metric
     * @param metricName The name of the metric
     * @param value The value of the metric
     */
    void recordMetric(String metricName, double value);
=======
import java.util.Collection;

/**
 * Interface for a sink connector providing information about environment where it is running.
 * It also allows to propagate information, such as logs, metrics, states, back to the Pulsar environment.
 */
public interface SinkContext extends ConnectorContext {
>>>>>>> f773c602c... Test pr 10 (#27)

    /**
     * Get a list of all input topics
     * @return a list of all input topics
     */
    Collection<String> getInputTopics();

    /**
<<<<<<< HEAD
     * The tenant this sink belongs to
     * @return the tenant this sink belongs to
     */
    String getTenant();

    /**
     * The namespace this sink belongs to
     * @return the namespace this sink belongs to
     */
    String getNamespace();

    /**
=======
>>>>>>> f773c602c... Test pr 10 (#27)
     * The name of the sink that we are executing
     * @return The Sink name
     */
    String getSinkName();

<<<<<<< HEAD
    /**
     * The logger object that can be used to log in a sink
     * @return the logger object
     */
    Logger getLogger();
=======
>>>>>>> f773c602c... Test pr 10 (#27)
}
