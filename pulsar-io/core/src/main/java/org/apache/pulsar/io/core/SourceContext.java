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

public interface SourceContext {

    /**
     * The id of the instance that invokes this source.
     *
     * @return the instance id
     */
    int getInstanceId();

    /**
     * Get the number of instances that invoke this source.
     *
     * @return the number of instances that invoke this source.
     */
    int getNumInstances();

    /**
     * Record a user defined metric
     * @param metricName The name of the metric
     * @param value The value of the metric
     */
    void recordMetric(String metricName, double value);

    /**
     * Get the output topic of the source
=======
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;

/**
 * Interface for a source connector providing information about environment where it is running.
 * It also allows to propagate information, such as logs, metrics, states, back to the Pulsar environment.
 */
public interface SourceContext extends ConnectorContext {

    /**
     * Get the output topic of the source.
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @return output topic name
     */
    String getOutputTopic();

    /**
<<<<<<< HEAD
     * The tenant this source belongs to
     * @return the tenant this source belongs to
     */
    String getTenant();

    /**
     * The namespace this source belongs to
     * @return the namespace this source belongs to
     */
    String getNamespace();

    /**
     * The name of the source that we are executing
     * @return The Source name
     */
    String getSourceName();

    /**
     * The logger object that can be used to log in a source
     * @return the logger object
     */
    Logger getLogger();
=======
     * The name of the source that we are executing.
     *
     * @return The Source name
     */
    String getSourceName();

    /**
     * New output message using schema for serializing to the topic
     *
     * @param topicName The name of the topic for output message
     * @param schema provide a way to convert between serialized data and domain objects
     * @param <O>
     * @return the message builder instance
     * @throws PulsarClientException
     */
    <O> TypedMessageBuilder<O> newOutputMessage(String topicName, Schema<O> schema) throws PulsarClientException;

    /**
     * Create a ConsumerBuilder with the schema.
     *
     * @param schema provide a way to convert between serialized data and domain objects
     * @param <O>
     * @return the consumer builder instance
     * @throws PulsarClientException
     */
    <O> ConsumerBuilder<O> newConsumerBuilder(Schema<O> schema) throws PulsarClientException;
>>>>>>> f773c602c... Test pr 10 (#27)
}
