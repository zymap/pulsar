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
package org.apache.pulsar.functions.api;

import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
<<<<<<< HEAD
=======
import java.util.Optional;
>>>>>>> f773c602c... Test pr 10 (#27)
import java.util.concurrent.CompletableFuture;

public interface WindowContext {

    /**
<<<<<<< HEAD
     * The tenant this function belongs to
=======
     * The tenant this function belongs to.
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @return the tenant this function belongs to
     */
    String getTenant();

    /**
<<<<<<< HEAD
     * The namespace this function belongs to
=======
     * The namespace this function belongs to.
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @return the namespace this function belongs to
     */
    String getNamespace();

    /**
<<<<<<< HEAD
     * The name of the function that we are executing
=======
     * The name of the function that we are executing.
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @return The Function name
     */
    String getFunctionName();

    /**
<<<<<<< HEAD
     * The id of the function that we are executing
=======
     * The id of the function that we are executing.
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @return The function id
     */
    String getFunctionId();

    /**
     * The id of the instance that invokes this function.
     *
     * @return the instance id
     */
    int getInstanceId();

    /**
     * Get the number of instances that invoke this function.
     *
     * @return the number of instances that invoke this function.
     */
    int getNumInstances();

    /**
<<<<<<< HEAD
     * The version of the function that we are executing
=======
     * The version of the function that we are executing.
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @return The version id
     */
    String getFunctionVersion();

    /**
<<<<<<< HEAD
     * Get a list of all input topics
=======
     * Get a list of all input topics.
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @return a list of all input topics
     */
    Collection<String> getInputTopics();

    /**
<<<<<<< HEAD
     * Get the output topic of the function
=======
     * Get the output topic of the function.
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @return output topic name
     */
    String getOutputTopic();

    /**
<<<<<<< HEAD
     * Get output schema builtin type or custom class name
=======
     * Get output schema builtin type or custom class name.
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @return output schema builtin type or custom class name
     */
    String getOutputSchemaType();

    /**
<<<<<<< HEAD
     * The logger object that can be used to log in a function
=======
     * The logger object that can be used to log in a function.
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @return the logger object
     */
    Logger getLogger();

    /**
<<<<<<< HEAD
     * Increment the builtin distributed counter refered by key
=======
     * Increment the builtin distributed counter referred by key.
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @param key The name of the key
     * @param amount The amount to be incremented
     */
    void incrCounter(String key, long amount);

    /**
     * Retrieve the counter value for the key.
     *
     * @param key name of the key
     * @return the amount of the counter value for this key
     */
    long getCounter(String key);

    /**
<<<<<<< HEAD
     * Updare the state value for the key.
=======
     * Update the state value for the key.
>>>>>>> f773c602c... Test pr 10 (#27)
     *
     * @param key name of the key
     * @param value state value of the key
     */
    void putState(String key, ByteBuffer value);

    /**
     * Retrieve the state value for the key.
     *
     * @param key name of the key
     * @return the state value for the key.
     */
    ByteBuffer getState(String key);

    /**
<<<<<<< HEAD
     * Get a map of all user-defined key/value configs for the function
=======
     * Get a map of all user-defined key/value configs for the function.
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @return The full map of user-defined config values
     */
    Map<String, Object> getUserConfigMap();

    /**
<<<<<<< HEAD
     * Get Any user defined key/value
     * @param key The key
     * @return The value specified by the user for that key. null if no such key
     */
    String getUserConfigValue(String key);

    /**
     * Get any user-defined key/value or a default value if none is present
=======
     * Get any user-defined key/value.
     *
     * @param key The key
     * @return The Optional value specified by the user for that key.
     */
    Optional<Object> getUserConfigValue(String key);

    /**
     * Get any user-defined key/value or a default value if none is present.
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @param key
     * @param defaultValue
     * @return Either the user config value associated with a given key or a supplied default value
     */
    Object getUserConfigValueOrDefault(String key, Object defaultValue);

    /**
<<<<<<< HEAD
     * Record a user defined metric
=======
     * Record a user defined metric.
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @param metricName The name of the metric
     * @param value The value of the metric
     */
    void recordMetric(String metricName, double value);

    /**
<<<<<<< HEAD
     * Publish an object using serDe for serializing to the topic
=======
     * Publish an object using serDe for serializing to the topic.
>>>>>>> f773c602c... Test pr 10 (#27)
     *
     * @param topicName
     *            The name of the topic for publishing
     * @param object
     *            The object that needs to be published
     * @param schemaOrSerdeClassName
     *            Either a builtin schema type (eg: "avro", "json", "protobuf") or the class name of the custom schema class
     * @return A future that completes when the framework is done publishing the message
     */
    <O> CompletableFuture<Void> publish(String topicName, O object, String schemaOrSerdeClassName);

    /**
<<<<<<< HEAD
     * Publish an object to the topic using default schemas
=======
     * Publish an object to the topic using default schemas.
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @param topicName The name of the topic for publishing
     * @param object The object that needs to be published
     * @return A future that completes when the framework is done publishing the message
     */
    <O> CompletableFuture<Void> publish(String topicName, O object);
}
