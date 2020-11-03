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
package org.apache.pulsar.client.api;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

/**
 * Producer is used to publish messages on a topic.
 *
<<<<<<< HEAD
 * A single producer instance can be used across multiple threads.
=======
 * <p>A single producer instance can be used across multiple threads.
>>>>>>> f773c602c... Test pr 10 (#27)
 */
public interface Producer<T> extends Closeable {

    /**
     * @return the topic which producer is publishing to
     */
    String getTopic();

    /**
     * @return the producer name which could have been assigned by the system or specified by the client
     */
    String getProducerName();

    /**
     * Sends a message.
<<<<<<< HEAD
     * <p>
     * This call will be blocking until is successfully acknowledged by the Pulsar broker.
     * <p>
     * Use {@link #newMessage()} to specify more properties than just the value on the message to be sent.
=======
     *
     * <p>This call will be blocking until is successfully acknowledged by the Pulsar broker.
     *
     * <p>Use {@link #newMessage()} to specify more properties than just the value on the message to be sent.
>>>>>>> f773c602c... Test pr 10 (#27)
     *
     * @param message
     *            a message
     * @return the message id assigned to the published message
     * @throws PulsarClientException.TimeoutException
     *             if the message was not correctly received by the system within the timeout period
     * @throws PulsarClientException.AlreadyClosedException
     *             if the producer was already closed
     */
    MessageId send(T message) throws PulsarClientException;

    /**
<<<<<<< HEAD
     * Send a message asynchronously
     * <p>
     * When the producer queue is full, by default this method will complete the future with an exception
     * {@link PulsarClientException.ProducerQueueIsFullError}
     * <p>
     * See {@link ProducerBuilder#maxPendingMessages(int)} to configure the producer queue size and
     * {@link ProducerBuilder#blockIfQueueFull(boolean)} to change the blocking behavior.
     * <p>
     * Use {@link #newMessage()} to specify more properties than just the value on the message to be sent.
=======
     * Send a message asynchronously.
     *
     * <p>When the producer queue is full, by default this method will complete the future with an exception
     * {@link PulsarClientException.ProducerQueueIsFullError}
     *
     * <p>See {@link ProducerBuilder#maxPendingMessages(int)} to configure the producer queue size and
     * {@link ProducerBuilder#blockIfQueueFull(boolean)} to change the blocking behavior.
     *
     * <p>Use {@link #newMessage()} to specify more properties than just the value on the message to be sent.
>>>>>>> f773c602c... Test pr 10 (#27)
     *
     * @param message
     *            a byte array with the payload of the message
     * @return a future that can be used to track when the message will have been safely persisted
     */
    CompletableFuture<MessageId> sendAsync(T message);

    /**
     * Flush all the messages buffered in the client and wait until all messages have been successfully persisted.
     *
     * @throws PulsarClientException
     * @since 2.1.0
     * @see #flushAsync()
     */
    void flush() throws PulsarClientException;

    /**
     * Flush all the messages buffered in the client and wait until all messages have been successfully persisted.
     *
     * @return a future that can be used to track when all the messages have been safely persisted.
     * @since 2.1.0
     * @see #flush()
     */
    CompletableFuture<Void> flushAsync();

    /**
<<<<<<< HEAD
     * Create a new message builder
     *
     * This message builder allows to specify additional properties on the message. For example:
     *
=======
     * Create a new message builder.
     *
     * <p>This message builder allows to specify additional properties on the message. For example:
>>>>>>> f773c602c... Test pr 10 (#27)
     * <pre>{@code
     * producer.newMessage()
     *       .key(messageKey)
     *       .value(myValue)
     *       .property("user-defined-property", "value")
     *       .send();
     * }</pre>
     *
     * @return a typed message builder that can be used to construct the message to be sent through this producer
     */
    TypedMessageBuilder<T> newMessage();

    /**
<<<<<<< HEAD
     * Get the last sequence id that was published by this producer.
     * <p>
     * This represent either the automatically assigned or custom sequence id (set on the {@link MessageBuilder}) that
     * was published and acknowledged by the broker.
     * <p>
     * After recreating a producer with the same producer name, this will return the last message that was published in
     * the previous producer session, or -1 if there no message was ever published.
=======
     * Create a new message builder with schema, not required same parameterized type with the producer.
     *
     * @return a typed message builder that can be used to construct the message to be sent through this producer
     * @see #newMessage()
     */
    <V> TypedMessageBuilder<V> newMessage(Schema<V> schema);

    /**
     * Get the last sequence id that was published by this producer.
     *
     * <p>This represent either the automatically assigned
     * or custom sequence id (set on the {@link TypedMessageBuilder})
     * that was published and acknowledged by the broker.
     *
     * <p>After recreating a producer with the same producer name, this will return the last message that was
     * published in the previous producer session, or -1 if there no message was ever published.
>>>>>>> f773c602c... Test pr 10 (#27)
     *
     * @return the last sequence id published by this producer
     */
    long getLastSequenceId();

    /**
<<<<<<< HEAD
     * Get statistics for the producer
     *
=======
     * Get statistics for the producer.
>>>>>>> f773c602c... Test pr 10 (#27)
     * <ul>
     * <li>numMsgsSent : Number of messages sent in the current interval
     * <li>numBytesSent : Number of bytes sent in the current interval
     * <li>numSendFailed : Number of messages failed to send in the current interval
     * <li>numAcksReceived : Number of acks received in the current interval
     * <li>totalMsgsSent : Total number of messages sent
     * <li>totalBytesSent : Total number of bytes sent
     * <li>totalSendFailed : Total number of messages failed to send
     * <li>totalAcksReceived: Total number of acks received
     * </ul>
     *
     * @return statistic for the producer or null if ProducerStatsRecorderImpl is disabled.
     */
    ProducerStats getStats();

    /**
     * Close the producer and releases resources allocated.
     *
<<<<<<< HEAD
     * No more writes will be accepted from this producer. Waits until all pending write request are persisted. In case
     * of errors, pending writes will not be retried.
=======
     * <p>No more writes will be accepted from this producer. Waits until all pending write request are persisted.
     * In case of errors, pending writes will not be retried.
>>>>>>> f773c602c... Test pr 10 (#27)
     *
     * @throws PulsarClientException.AlreadyClosedException
     *             if the producer was already closed
     */
    @Override
    void close() throws PulsarClientException;

    /**
     * Close the producer and releases resources allocated.
     *
<<<<<<< HEAD
     * No more writes will be accepted from this producer. Waits until all pending write request are persisted. In case
     * of errors, pending writes will not be retried.
=======
     * <p>No more writes will be accepted from this producer. Waits until all pending write request are persisted.
     * In case of errors, pending writes will not be retried.
>>>>>>> f773c602c... Test pr 10 (#27)
     *
     * @return a future that can used to track when the producer has been closed
     */
    CompletableFuture<Void> closeAsync();

    /**
     * @return Whether the producer is currently connected to the broker
     */
    boolean isConnected();
}
